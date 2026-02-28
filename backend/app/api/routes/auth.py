# backend/app/api/routes/auth.py
"""
Multi-tenant auth system — Julius.ai style.

Storage (all in Docker PostgreSQL da_db):
┌─────────────────────┬────────────────────────────────────────────────┐
│ Table               │ Purpose                                        │
├─────────────────────┼────────────────────────────────────────────────┤
│ users               │ accounts, bcrypt passwords, active flag        │
│ user_connections    │ saved DB connections, AES-encrypted passwords  │
│ user_api_keys       │ per-user API keys (hashed), with permissions   │
│ query_audit_log     │ every query: who, what table, what SQL, when   │
└─────────────────────┴────────────────────────────────────────────────┘

Security layers:
  1. Login passwords   → bcrypt hashed (one-way, never recoverable)
  2. DB conn passwords → AES-256 / Fernet encrypted at rest
  3. JWT tokens        → HS256, configurable expiry, signed with SECRET_KEY
  4. API keys          → sha256 hashed in DB, only shown once at creation
  5. Audit log         → every query recorded with user_id, cannot be tampered
  6. Row-level isolation → every query filters WHERE user_id = %s
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import secrets
import string
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2
import psycopg2.extras
from cryptography.fernet import Fernet
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
import bcrypt
from pydantic import BaseModel, EmailStr, Field

logger = logging.getLogger("db_assistant.auth")
router = APIRouter(prefix="/auth", tags=["auth"])

# ─────────────────────────────────────────────────────────────
# Configuration  (set via environment variables)
# ─────────────────────────────────────────────────────────────
SECRET_KEY   = os.getenv("SECRET_KEY", "CHANGE_ME_use_python_secrets_token_hex_32")
ALGORITHM    = "HS256"
TOKEN_EXPIRE = int(os.getenv("TOKEN_EXPIRE_DAYS", "7"))

# Fernet key for DB password encryption
# Generate once: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
_RAW_ENC_KEY = os.getenv("ENCRYPTION_KEY", "")
if not _RAW_ENC_KEY:
    derived      = hashlib.sha256(SECRET_KEY.encode()).digest()
    _RAW_ENC_KEY = base64.urlsafe_b64encode(derived).decode()

fernet = Fernet(_RAW_ENC_KEY.encode())
oauth2 = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


# ─────────────────────────────────────────────────────────────
# System DB connection  (your Docker PostgreSQL)
# ─────────────────────────────────────────────────────────────
def _system_conn():
    uri = (
        f"postgresql://"
        f"{os.getenv('DB_USER','da_user')}:{os.getenv('DB_PASSWORD','da_pass')}"
        f"@{os.getenv('DB_HOST','127.0.0.1')}:{os.getenv('DB_PORT','5433')}"
        f"/{os.getenv('DB_NAME','da_db')}"
    )
    try:
        return psycopg2.connect(uri, connect_timeout=8,
                                cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as e:
        raise HTTPException(503, detail=f"System DB unavailable: {e}")


# ─────────────────────────────────────────────────────────────
# Table bootstrap  (runs on first import)
# ─────────────────────────────────────────────────────────────
def _ensure_tables():
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            -- ── Users ─────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL PRIMARY KEY,
                email           TEXT    NOT NULL UNIQUE,
                hashed_password TEXT    NOT NULL,
                full_name       TEXT,
                is_active       BOOLEAN NOT NULL DEFAULT TRUE,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_login_at   TIMESTAMPTZ
            );

            -- ── Saved DB connections ──────────────────────────────────
            CREATE TABLE IF NOT EXISTS user_connections (
                id                 SERIAL PRIMARY KEY,
                user_id            INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name               TEXT    NOT NULL,
                db_type            TEXT    NOT NULL DEFAULT 'postgresql',
                host               TEXT    NOT NULL,
                port               INTEGER NOT NULL,
                dbname             TEXT    NOT NULL,
                db_username        TEXT    NOT NULL,
                encrypted_password TEXT    NOT NULL,   -- Fernet AES-256
                is_default         BOOLEAN NOT NULL DEFAULT FALSE,
                last_used_at       TIMESTAMPTZ,
                created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(user_id, name)
            );

            -- ── Per-user API keys ─────────────────────────────────────
            CREATE TABLE IF NOT EXISTS user_api_keys (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name        TEXT    NOT NULL,           -- friendly label e.g. "My App"
                key_hash    TEXT    NOT NULL UNIQUE,    -- sha256(raw_key) — raw never stored
                key_prefix  TEXT    NOT NULL,           -- first 8 chars for display e.g. "dba_a1b2"
                permissions TEXT[]  NOT NULL DEFAULT ARRAY['read'],  -- e.g. ['read','write']
                is_active   BOOLEAN NOT NULL DEFAULT TRUE,
                last_used_at TIMESTAMPTZ,
                expires_at  TIMESTAMPTZ,               -- NULL = never expires
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(user_id, name)
            );

            -- ── Query audit log ───────────────────────────────────────
            CREATE TABLE IF NOT EXISTS query_audit_log (
                id              BIGSERIAL PRIMARY KEY,
                user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                connection_id   INTEGER REFERENCES user_connections(id) ON DELETE SET NULL,
                query_type      TEXT    NOT NULL,       -- 'nl_query' | 'direct_sql' | 'upload' | 'login' | 'api_key_use'
                table_names     TEXT[],                 -- tables involved
                question        TEXT,                   -- original NL question if applicable
                sql_generated   TEXT,                   -- SQL that was run
                row_count       INTEGER,
                execution_ms    INTEGER,
                ip_address      TEXT,
                status          TEXT    NOT NULL DEFAULT 'success',  -- 'success'|'error'
                error_detail    TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_audit_user ON query_audit_log(user_id);
            CREATE INDEX IF NOT EXISTS idx_audit_created ON query_audit_log(created_at DESC);

            -- ── User uploads tracker ──────────────────────────────
            CREATE TABLE IF NOT EXISTS user_uploads (
                id              SERIAL PRIMARY KEY,
                user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                file_name       TEXT    NOT NULL,
                file_size_bytes INTEGER,
                row_count       INTEGER,
                db_type         TEXT    NOT NULL DEFAULT 'postgresql',  -- 'postgresql' | 'mongodb'
                destination     TEXT    NOT NULL,   -- "schema.table" or "db.collection"
                connection_name TEXT,               -- friendly name of the connection used
                status          TEXT    NOT NULL DEFAULT 'success',
                error_detail    TEXT,
                uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_uploads_user ON user_uploads(user_id);

            -- ── Dataset registry ──────────────────────────────────────
            CREATE TABLE IF NOT EXISTS dataset_registry (
                dataset_id          TEXT PRIMARY KEY,
                user_id             INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                table_name          TEXT NOT NULL,
                table_schema_name   TEXT NOT NULL,
                original_filename   TEXT,
                row_count           INTEGER,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_dataset_user ON dataset_registry(user_id);

            -- ── Dataset columns ───────────────────────────────────────
            CREATE TABLE IF NOT EXISTS dataset_columns (
                id                  SERIAL PRIMARY KEY,
                dataset_id          TEXT NOT NULL REFERENCES dataset_registry(dataset_id) ON DELETE CASCADE,
                column_name         TEXT NOT NULL,
                pg_type             TEXT NOT NULL,
                ordinal_position    INTEGER NOT NULL
            );
            """)
        conn.commit()
        logger.info("All auth tables ready (users, user_connections, user_api_keys, query_audit_log, dataset_registry).")
    except Exception as e:
        logger.error("Table bootstrap failed: %s", e)
        conn.rollback()
    finally:
        conn.close()

_ensure_tables()


# ─────────────────────────────────────────────────────────────
# Crypto helpers  — raw bcrypt, NO passlib (avoids 72-byte error)
# ─────────────────────────────────────────────────────────────
def _prepare(plain: str) -> bytes:
    """SHA-256 the password first → always 64 hex chars → safe for bcrypt."""
    return hashlib.sha256(plain.encode("utf-8")).hexdigest().encode("utf-8")

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(_prepare(plain), bcrypt.gensalt(rounds=12)).decode("utf-8")

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_prepare(plain), hashed.encode("utf-8"))
    except Exception:
        return False

def encrypt_db_password(plain: str) -> str:
    return fernet.encrypt(plain.encode()).decode()

def decrypt_db_password(enc: str) -> str:
    return fernet.decrypt(enc.encode()).decode()

def _generate_api_key() -> tuple[str, str]:
    """
    Returns (raw_key, key_hash).
    raw_key  = "dba_" + 32 random url-safe chars  — shown ONCE to user
    key_hash = sha256(raw_key)                      — stored in DB
    """
    alphabet = string.ascii_letters + string.digits
    random_part = "".join(secrets.choice(alphabet) for _ in range(32))
    raw_key  = f"dba_{random_part}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    return raw_key, key_hash

def _hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()

def create_access_token(user_id: int, email: str) -> str:
    exp = datetime.utcnow() + timedelta(days=TOKEN_EXPIRE)
    return jwt.encode({"sub": str(user_id), "email": email, "exp": exp},
                      SECRET_KEY, algorithm=ALGORITHM)


# ─────────────────────────────────────────────────────────────
# Auth dependencies
# ─────────────────────────────────────────────────────────────
def get_current_user(token: str = Depends(oauth2)) -> dict:
    """
    FastAPI dependency: validates JWT → returns {"user_id": int, "email": str}.
    Raise 401 if missing or invalid.
    """
    if not token:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated. Please log in.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        payload  = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id  = int(payload.get("sub", 0))
        email    = payload.get("email", "")
        if not user_id:
            raise ValueError("Empty sub")
        return {"user_id": user_id, "email": email}
    except (JWTError, ValueError):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Token invalid or expired. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_user_from_api_key(request: Request) -> Optional[dict]:
    """
    Extract user from X-API-Key header.
    Returns user dict or None if header absent.
    """
    raw_key = request.headers.get("X-API-Key")
    if not raw_key:
        return None
    key_hash = _hash_api_key(raw_key)
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ak.user_id, ak.permissions, ak.is_active, ak.expires_at,
                       u.email, u.is_active AS user_active
                FROM user_api_keys ak
                JOIN users u ON u.id = ak.user_id
                WHERE ak.key_hash = %s
            """, (key_hash,))
            row = cur.fetchone()
        if not row:
            return None
        r = dict(row)
        if not r["is_active"] or not r["user_active"]:
            return None
        if r["expires_at"] and r["expires_at"] < datetime.utcnow():
            return None
        # Update last_used_at
        with conn.cursor() as cur:
            cur.execute("UPDATE user_api_keys SET last_used_at=NOW() WHERE key_hash=%s", (key_hash,))
        conn.commit()
        return {"user_id": r["user_id"], "email": r["email"], "permissions": r["permissions"]}
    finally:
        conn.close()


def get_current_user_flexible(
    request: Request,
    token: str = Depends(oauth2),
) -> dict:
    """
    Accepts EITHER a JWT Bearer token OR an X-API-Key header.
    JWT = interactive login | API key = programmatic/developer access.
    """
    # Try API key first
    api_user = get_user_from_api_key(request)
    if api_user:
        return api_user
    # Fall back to JWT
    return get_current_user(token)


# ─────────────────────────────────────────────────────────────
# Audit log helper
# ─────────────────────────────────────────────────────────────
def log_query(
    user_id:       int,
    query_type:    str,
    *,
    connection_id: Optional[int] = None,
    table_names:   Optional[List[str]] = None,
    question:      Optional[str] = None,
    sql_generated: Optional[str] = None,
    row_count:     Optional[int] = None,
    execution_ms:  Optional[int] = None,
    ip_address:    Optional[str] = None,
    status:        str = "success",
    error_detail:  Optional[str] = None,
):
    """Write one row to query_audit_log. Non-blocking — errors are swallowed."""
    try:
        conn = _system_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO query_audit_log
                    (user_id, connection_id, query_type, table_names, question,
                     sql_generated, row_count, execution_ms, ip_address, status, error_detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (user_id, connection_id, query_type,
                  table_names or [], question, sql_generated,
                  row_count, execution_ms, ip_address, status, error_detail))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Audit log write failed (non-fatal): %s", e)


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    email:     EmailStr
    password:  str = Field(..., min_length=6, description="Min 6 characters")
    full_name: Optional[str] = None

class LoginResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    user_id:      int
    email:        str
    full_name:    Optional[str]
    expires_in_days: int

class SaveConnectionRequest(BaseModel):
    name:        str = Field(..., min_length=1, max_length=80)
    db_type:     str = "postgresql"
    host:        str
    port:        int = Field(..., ge=1, le=65535)
    dbname:      str
    db_username: str
    password:    str
    is_default:  bool = False

class ConnectionResponse(BaseModel):
    id:           int
    name:         str
    db_type:      str
    host:         str
    port:         int
    dbname:       str
    db_username:  str
    is_default:   bool
    last_used_at: Optional[str]
    created_at:   str

class CreateApiKeyRequest(BaseModel):
    name:        str = Field(..., min_length=1, max_length=80,
                             description="Friendly label e.g. 'My App'")
    permissions: List[str] = Field(default=["read"],
                                   description="List of: 'read', 'write'")
    expires_days: Optional[int] = Field(None, ge=1, le=365,
                                        description="Expiry in days. Omit for no expiry.")

class ApiKeyResponse(BaseModel):
    id:          int
    name:        str
    key_prefix:  str
    permissions: List[str]
    is_active:   bool
    last_used_at: Optional[str]
    expires_at:  Optional[str]
    created_at:  str

class AuditLogEntry(BaseModel):
    id:            int
    query_type:    str
    table_names:   List[str]
    question:      Optional[str]
    sql_generated: Optional[str]
    row_count:     Optional[int]
    execution_ms:  Optional[int]
    status:        str
    created_at:    str


# ─────────────────────────────────────────────────────────────
# Auth endpoints
# ─────────────────────────────────────────────────────────────

@router.post("/register", status_code=201, summary="Create account")
def register(req: RegisterRequest, request: Request):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE email=%s", (req.email,))
            if cur.fetchone():
                raise HTTPException(400, detail="Email already registered.")
            cur.execute(
                "INSERT INTO users (email,hashed_password,full_name) VALUES (%s,%s,%s) RETURNING id",
                (req.email, hash_password(req.password), req.full_name)
            )
            user_id = dict(cur.fetchone())["id"]
        conn.commit()
    finally:
        conn.close()

    log_query(user_id, "register",
              ip_address=request.client.host if request.client else None)
    logger.info("Registered: %s (id=%s)", req.email, user_id)
    return {"message": "Account created!", "user_id": user_id}


@router.post("/login", response_model=LoginResponse, summary="Sign in")
def login(form: OAuth2PasswordRequestForm = Depends(), request: Request = None):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id,email,hashed_password,full_name,is_active FROM users WHERE email=%s",
                (form.username,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not verify_password(form.password, dict(row)["hashed_password"]):
        raise HTTPException(401, detail="Invalid email or password.")
    user = dict(row)
    if not user["is_active"]:
        raise HTTPException(403, detail="Account disabled.")

    # Update last_login_at
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_login_at=NOW() WHERE id=%s", (user["id"],))
        conn.commit()
    finally:
        conn.close()

    token = create_access_token(user["id"], user["email"])
    log_query(user["id"], "login",
              ip_address=request.client.host if request and request.client else None)
    return LoginResponse(
        access_token=token, user_id=user["id"],
        email=user["email"], full_name=user["full_name"],
        expires_in_days=TOKEN_EXPIRE,
    )


@router.get("/me", summary="Current user info")
def get_me(user=Depends(get_current_user)):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id,email,full_name,created_at,last_login_at FROM users WHERE id=%s",
                (user["user_id"],)
            )
            row = dict(cur.fetchone() or {})
    finally:
        conn.close()
    return {
        "user_id":       row.get("id"),
        "email":         row.get("email"),
        "full_name":     row.get("full_name"),
        "created_at":    str(row.get("created_at", "")),
        "last_login_at": str(row.get("last_login_at", "")),
    }


# ─────────────────────────────────────────────────────────────
# Connection endpoints
# ─────────────────────────────────────────────────────────────

@router.post("/connections", status_code=201, summary="Save a DB connection")
def save_connection(req: SaveConnectionRequest, user=Depends(get_current_user)):
    user_id = user["user_id"]
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            if req.is_default:
                cur.execute(
                    "UPDATE user_connections SET is_default=FALSE WHERE user_id=%s", (user_id,)
                )
            cur.execute("""
                INSERT INTO user_connections
                    (user_id,name,db_type,host,port,dbname,db_username,encrypted_password,is_default)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id,name) DO UPDATE SET
                    host=EXCLUDED.host, port=EXCLUDED.port, dbname=EXCLUDED.dbname,
                    db_username=EXCLUDED.db_username, encrypted_password=EXCLUDED.encrypted_password,
                    is_default=EXCLUDED.is_default
                RETURNING id
            """, (user_id, req.name, req.db_type, req.host, req.port,
                  req.dbname, req.db_username, encrypt_db_password(req.password), req.is_default))
            conn_id = dict(cur.fetchone())["id"]
        conn.commit()
    finally:
        conn.close()
    log_query(user_id, "save_connection",
              connection_id=conn_id, table_names=[req.name])
    return {"message": "Connection saved.", "id": conn_id}


@router.get("/connections", response_model=List[ConnectionResponse],
            summary="List my saved connections")
def list_connections(user=Depends(get_current_user)):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,name,db_type,host,port,dbname,db_username,
                       is_default,last_used_at,created_at
                FROM user_connections
                WHERE user_id=%s
                ORDER BY is_default DESC, last_used_at DESC NULLS LAST, created_at DESC
            """, (user["user_id"],))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [ConnectionResponse(**{k: str(v) if isinstance(v, datetime) else v
                                  for k, v in dict(r).items()}) for r in rows]


class GetURIRequest(BaseModel):
    connection_id: int

@router.post("/connections/get-uri", summary="Get decrypted URI for a saved connection")
def get_connection_uri_by_id(req: GetURIRequest, user=Depends(get_current_user)):
    """Returns the decrypted connection URI. Used by upload and query pages."""
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT host, port, dbname, db_username, encrypted_password, db_type
                FROM user_connections
                WHERE id = %s AND user_id = %s
            """, (req.connection_id, user["user_id"]))
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, detail="Connection not found.")

    r = dict(row)
    # Use the same decrypt function used everywhere else in this file
    try:
        pw = decrypt_db_password(r["encrypted_password"])
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to decrypt connection password: {e}")

    db_type = r.get("db_type", "postgresql")

    if db_type == "mongodb":
        # For MongoDB, the full URI is stored as the encrypted value
        uri = pw
    else:
        # URL-encode password in case it contains special chars (@, #, /, etc.)
        from urllib.parse import quote_plus
        safe_pw   = quote_plus(pw)
        safe_user = quote_plus(r["db_username"])
        uri = f"postgresql://{safe_user}:{safe_pw}@{r['host']}:{r['port']}/{r['dbname']}"

    return {"uri": uri, "db_type": db_type}


@router.post("/connections/{conn_id}/set-default", summary="Set a connection as default")
def set_default_connection(conn_id: int, user=Depends(get_current_user)):
    """Mark one connection as default, clear default on all others. No password needed."""
    user_id = user["user_id"]
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            # Verify ownership first
            cur.execute("SELECT id FROM user_connections WHERE id=%s AND user_id=%s",
                        (conn_id, user_id))
            if not cur.fetchone():
                raise HTTPException(404, detail="Connection not found.")
            # Clear all defaults for this user, then set this one
            cur.execute("UPDATE user_connections SET is_default=FALSE WHERE user_id=%s", (user_id,))
            cur.execute("UPDATE user_connections SET is_default=TRUE  WHERE id=%s AND user_id=%s",
                        (conn_id, user_id))
        conn.commit()
    finally:
        conn.close()
    return {"message": "Default connection updated."}


@router.delete("/connections/{conn_id}", summary="Delete a saved connection")
def delete_connection(conn_id: int, user=Depends(get_current_user)):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_connections WHERE id=%s AND user_id=%s RETURNING id",
                (conn_id, user["user_id"])
            )
            if not cur.fetchone():
                raise HTTPException(404, detail="Connection not found.")
        conn.commit()
    finally:
        conn.close()
    return {"message": "Deleted."}


# ─────────────────────────────────────────────────────────────
# API Key endpoints
# ─────────────────────────────────────────────────────────────

@router.post("/api-keys", status_code=201, summary="Generate a new API key")
def create_api_key(req: CreateApiKeyRequest, user=Depends(get_current_user)):
    """
    Creates a new API key.
    ⚠️  The raw key is returned ONCE — store it safely. We only keep the hash.
    """
    user_id    = user["user_id"]
    raw_key, key_hash = _generate_api_key()
    key_prefix = raw_key[:8]   # e.g. "dba_a1b2" shown in dashboard
    expires_at = (datetime.utcnow() + timedelta(days=req.expires_days)
                  if req.expires_days else None)

    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_api_keys
                    (user_id,name,key_hash,key_prefix,permissions,expires_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id,name) DO UPDATE SET
                    key_hash=EXCLUDED.key_hash, key_prefix=EXCLUDED.key_prefix,
                    permissions=EXCLUDED.permissions, expires_at=EXCLUDED.expires_at,
                    is_active=TRUE
                RETURNING id
            """, (user_id, req.name, key_hash, key_prefix,
                  req.permissions, expires_at))
            key_id = dict(cur.fetchone())["id"]
        conn.commit()
    finally:
        conn.close()

    log_query(user_id, "api_key_created")
    return {
        "id":          key_id,
        "name":        req.name,
        "api_key":     raw_key,          # ← shown ONCE, not stored
        "key_prefix":  key_prefix,
        "permissions": req.permissions,
        "expires_at":  str(expires_at) if expires_at else None,
        "warning":     "Copy this key now — it will never be shown again.",
    }


@router.get("/api-keys", response_model=List[ApiKeyResponse],
            summary="List my API keys")
def list_api_keys(user=Depends(get_current_user)):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,name,key_prefix,permissions,is_active,last_used_at,expires_at,created_at
                FROM user_api_keys
                WHERE user_id=%s
                ORDER BY created_at DESC
            """, (user["user_id"],))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [ApiKeyResponse(**{k: (str(v) if isinstance(v, datetime) else
                                  (list(v) if isinstance(v, (list, tuple)) else v))
                               for k, v in dict(r).items()}) for r in rows]


@router.delete("/api-keys/{key_id}", summary="Revoke an API key")
def revoke_api_key(key_id: int, user=Depends(get_current_user)):
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE user_api_keys SET is_active=FALSE WHERE id=%s AND user_id=%s RETURNING id",
                (key_id, user["user_id"])
            )
            if not cur.fetchone():
                raise HTTPException(404, detail="API key not found.")
        conn.commit()
    finally:
        conn.close()
    return {"message": "API key revoked."}


# ─────────────────────────────────────────────────────────────
# Audit log endpoint
# ─────────────────────────────────────────────────────────────

@router.get("/audit-log", response_model=List[AuditLogEntry],
            summary="My query history / audit log")
def get_audit_log(limit: int = 50, user=Depends(get_current_user)):
    """Returns the last N audit log entries for the current user only."""
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,query_type,
                       COALESCE(table_names, ARRAY[]::text[]) AS table_names,
                       question,sql_generated,row_count,
                       execution_ms,status,created_at
                FROM query_audit_log
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT %s
            """, (user["user_id"], min(limit, 200)))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [AuditLogEntry(**{k: (str(v) if isinstance(v, datetime) else
                                 (list(v) if isinstance(v, (list, tuple)) else v))
                              for k, v in dict(r).items()}) for r in rows]


# ─────────────────────────────────────────────────────────────
# Upload tracking endpoints
# ─────────────────────────────────────────────────────────────

class TrackUploadRequest(BaseModel):
    file_name:       str
    file_size_bytes: Optional[int] = None
    row_count:       Optional[int] = None
    db_type:         str  = "postgresql"
    destination:     str  = ""     # "public.my_table" or "sales_db.orders"
    connection_name: Optional[str] = None
    status:          str  = "success"
    error_detail:    Optional[str] = None

class UploadRecord(BaseModel):
    id:              int
    file_name:       str
    file_size_bytes: Optional[int]
    row_count:       Optional[int]
    db_type:         str
    destination:     str
    connection_name: Optional[str]
    status:          str
    uploaded_at:     str


@router.post("/uploads/track", status_code=201, summary="Record a file upload")
def track_upload(req: TrackUploadRequest, user=Depends(get_current_user)):
    """Called by Streamlit after every successful (or failed) upload."""
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_uploads
                    (user_id, file_name, file_size_bytes, row_count, db_type,
                     destination, connection_name, status, error_detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (user["user_id"], req.file_name, req.file_size_bytes,
                  req.row_count, req.db_type, req.destination,
                  req.connection_name, req.status, req.error_detail))
            upload_id = dict(cur.fetchone())["id"]
        conn.commit()
    finally:
        conn.close()
    return {"id": upload_id, "message": "Upload tracked."}


@router.get("/uploads", response_model=List[UploadRecord],
            summary="List my uploaded files")
def list_uploads(limit: int = 100, user=Depends(get_current_user)):
    """Returns all uploads for the current user, newest first."""
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, file_name, file_size_bytes, row_count, db_type,
                       destination, connection_name, status, uploaded_at
                FROM user_uploads
                WHERE user_id = %s
                ORDER BY uploaded_at DESC
                LIMIT %s
            """, (user["user_id"], min(limit, 500)))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [UploadRecord(**{k: (str(v) if isinstance(v, datetime) else v)
                             for k, v in dict(r).items()}) for r in rows]


@router.delete("/uploads/{upload_id}", summary="Remove an upload record")
def delete_upload_record(upload_id: int, user=Depends(get_current_user)):
    """Removes the tracking record only — does NOT delete the actual table/collection."""
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_uploads WHERE id=%s AND user_id=%s RETURNING id",
                (upload_id, user["user_id"])
            )
            if not cur.fetchone():
                raise HTTPException(404, detail="Upload record not found.")
        conn.commit()
    finally:
        conn.close()
    return {"message": "Record removed."}


# ─────────────────────────────────────────────────────────────
# Internal helpers used by pg_query.py
# ─────────────────────────────────────────────────────────────

def get_connection_uri(conn_id: int, user_id: int) -> str:
    """
    Resolve a saved connection → decrypted pg URI.
    Enforces ownership: raises 404 if conn_id belongs to a different user.
    """
    conn = _system_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE user_connections SET last_used_at=NOW() "
                "WHERE id=%s AND user_id=%s RETURNING *",
                (conn_id, user_id)
            )
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, detail="Connection not found or access denied.")

    r  = dict(row)
    pw = decrypt_db_password(r["encrypted_password"])
    return (f"postgresql://{r['db_username']}:{pw}"
            f"@{r['host']}:{r['port']}/{r['dbname']}")