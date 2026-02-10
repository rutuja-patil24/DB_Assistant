from fastapi import APIRouter, UploadFile, File, Header, HTTPException
from google.cloud import storage
import pandas as pd
import uuid
import os
import io
import re
from datetime import datetime
from app.services.schema_summary import build_schema_prompt


from app.db import get_conn  # assumes you already have this
# If you don't have get_conn, tell me and I’ll adapt to your db.py

router = APIRouter(prefix="/datasets", tags=["datasets"])

def _safe_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "col"
    if name[0].isdigit():
        name = f"c_{name}"
    return name

def _infer_pg_type(series: pd.Series) -> str:
    # Try numeric, bool, datetime; fallback text
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_integer_dtype(series):
        return "bigint"
    if pd.api.types.is_float_dtype(series):
        return "double precision"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamp"
    return "text"

def _user_schema(user_id: str) -> str:
    # stable schema name per user; keep it simple:
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", user_id).lower()
    return f"u_{safe[:32]}"

def _upload_to_gcs(bucket_name: str, object_name: str, data: bytes, content_type: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket_name}/{object_name}"

def _save_local(schema: str, dataset_id: uuid.UUID, filename: str, data: bytes) -> str:
    base = os.getenv("UPLOAD_DIR", "uploads")
    local_dir = os.path.join(base, schema, str(dataset_id))
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)
    with open(local_path, "wb") as f:
        f.write(data)
    return f"file://{os.path.abspath(local_path)}"


@router.post("/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    x_user_id: str = Header(..., alias="X-User-Id"),
    dataset_name: str | None = None
):
    bucket = os.getenv("GCS_BUCKET")  # optional in local mode


    filename = file.filename or "upload"
    ext = filename.split(".")[-1].lower()
    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(status_code=400, detail="Only CSV/XLSX supported")

    raw = await file.read()
    dataset_id = uuid.uuid4()
    ds_name = dataset_name or filename.rsplit(".", 1)[0]

    # Read into DataFrame
    try:
        if ext == "csv":
            df = pd.read_csv(io.BytesIO(raw))
            content_type = "text/csv"
            file_type = "csv"
        else:
            df = pd.read_excel(io.BytesIO(raw))  # needs openpyxl
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            file_type = "xlsx"
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows")

    # Normalize columns
    original_cols = list(df.columns)
    new_cols = []
    seen = set()
    for c in original_cols:
        sc = _safe_col(str(c))
        if sc in seen:
            i = 2
            while f"{sc}_{i}" in seen:
                i += 1
            sc = f"{sc}_{i}"
        seen.add(sc)
        new_cols.append(sc)
    df.columns = new_cols

    schema = _user_schema(x_user_id)
    table = f"ds_{str(dataset_id).replace('-', '')}"

    # Upload raw file to GCS
    # Upload raw file (GCS if configured, else local)
    if bucket:
        object_name = f"{schema}/{dataset_id}/{filename}"
        gcs_uri = _upload_to_gcs(bucket, object_name, raw, content_type)
    else:
        gcs_uri = _save_local(schema, dataset_id, filename, raw)


    # Create schema + table + load data into Postgres
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

            # infer pg types
            col_defs = []
            cols_meta = []
            for i, col in enumerate(df.columns, start=1):
                pg_type = _infer_pg_type(df[col])
                col_defs.append(f'"{col}" {pg_type}')
                cols_meta.append((str(dataset_id), col, pg_type, i))

            cur.execute(f'CREATE TABLE "{schema}"."{table}" ({", ".join(col_defs)});')

            # COPY needs CSV text. For Excel, convert df -> CSV in memory
            csv_buf = io.StringIO()
            df.to_csv(csv_buf, index=False)
            csv_buf.seek(0)

             # Build quoted columns safely (NO nested f-strings)
            columns = ", ".join(f'"{c}"' for c in df.columns)
            copy_sql = f"""
            COPY "{schema}"."{table}" ({columns})
            FROM STDIN WITH CSV HEADER
            """

            cur.copy_expert(copy_sql, csv_buf)


            

            # metadata insert
            cur.execute(
                """
                INSERT INTO dataset_registry
                (dataset_id, user_id, dataset_name, original_filename, gcs_uri, file_type, table_schema_name, table_name, row_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (str(dataset_id), x_user_id, ds_name, filename, gcs_uri, file_type, schema, table, int(len(df)))
            )

            cur.executemany(
                "INSERT INTO dataset_columns(dataset_id, column_name, pg_type, ordinal_position) VALUES (%s,%s,%s,%s)",
                cols_meta
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
    finally:
        conn.close()

    return {
        "dataset_id": str(dataset_id),
        "dataset_name": ds_name,
        "user_id": x_user_id,
        "gcs_uri": gcs_uri,
        "table": f'{schema}.{table}',
        "row_count": int(len(df)),
        "columns": [{"name": c, "pg_type": _infer_pg_type(df[c])} for c in df.columns]
    }

@router.get("")
def list_datasets(x_user_id: str = Header(..., alias="X-User-Id")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT dataset_id, dataset_name, original_filename, gcs_uri, file_type, table_schema_name, table_name, row_count, created_at
                   FROM dataset_registry WHERE user_id=%s ORDER BY created_at DESC""",
                (x_user_id,)
            )
            rows = cur.fetchall()
        return {"count": len(rows), "data": [
            {
                "dataset_id": r[0], "dataset_name": r[1], "original_filename": r[2],
                "gcs_uri": r[3], "file_type": r[4],
                "table": f"{r[5]}.{r[6]}",
                "row_count": r[7], "created_at": str(r[8])
            } for r in rows
        ]}
    finally:
        conn.close()

@router.get("/{dataset_id}/schema")
def dataset_schema(dataset_id: str, x_user_id: str = Header(..., alias="X-User-Id")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dataset_registry WHERE dataset_id=%s AND user_id=%s", (dataset_id, x_user_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Dataset not found")

            cur.execute(
                """SELECT column_name, pg_type, ordinal_position
                   FROM dataset_columns WHERE dataset_id=%s ORDER BY ordinal_position""",
                (dataset_id,)
            )
            cols = cur.fetchall()
        return {"dataset_id": dataset_id, "columns": [{"name": c[0], "pg_type": c[1], "pos": c[2]} for c in cols]}
    finally:
        conn.close()

@router.get("/{dataset_id}/preview")
def dataset_preview(dataset_id: str, limit: int = 20, x_user_id: str = Header(..., alias="X-User-Id")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT table_schema_name, table_name FROM dataset_registry
                   WHERE dataset_id=%s AND user_id=%s""",
                (dataset_id, x_user_id)
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Dataset not found")
            schema, table = r[0], r[1]
            cur.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT %s', (limit,))
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
        return {"count": len(rows), "columns": colnames, "data": [dict(zip(colnames, row)) for row in rows]}
    finally:
        conn.close()

@router.get("/{dataset_id}/schema_prompt")
def dataset_schema_prompt(dataset_id: str, x_user_id: str = Header(..., alias="X-User-Id")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # validate dataset belongs to user + fetch table
            cur.execute(
                """SELECT table_schema_name, table_name
                   FROM dataset_registry
                   WHERE dataset_id=%s AND user_id=%s""",
                (dataset_id, x_user_id)
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Dataset not found")

            schema, table = r[0], r[1]
            table_fqn = f'{schema}.{table}'

            # fetch columns
            cur.execute(
                """SELECT column_name, pg_type
                   FROM dataset_columns
                   WHERE dataset_id=%s
                   ORDER BY ordinal_position""",
                (dataset_id,)
            )
            cols = [{"name": c[0], "pg_type": c[1]} for c in cur.fetchall()]

        prompt = build_schema_prompt(table_fqn, cols)
        return {"dataset_id": dataset_id, "table": table_fqn, "schema_prompt": prompt, "columns": cols}
    finally:
        conn.close()
