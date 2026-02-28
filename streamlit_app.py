# streamlit_app.py  — v4  (multi-tenant: JWT + API keys + audit log)
import io, json, re, requests
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime

API_BASE = "http://127.0.0.1:8000"

st.set_page_config(page_title="DB Assistant", page_icon="⬡",
                   layout="wide", initial_sidebar_state="expanded")

# ─────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Outfit:wght@300;400;500;600;700;800&display=swap');

:root {
  --bg:       #05080f;
  --s1:       #0b0f1a;
  --s2:       #111827;
  --s3:       #1a2235;
  --border:   #1e2d45;
  --border2:  #2a3d5a;
  --accent:   #00e5b4;
  --blue:     #3b82f6;
  --orange:   #f97316;
  --red:      #ef4444;
  --yellow:   #eab308;
  --text:     #e2eaf8;
  --muted:    #4a6080;
  --mono:     'JetBrains Mono', monospace;
  --sans:     'Outfit', sans-serif;
}

/* ── Base ── */
html, body, [class*="css"] { font-family: var(--sans); background: var(--bg) !important; color: var(--text) !important; }
.stApp { background: var(--bg) !important; }

/* Subtle grid background */
.stApp::before {
  content: '';
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  pointer-events: none; z-index: 0;
  background-image:
    linear-gradient(rgba(0,229,180,.018) 1px, transparent 1px),
    linear-gradient(90deg, rgba(0,229,180,.018) 1px, transparent 1px);
  background-size: 40px 40px;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
  background: #060e1a !important;
  border-right: 1px solid #0d1e30 !important;
  width: 260px !important;
}
[data-testid="stSidebar"] > div {
  padding-top: 0 !important;
  padding-left: .6rem !important;
  padding-right: .6rem !important;
}

/* Nav radio — style as clean nav list */
[data-testid="stSidebar"] .stRadio > div {
  gap: 2px !important;
}
[data-testid="stSidebar"] .stRadio label {
  background: transparent !important;
  border-radius: 8px !important;
  padding: 8px 12px !important;
  font-size: .82rem !important;
  color: #4a6080 !important;
  transition: background .15s, color .15s !important;
  cursor: pointer !important;
  width: 100% !important;
}
[data-testid="stSidebar"] .stRadio label:hover {
  background: rgba(255,255,255,.04) !important;
  color: #c9d1d9 !important;
}
[data-testid="stSidebar"] .stRadio [data-checked="true"] label,
[data-testid="stSidebar"] .stRadio label[data-checked="true"] {
  background: rgba(0,229,180,.08) !important;
  color: #00e5b4 !important;
  font-weight: 600 !important;
}
[data-testid="stSidebar"] .stRadio [aria-checked="true"] + label {
  background: rgba(0,229,180,.08) !important;
  color: #00e5b4 !important;
}
/* Hide the radio circle dots */
[data-testid="stSidebar"] .stRadio input[type="radio"] {
  display: none !important;
}
[data-testid="stSidebar"] .stRadio [data-baseweb="radio"] > div:first-child {
  display: none !important;
}

/* ── Typography ── */
h1 {
  font-family: var(--sans) !important;
  font-weight: 800 !important; font-size: 1.75rem !important;
  color: var(--text) !important; letter-spacing: -.5px !important;
  margin-bottom: 0 !important;
}
h2, h3, h4 { font-family: var(--sans) !important; font-weight: 600 !important; color: var(--text) !important; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
  background: var(--s2) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important; padding: 4px !important; gap: 2px !important;
}
.stTabs [data-baseweb="tab"] {
  background: transparent !important; color: var(--muted) !important;
  border-radius: 7px !important; font-family: var(--mono) !important;
  font-size: .71rem !important; font-weight: 500 !important;
  padding: 7px 16px !important; border: none !important;
  transition: all .15s !important;
}
.stTabs [aria-selected="true"] {
  background: var(--accent) !important; color: #05080f !important; font-weight: 700 !important;
}

/* ── Buttons ── */
.stButton > button {
  font-family: var(--mono) !important; font-size: .73rem !important;
  font-weight: 600 !important; letter-spacing: .5px !important;
  background: var(--accent) !important; color: #05080f !important;
  border: none !important; border-radius: 8px !important;
  padding: .5rem 1.2rem !important; transition: all .15s !important;
}
.stButton > button:hover { opacity: .88 !important; transform: translateY(-1px) !important; }
.stButton > button[kind="secondary"] {
  background: var(--s3) !important; color: var(--text) !important;
  border: 1px solid var(--border2) !important;
}

/* ── Inputs ── */
.stTextInput > div > div > input,
.stTextArea textarea,
.stNumberInput > div > div > input {
  background: var(--s2) !important; border: 1px solid var(--border) !important;
  border-radius: 8px !important; color: var(--text) !important;
  font-family: var(--mono) !important; font-size: .82rem !important;
}
.stTextInput > div > div > input:focus,
.stTextArea textarea:focus {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 2px rgba(0,229,180,.12) !important;
}
.stTextInput label, .stTextArea label, .stNumberInput label,
.stFileUploader label, .stCheckbox label, .stMultiSelect label {
  font-family: var(--mono) !important; font-size: .68rem !important;
  color: var(--muted) !important; text-transform: uppercase !important; letter-spacing: 1.2px !important;
}

/* ── Selectbox ── */
[data-baseweb="select"] > div {
  background: var(--s2) !important; border: 1px solid var(--border) !important;
  border-radius: 8px !important;
}

/* ── Metrics ── */
[data-testid="stMetric"] {
  background: var(--s2); border: 1px solid var(--border);
  border-radius: 10px; padding: .9rem 1.1rem;
}
[data-testid="stMetricLabel"] {
  font-family: var(--mono) !important; font-size: .63rem !important;
  color: var(--muted) !important; text-transform: uppercase !important; letter-spacing: 1px !important;
}
[data-testid="stMetricValue"] {
  font-family: var(--sans) !important; font-size: 1.6rem !important;
  font-weight: 700 !important; color: var(--accent) !important;
}

/* ── File uploader ── */
[data-testid="stFileUploader"] {
  background: var(--s2) !important;
  border: 1.5px dashed var(--border2) !important;
  border-radius: 10px !important;
}

/* ── DataFrame ── */
.stDataFrame { border-radius: 10px !important; overflow: hidden !important; border: 1px solid var(--border) !important; }

/* ── Alerts ── */
.stSuccess, .stInfo, .stWarning, .stError {
  border-radius: 8px !important; font-family: var(--mono) !important; font-size: .78rem !important;
}

/* ── Expanders ── */
.streamlit-expanderHeader {
  background: var(--s2) !important; border: 1px solid var(--border) !important;
  border-radius: 8px !important; font-family: var(--mono) !important; font-size: .78rem !important;
}

/* ── Divider ── */
hr { border-color: var(--border) !important; margin: .7rem 0 !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }

/* ── Auth ── */
.auth-card {
  background: var(--s1); border: 1px solid var(--border);
  border-radius: 16px; padding: 2.5rem; max-width: 420px; margin: 0 auto;
}

/* ── Cards ── */
.card {
  background: var(--s2); border: 1px solid var(--border);
  border-radius: 12px; padding: 1.1rem 1.4rem; margin-bottom: .6rem;
}
.card-title { font-family: var(--sans); font-weight: 600; font-size: .95rem; color: var(--text); }
.card-sub   { font-family: var(--mono); font-size: .68rem; color: var(--muted); margin-top: 4px; }

/* ── Badges ── */
.badge {
  display: inline-flex; align-items: center; padding: 2px 9px;
  border-radius: 20px; font-family: var(--mono); font-size: .63rem; font-weight: 600;
}
.b-ok   { background: rgba(0,229,180,.1);  color: #00e5b4; border: 1px solid rgba(0,229,180,.25); }
.b-err  { background: rgba(239,68,68,.1);  color: #ef4444; border: 1px solid rgba(239,68,68,.25); }
.b-warn { background: rgba(234,179,8,.1);  color: #eab308; border: 1px solid rgba(234,179,8,.25); }
.b-info { background: rgba(59,130,246,.1); color: #3b82f6; border: 1px solid rgba(59,130,246,.25); }
.b-def  { background: rgba(0,229,180,.15); color: #00e5b4; border: 1px solid rgba(0,229,180,.35); }

/* ── Key reveal ── */
.key-box {
  background: rgba(0,229,180,.05); border: 1.5px solid rgba(0,229,180,.35);
  border-radius: 10px; padding: 1.2rem 1.4rem; margin: 1rem 0;
}
.key-box-label { font-family: var(--mono); font-size: .63rem; color: var(--accent); text-transform: uppercase; letter-spacing: 2px; margin-bottom: .5rem; }
.key-box-value { font-family: var(--mono); font-size: .88rem; color: var(--text); word-break: break-all; }
.key-box-warn  { font-family: var(--mono); font-size: .65rem; color: var(--yellow); margin-top: .5rem; }

/* ── Section tag ── */
.section-tag {
  font-family: var(--mono); font-size: .62rem; color: var(--accent);
  text-transform: uppercase; letter-spacing: 2.5px;
  margin: 1.2rem 0 .5rem 0; padding-bottom: .3rem; border-bottom: 1px solid var(--border);
}

/* ── User pill ── */
.user-pill {
  background: rgba(0,229,180,.06); border: 1px solid rgba(0,229,180,.15);
  border-radius: 8px; padding: .55rem .85rem;
  font-family: var(--mono); font-size: .72rem; color: var(--accent);
}

/* ── Schema / table browser ── */
.schema-label {
  font-family: var(--mono); font-size: .65rem; color: var(--accent);
  text-transform: uppercase; letter-spacing: 1.5px;
  padding: .25rem .55rem; background: rgba(0,229,180,.07);
  border-radius: 5px; display: inline-block; margin: .35rem 0;
}
.sel-tables {
  background: rgba(0,229,180,.05); border: 1px solid rgba(0,229,180,.2);
  border-radius: 10px; padding: .75rem 1rem; margin: .5rem 0;
}

/* ── Deploy button special ── */
.deploy-btn > button {
  background: linear-gradient(135deg, #00e5b4, #3b82f6) !important;
  font-weight: 700 !important; font-size: .76rem !important;
}
</style>
""", unsafe_allow_html=True)

PLY = dict(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(11,15,26,.9)",
           font=dict(family="Syne",color="#c8d8f0",size=12),
           title_font=dict(family="Syne",color="#00d4a8",size=14),
           xaxis=dict(gridcolor="#1e2d4a",showgrid=True,zeroline=False),
           yaxis=dict(gridcolor="#1e2d4a",showgrid=True,zeroline=False),
           margin=dict(l=20,r=20,t=50,b=20),height=420,
           colorway=["#00e5b4","#3b82f6","#f97316","#eab308","#a78bfa","#fb7185"])

# ─────────────────────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────────────────────
def _hdrs():
    t = st.session_state.get("jwt_token","")
    return {"Authorization": f"Bearer {t}"} if t else {}

def api_get(path, params=None, timeout=20):
    try: return requests.get(f"{API_BASE}{path}", params=params, headers=_hdrs(), timeout=timeout).json()
    except Exception as e: return {"error": str(e)}

def api_post(path, payload=None, timeout=60):
    try: return requests.post(f"{API_BASE}{path}", json=payload, headers=_hdrs(), timeout=timeout).json()
    except Exception as e: return {"error": str(e)}

def api_post_form(path, data=None, files=None, timeout=60):
    try: return requests.post(f"{API_BASE}{path}", data=data, files=files, headers=_hdrs(), timeout=timeout).json()
    except Exception as e: return {"error": str(e)}

def api_delete(path, timeout=20):
    try: return requests.delete(f"{API_BASE}{path}", headers=_hdrs(), timeout=timeout).json()
    except Exception as e: return {"error": str(e)}

def build_uri(host, port, db, user, pw):
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"

def is_logged_in(): return bool(st.session_state.get("jwt_token"))

def logout():
    for k in ["jwt_token","user_id","user_email","user_name",
              "saved_connections","active_conn_id","active_conn_name",
              "pg_all_tables","pg_table_schemas","pg_uri_active","pg_uri_for_conn"]:
        st.session_state.pop(k,None)
    st.rerun()


# ─────────────────────────────────────────────────────────────
# LOGIN / REGISTER PAGE
# Uses st.form() — the ONLY reliable way to handle auth in Streamlit.
# st.tabs() + st.button() causes session_state loss on rerun. Avoided.
# ─────────────────────────────────────────────────────────────
def page_auth():
    # Logo
    st.markdown("""
    <div style="text-align:center;padding:2.5rem 0 2rem 0">
      <div style="font-family:'Outfit',sans-serif;font-size:2.6rem;font-weight:800;
          color:#00e5b4;letter-spacing:-1px">⬡ DB Assistant</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:.65rem;
          color:#2a3d5a;letter-spacing:3.5px;margin-top:8px;text-transform:uppercase">
          Multi-tenant · Secure · Powered by Gemini</div>
    </div>""", unsafe_allow_html=True)

    # Mode toggle — radio is 100% reliable, no state loss
    _, mc, _ = st.columns([1, 2, 1])
    with mc:
        mode = st.radio("auth_mode", ["🔑  Sign In", "✨  Create Account"],
                        horizontal=True, label_visibility="collapsed",
                        key="auth_mode")
        st.write("")

        # ── SIGN IN ──────────────────────────────────────────────
        if mode == "🔑  Sign In":
            with st.form("form_login", clear_on_submit=False):
                email = st.text_input("Email address", placeholder="you@example.com")
                pw    = st.text_input("Password",      placeholder="••••••••", type="password")
                submitted = st.form_submit_button("Sign In →", use_container_width=True)

            if submitted:
                email = email.strip()
                if not email or not pw:
                    st.error("Please enter both email and password.")
                else:
                    with st.spinner("Signing in…"):
                        try:
                            r = requests.post(
                                f"{API_BASE}/auth/login",
                                data={"username": email, "password": pw},
                                headers={"Content-Type": "application/x-www-form-urlencoded"},
                                timeout=10,
                            )
                            d = r.json()
                        except Exception as e:
                            st.error(f"Cannot reach server: {e}")
                            st.stop()
                    if r.status_code == 200:
                        st.session_state["jwt_token"]  = d["access_token"]
                        st.session_state["user_id"]    = d["user_id"]
                        st.session_state["user_email"] = d["email"]
                        st.session_state["user_name"]  = d.get("full_name") or d["email"].split("@")[0]
                        st.rerun()
                    else:
                        st.error(d.get("detail", "Invalid email or password."))

        # ── CREATE ACCOUNT ───────────────────────────────────────
        else:
            with st.form("form_register", clear_on_submit=False):
                full_name = st.text_input("Full name (optional)", placeholder="Your name")
                email     = st.text_input("Email address *",      placeholder="you@example.com")
                pw        = st.text_input("Password *",           placeholder="Min 6 characters", type="password")
                pw2       = st.text_input("Confirm password *",   placeholder="Repeat password",  type="password")
                submitted = st.form_submit_button("Create Account →", use_container_width=True)

            if submitted:
                email = email.strip()
                full_name = full_name.strip()
                # Validate
                if not email or not pw:
                    st.error("Email and password are required.")
                elif "@" not in email or "." not in email:
                    st.error("Please enter a valid email address.")
                elif len(pw) < 6:
                    st.error("Password must be at least 6 characters.")
                elif pw != pw2:
                    st.error("Passwords do not match.")
                else:
                    with st.spinner("Creating your account…"):
                        try:
                            r = requests.post(
                                f"{API_BASE}/auth/register",
                                json={"email": email, "password": pw,
                                      "full_name": full_name or None},
                                timeout=10,
                            )
                            d = r.json()
                        except Exception as e:
                            st.error(f"Cannot reach server: {e}")
                            st.stop()
                    if r.status_code == 201:
                        st.success("✅ Account created! Select **Sign In** above to log in.")
                    else:
                        st.error(d.get("detail", "Registration failed. Try again."))


# ─────────────────────────────────────────────────────────────
# MY CONNECTIONS PAGE
# ─────────────────────────────────────────────────────────────
def page_connections():
    # Always refresh from server
    conns = api_get("/auth/connections")
    if isinstance(conns, list):
        st.session_state["saved_connections"] = conns
    conns = st.session_state.get("saved_connections", [])

    # ── Saved connections ─────────────────────────────────────
    if conns:
        st.markdown('<p class="section-tag">Your Connections</p>', unsafe_allow_html=True)
        for c in conns:
            is_def = c.get("is_default", False)
            last   = (c.get("last_used_at") or "")[:16] or "Never"
            active = st.session_state.get("active_conn_id") == c["id"]

            # Pre-compute all values outside f-string (no backslashes allowed in Python 3.10 f-strings)
            is_mongo     = c.get("db_type", "postgresql") == "mongodb"
            border       = "2px solid #00d4a8" if active else "1px solid #1e2d4a"
            db_icon      = "🍃" if is_mongo else "🐘"
            default_html = (
                '&nbsp;<span style="background:rgba(0,212,168,.15);color:#00d4a8;'
                'border:1px solid rgba(0,212,168,.4);border-radius:20px;'
                'padding:2px 10px;font-size:.62rem;font-weight:700">DEFAULT</span>'
            ) if is_def else ""
            active_html = (
                '&nbsp;<span style="background:rgba(0,153,255,.15);color:#60a5fa;'
                'border:1px solid rgba(0,153,255,.4);border-radius:20px;'
                'padding:2px 10px;font-size:.62rem;font-weight:700">ACTIVE</span>'
            ) if active else ""
            mongo_badge = (
                '&nbsp;<span style="background:rgba(0,200,83,.12);color:#4ade80;'
                'border:1px solid rgba(0,200,83,.3);border-radius:20px;'
                'padding:2px 10px;font-size:.62rem;font-weight:700">MONGODB</span>'
            ) if is_mongo else ""
            conn_name  = c["name"]
            conn_db    = c["dbname"]
            conn_user  = c["db_username"]
            # For MongoDB, show the URI host cleanly; for PG show host:port
            if is_mongo:
                raw_host = c["host"]
                # Trim long URIs for display
                conn_detail = raw_host[:60] + "…" if len(raw_host) > 60 else raw_host
            else:
                conn_detail = str(c["host"]) + ":" + str(c["port"]) + " / " + conn_db

            st.markdown(f"""
            <div style="background:#141c2e;border:{border};border-radius:14px;
                        padding:1rem 1.4rem;margin-bottom:.5rem;">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
                <span style="font-family:Syne,sans-serif;font-weight:700;font-size:.95rem">
                  {db_icon} {conn_name}
                </span>
                {mongo_badge}{default_html}{active_html}
              </div>
              <div style="font-family:IBM Plex Mono,monospace;font-size:.7rem;color:#566a8a">
                📍 {conn_detail}
                &nbsp;·&nbsp; 👤 {conn_user}
                &nbsp;·&nbsp; 🔒 AES-256 encrypted
                &nbsp;·&nbsp; 🕐 {last}
              </div>
            </div>""", unsafe_allow_html=True)

            col1, col2, col3, _ = st.columns([1.2, 1.2, 1, 4])

            # ACTIVATE
            if col1.button("▶ Activate", key=f"act_{c['id']}",
                           type="primary" if not active else "secondary"):
                st.session_state["active_conn_id"]   = c["id"]
                st.session_state["active_conn_name"] = c["name"]
                st.session_state.pop("pg_uri_for_conn", None)
                st.session_state.pop("pg_all_tables", None)
                st.rerun()

            # SET DEFAULT — uses a dedicated backend endpoint, no password needed
            if not is_def:
                if col2.button("⭐ Default", key=f"def_{c['id']}"):
                    out = api_post(f"/auth/connections/{c['id']}/set-default", {})
                    if "message" in out:
                        st.session_state.pop("saved_connections", None)
                        st.rerun()
                    else:
                        st.error(out.get("detail", "Could not set default."))

            # DELETE
            if col3.button("🗑", key=f"del_{c['id']}",
                           help=f"Delete {c['name']}"):
                out = api_delete(f"/auth/connections/{c['id']}")
                if "message" in out:
                    if st.session_state.get("active_conn_id") == c["id"]:
                        for k in ["active_conn_id","active_conn_name",
                                  "pg_uri_for_conn","pg_all_tables","pg_uri_active"]:
                            st.session_state.pop(k, None)
                    st.session_state.pop("saved_connections", None)
                    st.rerun()
                else:
                    st.error(out.get("detail", "Delete failed."))

    else:
        st.info("No saved connections yet — add one below.")

    # ── Add new connection ────────────────────────────────────
    st.divider()
    st.markdown('<p class="section-tag">➕ Add New Connection</p>', unsafe_allow_html=True)

    # DB type selector — outside the form so it controls what fields show
    db_type = st.radio(
        "Database type",
        ["🐘  PostgreSQL", "🍃  MongoDB"],
        horizontal=True,
        key="new_conn_db_type",
        label_visibility="collapsed",
    )
    st.write("")
    is_mongo = db_type == "🍃  MongoDB"

    # ── PostgreSQL form ───────────────────────────────────────
    if not is_mongo:
        with st.form("form_add_pg", clear_on_submit=True):
            f_name = st.text_input("Connection name *",
                                   placeholder="e.g. My Production DB")
            col1, col2 = st.columns([3, 1])
            f_host = col1.text_input("Host", value="127.0.0.1")
            f_port = col2.number_input("Port", value=5433, min_value=1, max_value=65535)
            col3, col4, col5 = st.columns(3)
            f_db   = col3.text_input("Database", value="da_db")
            f_user = col4.text_input("Username", value="da_user")
            f_pw   = col5.text_input("Password", type="password", placeholder="required")
            f_def  = st.checkbox("Set as default connection")
            save_pg = st.form_submit_button("🔌 Test & Save PostgreSQL",
                                            use_container_width=True)

        if save_pg:
            f_name = f_name.strip()
            if not f_name:
                st.error("❌ Connection name is required.")
            elif not f_pw:
                st.error("❌ Password is required.")
            elif not f_db:
                st.error("❌ Database name is required.")
            else:
                pg_uri = build_uri(f_host, int(f_port), f_db, f_user, f_pw)
                with st.spinner(f"Testing {f_host}:{f_port}/{f_db}…"):
                    ping = api_post("/pg/ping", {"pg_uri": pg_uri})
                if "error" in ping or "detail" in ping:
                    st.error(f"❌ Connection failed: {ping.get('detail') or ping.get('error')}")
                else:
                    with st.spinner("Saving…"):
                        out = api_post("/auth/connections", {
                            "name": f_name, "db_type": "postgresql",
                            "host": f_host, "port": int(f_port),
                            "dbname": f_db, "db_username": f_user,
                            "password": f_pw, "is_default": f_def,
                        })
                    if "id" in out:
                        st.success(f"✅ **{f_name}** saved!")
                        st.session_state.pop("saved_connections", None)
                        st.rerun()
                    else:
                        st.error(f"❌ {out.get('detail', 'Save failed')}")

    # ── MongoDB form ──────────────────────────────────────────
    else:
        with st.form("form_add_mongo", clear_on_submit=True):
            m_name = st.text_input("Connection name *",
                                   placeholder="e.g. Local MongoDB, Atlas Cluster")
            st.markdown("---")

            # Individual fields — same UX as PostgreSQL
            mc1, mc2 = st.columns([3, 1])
            m_host = mc1.text_input("Host", value="localhost")
            m_port = mc2.number_input("Port", value=27017, min_value=1, max_value=65535)

            mc3, mc4, mc5 = st.columns(3)
            m_db   = mc3.text_input("Database *", value="sales_db",
                                    placeholder="e.g. sales_db")
            m_user = mc4.text_input("Username",
                                    placeholder="blank if no auth")
            m_pw   = mc5.text_input("Password", type="password",
                                    placeholder="blank if no auth")

            m_def  = st.checkbox("Set as default connection")
            save_mg = st.form_submit_button("🔌 Test Connection & Save",
                                            use_container_width=True)

        if save_mg:
            m_name = m_name.strip()
            m_db   = m_db.strip()
            if not m_name:
                st.error("❌ Connection name is required.")
            elif not m_db:
                st.error("❌ Database name is required.")
            else:
                # Build URI from individual fields
                if m_user and m_pw:
                    final_uri = f"mongodb://{m_user}:{m_pw}@{m_host}:{int(m_port)}"
                else:
                    final_uri = f"mongodb://{m_host}:{int(m_port)}"

                # Test first
                with st.spinner(f"Testing {m_host}:{int(m_port)}…"):
                    ping = api_post("/mongo/ping-uri", {"mongo_uri": final_uri})

                if "error" in ping or "detail" in ping:
                    st.error(f"❌ Cannot connect: {ping.get('detail') or ping.get('error')}")
                    st.info("Check host, port, username and password.")
                else:
                    dbs = ping.get("databases", [])
                    if dbs:
                        st.success(f"✅ Connected! Databases found: {', '.join(dbs[:8])}")
                    with st.spinner("Saving connection…"):
                        out = api_post("/auth/connections", {
                            "name":        m_name,
                            "db_type":     "mongodb",
                            "host":        final_uri,   # full URI stored here
                            "port":        int(m_port),
                            "dbname":      m_db,
                            "db_username": m_user or "",
                            "password":    final_uri,   # AES-256 encrypted copy
                            "is_default":  m_def,
                        })
                    if "id" in out:
                        st.success(f"✅ **{m_name}** saved!")
                        st.session_state.pop("saved_connections", None)
                        st.rerun()
                    else:
                        st.error(f"❌ {out.get('detail', 'Save failed')}") 


# ─────────────────────────────────────────────────────────────
# API KEYS PAGE
# ─────────────────────────────────────────────────────────────
def page_audit_log():
    lc, _ = st.columns([1,4])
    limit = lc.number_input("Show last N entries", 10, 200, 50, key="audit_limit")

    if st.button("🔄 Load Audit Log", use_container_width=False):
        out = api_get("/auth/audit-log", params={"limit": limit})
        st.session_state["audit_log"] = out if isinstance(out, list) else []

    entries = st.session_state.get("audit_log", [])
    if not entries:
        st.info("Click Load Audit Log to see your query history.")
        return

    # Summary metrics
    df_audit = pd.DataFrame(entries)
    total   = len(df_audit)
    success = (df_audit["status"] == "success").sum()
    errors  = total - success
    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Total Queries", total)
    m2.metric("Successful",    success)
    m3.metric("Errors",        errors)
    m4.metric("Unique Types",  df_audit["query_type"].nunique())

    st.divider()

    # Filter bar
    types = ["All"] + sorted(df_audit["query_type"].unique().tolist())
    fc1,fc2 = st.columns([2,4])
    sel_type   = fc1.selectbox("Filter by type", types, key="audit_type_filter")
    sel_status = fc2.radio("Status", ["All","success","error"], horizontal=True, key="audit_status_filter")

    mask = pd.Series([True]*len(df_audit))
    if sel_type   != "All":     mask &= df_audit["query_type"] == sel_type
    if sel_status != "All":     mask &= df_audit["status"]     == sel_status
    filtered = df_audit[mask]

    # Table
    st.markdown(f'<p class="section-tag">Showing {len(filtered)} of {total} entries</p>', unsafe_allow_html=True)

    for _, row in filtered.iterrows():
        is_ok      = row["status"] == "success"
        icon       = "✅" if is_ok else "❌"
        tables_str = ", ".join(row.get("table_names") or []) or "—"
        ts         = str(row.get("created_at",""))[:16]
        qtype      = row.get("query_type", "—")

        with st.expander(f"{icon} {qtype}  ·  {tables_str}  ·  {ts}"):
            sc  = "#00e5b4" if is_ok else "#ef4444"
            sbg = "rgba(0,229,180,.08)" if is_ok else "rgba(239,68,68,.08)"
            st.markdown(
                f'<div style="display:flex;gap:.5rem;align-items:center;margin-bottom:.7rem">' +
                f'<span style="background:{sbg};color:{sc};border:1px solid {sc}40;' +
                f'border-radius:20px;padding:2px 10px;font-family:JetBrains Mono,monospace;' +
                f'font-size:.65rem;font-weight:700">{"SUCCESS" if is_ok else "ERROR"}</span>' +
                f'<span style="font-family:JetBrains Mono,monospace;font-size:.68rem;color:#4a6080">{ts}</span>' +
                f'</div>',
                unsafe_allow_html=True
            )
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rows",   row.get("row_count") or "—")
            c2.metric("ms",     row.get("execution_ms") or "—")
            c3.metric("Type",   qtype)
            c4.metric("Tables", tables_str[:30])
            if row.get("question"):
                st.markdown(f"**❓ Question:** {row['question']}")
            if row.get("sql_generated"):
                st.code(row["sql_generated"], language="sql")
            if row.get("error_detail"):
                st.error(row["error_detail"])


# ─────────────────────────────────────────────────────────────
# ACTIVE CONNECTION LOADER
# ─────────────────────────────────────────────────────────────
def load_active_connection() -> tuple:
    """Returns (pg_uri, conn_name) or (None, None). Loads tables if needed."""
    conn_id   = st.session_state.get("active_conn_id")
    conn_name = st.session_state.get("active_conn_name", "")

    if not conn_id:
        st.warning("👈 Go to **🔌 My Connections** to save and activate a database connection first.")
        return None, None

    if st.session_state.get("pg_uri_for_conn") != conn_id:
        with st.spinner(f"Loading **{conn_name}**…"):
            out = api_post("/pg/all-tables-by-id", {"connection_id": conn_id})
        if "error" in out or "detail" in out:
            st.error(out.get("detail") or out.get("error"))
            return None, None
        st.session_state["pg_all_tables"]    = out.get("tables", [])
        st.session_state["pg_table_schemas"] = out.get("schemas", {})
        st.session_state["pg_uri_active"]    = out.get("pg_uri")
        st.session_state["pg_uri_for_conn"]  = conn_id

    return st.session_state.get("pg_uri_active"), conn_name


# ─────────────────────────────────────────────────────────────
# TABLE BROWSER
# ─────────────────────────────────────────────────────────────
def table_browser(key: str = "tb") -> list:
    all_t  = st.session_state.get("pg_all_tables", [])
    schs   = st.session_state.get("pg_table_schemas", {})
    if not all_t:
        return []

    st.markdown(f'<p class="section-tag">📋 Tables  <span style="color:var(--dim);font-size:.6rem">({len(all_t)} found)</span></p>', unsafe_allow_html=True)
    b1,b2,_ = st.columns([1,1,6])
    if b1.button("☑ All",  key=f"{key}_all"):
        for t in all_t: st.session_state[f"{key}_ck_{t['fqn']}"] = True
    if b2.button("☐ None", key=f"{key}_none"):
        for t in all_t: st.session_state[f"{key}_ck_{t['fqn']}"] = False

    selected = []
    for schema, tables in schs.items():
        st.markdown(f'<span class="schema-label">📁 {schema}</span>', unsafe_allow_html=True)
        for i in range(0, len(tables), 3):
            cols = st.columns(3)
            for col, t in zip(cols, tables[i:i+3]):
                rows_label = f"~{t['approx_rows']:,}" if t['approx_rows'] else "empty"
                if col.checkbox(f"**{t['table']}** `{rows_label}`",
                                value=st.session_state.get(f"{key}_ck_{t['fqn']}", False),
                                key=f"{key}_ck_{t['fqn']}"):
                    selected.append(t["fqn"])

    if selected:
        join_label = f"JOIN across {len(selected)} tables" if len(selected)>1 else "single table"
        names = "  ·  ".join(f"`{t.split('.')[-1]}`" for t in selected)
        st.markdown(f"""<div class="sel-tables">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--accent);text-transform:uppercase;letter-spacing:2px">✓ {len(selected)} selected — {join_label}</div>
          <div style="font-family:'IBM Plex Mono',monospace;font-size:.75rem;color:#c8d8f0;margin-top:4px">{names}</div>
        </div>""", unsafe_allow_html=True)
    return selected


# ─────────────────────────────────────────────────────────────
# VISUALIZE & RESULTS
# ─────────────────────────────────────────────────────────────
def _clean_df(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Clean a DataFrame before charting:
    - Rename '_id' to a human-friendly label (e.g. 'name', 'region', 'category')
      by inspecting the values. ObjectId hex strings → drop the column entirely
      since they can't be used as chart labels.
    - Convert any stringified numbers to numeric.
    """
    df = df.copy()

    # Rename _id to a sensible name based on content
    if "_id" in df.columns:
        sample = df["_id"].dropna().astype(str).head(5).tolist()
        # If values look like ObjectId hex (24 hex chars) — drop the column
        import re as _re
        if all(_re.match(r"^[0-9a-f]{24}$", v) for v in sample if v):
            df = df.drop(columns=["_id"])
        else:
            # Rename to something meaningful
            # Guess label from other column names or just call it "group"
            other_cols = [c for c in df.columns if c != "_id"]
            label = "group"
            if other_cols:
                # pick a name that sounds like a dimension
                for hint in ["name","region","country","city","category",
                             "status","tier","type","rep","product","method"]:
                    if any(hint in c.lower() for c in other_cols):
                        label = hint
                        break
            df = df.rename(columns={"_id": label})

    # Try to coerce object columns that look numeric
    for col in df.select_dtypes(include="object").columns:
        try:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() > len(df) * 0.8:
                df[col] = converted
        except Exception:
            pass

    return df


def _visualize_custom(df, prefix="v", suppress_auto=False):
    """
    Like visualize() but when suppress_auto=True, does not render
    when chart is set to Auto (avoids duplicate of agent chart).
    """
    df = _clean_df(df)
    num  = df.select_dtypes(include="number").columns.tolist()
    cat  = df.select_dtypes(include=["object","category"]).columns.tolist()
    date = [c for c in df.columns if any(k in c.lower() for k in ["date","time","month","year"])]
    if not num:
        return

    default_x = cat[0] if cat else (date[0] if date else num[0])
    default_y = num[0]

    c1, c2, c3, c4 = st.columns(4)
    chart = c1.selectbox("Chart", ["Auto","Bar","Line","Pie","Scatter","Area"], key=f"{prefix}_ct2")
    all_x = cat + date + num
    x_idx = all_x.index(default_x) if default_x in all_x else 0
    x = c2.selectbox("X axis", all_x, index=x_idx, key=f"{prefix}_x2")
    y_idx = num.index(default_y) if default_y in num else 0
    y = c3.selectbox("Y axis", num, index=y_idx, key=f"{prefix}_y2")
    clr   = c4.selectbox("Color", ["None"] + cat, key=f"{prefix}_clr2")
    color = None if clr == "None" else clr

    # If agent already showed Auto chart, skip rendering until user picks a specific type
    if suppress_auto and chart == "Auto":
        st.caption("☝️ Select a chart type above to explore further.")
        return

    eff = chart
    if chart == "Auto":
        if x in date:               eff = "Line"
        elif df[x].nunique() <= 15: eff = "Bar"
        else:                       eff = "Scatter"

    try:
        kw  = dict(color=color) if color else {}
        fig = None
        title = f"{y} by {x}"
        if eff == "Bar":
            fig = px.bar(df, x=x, y=y, title=title, **kw)
            fig.update_layout(xaxis_tickangle=-35)
        elif eff == "Line":
            fig = px.line(df, x=x, y=y, title=title, markers=True, **kw)
        elif eff == "Pie":
            fig = px.pie(df, names=x, values=y, title=title, hole=.4)
        elif eff == "Scatter":
            fig = px.scatter(df, x=x, y=y, title=title, **kw)
        elif eff == "Area":
            fig = px.area(df, x=x, y=y, title=title, **kw)
        if fig:
            fig.update_layout(**PLY)
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Chart error: {e}")


def visualize(df, prefix="v"):
    df = _clean_df(df)
    num  = df.select_dtypes(include="number").columns.tolist()
    cat  = df.select_dtypes(include=["object","category"]).columns.tolist()
    date = [c for c in df.columns if any(k in c.lower() for k in ["date","time","month","year"])]
    if not num:
        st.info("No numeric columns available to chart.")
        return

    # Smart defaults: pick best X and Y automatically
    default_x = cat[0] if cat else (date[0] if date else num[0])
    default_y = num[0]

    c1, c2, c3, c4 = st.columns(4)
    chart = c1.selectbox("Chart", ["Auto","Bar","Line","Pie","Scatter","Area"], key=f"{prefix}_ct")

    all_x = cat + date + num
    x_idx = all_x.index(default_x) if default_x in all_x else 0
    x = c2.selectbox("X axis", all_x, index=x_idx, key=f"{prefix}_x")

    y_idx = num.index(default_y) if default_y in num else 0
    y = c3.selectbox("Y axis", num, index=y_idx, key=f"{prefix}_y")

    clr   = c4.selectbox("Color", ["None"] + cat, key=f"{prefix}_clr")
    color = None if clr == "None" else clr

    eff = chart
    if chart == "Auto":
        if x in date:           eff = "Line"
        elif df[x].nunique() <= 15: eff = "Bar"
        else:                   eff = "Scatter"

    try:
        kw  = dict(color=color) if color else {}
        fig = None
        title = f"{y} by {x}"
        if eff == "Bar":
            fig = px.bar(df, x=x, y=y, title=title, **kw)
            fig.update_layout(xaxis_tickangle=-35)
        elif eff == "Line":
            fig = px.line(df, x=x, y=y, title=title, markers=True, **kw)
        elif eff == "Pie":
            fig = px.pie(df, names=x, values=y, title=title, hole=.4)
        elif eff == "Scatter":
            fig = px.scatter(df, x=x, y=y, title=title, **kw)
        elif eff == "Area":
            fig = px.area(df, x=x, y=y, title=title, **kw)
        if fig:
            fig.update_layout(**PLY)
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Chart error: {e}")


def _agent_chart(df, viz, key):
    """Render the agent-recommended chart (used inside Charts tab)."""
    import plotly.express as px
    if not viz or df.empty:
        return False
    chart_type = viz.get("type", "bar")
    category   = viz.get("category")
    value      = viz.get("value")
    title      = viz.get("title", "")
    kw = dict(
        template="plotly_dark",
        color_discrete_sequence=["#00e5b4","#3b82f6","#f59e0b","#ef4444","#8b5cf6"],
    )
    try:
        if chart_type == "bar" and category and value:
            fig = px.bar(df, x=category, y=value, title=f"📊 {title}", **kw)
        elif chart_type == "line" and category and value:
            fig = px.line(df, x=category, y=value, title=f"📈 {title}", markers=True, **kw)
        elif chart_type == "pie" and category and value:
            fig = px.pie(df, names=category, values=value, title=f"🥧 {title}", hole=.4)
            fig.update_layout(template="plotly_dark")
        else:
            return False
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#c9d1d9",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True, key=key)
        return True
    except Exception:
        return False


def show_results(out, prefix="r"):
    data    = out.get("data") or []
    df      = pd.DataFrame(data)
    summary = out.get("summary")
    viz     = out.get("viz")
    profile = out.get("profile")

    t1, t2, t3, t4 = st.tabs(["📋 Table", "📊 Charts & Insights", "🔍 EDA Profile", "🔧 Debug"])

    # ── Tab 1: Table ─────────────────────────────────────────
    with t1:
        if df.empty:
            st.warning("No results.")
        else:
            display_df = _clean_df(df)
            st.dataframe(display_df, use_container_width=True, height=360)
            st.download_button("⬇️ Download CSV", display_df.to_csv(index=False),
                file_name=f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv", key=f"dl_{prefix}")

    # ── Tab 2: Charts & Insights (merged — no duplicate) ────
    with t2:
        if df.empty:
            st.warning("No data to visualize.")
        else:
            # AI Insight summary banner
            if summary:
                # Build metric cards for quick stats extracted from summary
                st.markdown(f"""
                <div style="background:linear-gradient(135deg,#0d2137,#0a1929);
                    border-left:3px solid #00e5b4;border-radius:8px;
                    padding:16px 20px;margin-bottom:16px">
                  <div style="font-size:.68rem;color:#00e5b4;font-weight:700;
                      letter-spacing:2px;margin-bottom:8px">💡 AI INSIGHT</div>
                  <div style="color:#e2e8f0;font-size:.92rem;line-height:1.7;
                      font-family:'IBM Plex Sans',sans-serif">{summary}</div>
                </div>""", unsafe_allow_html=True)

            # Agent-recommended chart (single render here only)
            agent_chart_shown = _agent_chart(df, viz, key=f"agent_viz_{prefix}")

            # Manual chart explorer — only renders if user changes from Auto
            st.markdown("---")
            st.markdown(
                "<div style='font-size:.75rem;color:#4a6080;margin-bottom:6px'>"
                "🛠️ Custom chart explorer — change chart type to explore</div>",
                unsafe_allow_html=True
            )
            # Pass agent_chart_shown so visualize knows not to render Auto again
            _visualize_custom(df, prefix, suppress_auto=agent_chart_shown)

    # ── Tab 3: EDA Profile ───────────────────────────────────
    with t3:
        if not profile or not profile.get("columns"):
            st.markdown("""
            <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
                padding:60px 20px;text-align:center">
              <div style="width:56px;height:56px;background:linear-gradient(135deg,#0d1f35,#1a2d45);
                  border-radius:16px;display:flex;align-items:center;justify-content:center;
                  font-size:1.6rem;margin-bottom:16px;border:1px solid #1e3a5f">🔍</div>
              <div style="font-size:.95rem;color:#4a6080;font-weight:500">No profile data available</div>
              <div style="font-size:.75rem;color:#2d4a6b;margin-top:6px">Run a query to see column analysis</div>
            </div>""", unsafe_allow_html=True)
        else:
            col_profiles = profile.get("columns", [])
            total_rows   = profile.get("total_rows", 0)
            total_cols   = profile.get("total_cols", 0)
            warnings     = profile.get("warnings", [])
            num_cols_p   = [p for p in col_profiles if p.get("type") == "numeric"]
            txt_cols_p   = [p for p in col_profiles if p.get("type") == "text"]
            raw_data     = out.get("data") or []

            def _fmtn(v):
                try:
                    f = float(v)
                    return f"{int(f):,}" if f == int(f) else f"{f:,.2f}"
                except Exception:
                    return "—"

            # ── 1. Overview banner ───────────────────────────
            total_nulls  = sum(p.get("nulls", 0) for p in col_profiles)
            completeness = round((1 - total_nulls / max(total_rows * total_cols, 1)) * 100, 1)
            health_c  = "#00e5b4" if completeness == 100 else ("#f59e0b" if completeness >= 80 else "#ef4444")
            health_bg = "#001a12" if completeness == 100 else ("#1a1200" if completeness >= 80 else "#1a0505")
            health_bd = "#00e5b430" if completeness == 100 else ("#f59e0b30" if completeness >= 80 else "#ef444430")

            tiles = [
                ("📊", f"{total_rows:,}", "Rows",        "#e2e8f0", "#0d1f35", "#1a2d45"),
                ("⬡",  str(total_cols),   "Columns",     "#e2e8f0", "#0d1f35", "#1a2d45"),
                ("∑",  str(len(num_cols_p)), "Numeric",  "#60a5fa", "#051428", "#1e3a5f40"),
                ("◈",  str(len(txt_cols_p)), "Text",     "#a78bfa", "#0e0521", "#5b21b640"),
                ("✦",  f"{completeness}%", "Complete",  health_c,  health_bg, health_bd),
            ]

            st.markdown("<div style='display:flex;gap:10px;margin-bottom:20px'>", unsafe_allow_html=True)
            tile_cols = st.columns(5)
            for i, (icon, val, label, color, bg, border) in enumerate(tiles):
                tile_cols[i].markdown(f"""
                <div style="background:{bg};border:1px solid {border};border-radius:14px;
                    padding:18px 12px;text-align:center;position:relative;overflow:hidden">
                  <div style="font-size:1.6rem;font-weight:900;color:{color};
                      letter-spacing:-1px;line-height:1;font-variant-numeric:tabular-nums">{val}</div>
                  <div style="font-size:.6rem;color:#4a6080;margin-top:6px;
                      letter-spacing:1.5px;text-transform:uppercase;font-weight:600">{label}</div>
                </div>""", unsafe_allow_html=True)

            # ── 2. Gemini EDA Insights ──────────────────────
            eda = out.get("eda_insights") or {}
            if eda:
                headline   = eda.get("headline", "")
                findings   = eda.get("key_findings", [])
                dq         = eda.get("data_quality", {})
                col_ins    = eda.get("column_insights", [])
                recs       = eda.get("recommendations", [])

                # Headline banner
                if headline:
                    st.markdown(f"""
                    <div style="background:linear-gradient(135deg,#051428,#0a1f3a);
                        border:1px solid #1e3a5f;border-left:4px solid #60a5fa;
                        border-radius:10px;padding:14px 18px;margin-bottom:16px">
                      <div style="font-size:.55rem;color:#2d4a6b;letter-spacing:2px;
                          text-transform:uppercase;font-weight:700;margin-bottom:6px">
                        🤖 Gemini Analysis
                      </div>
                      <div style="font-size:.95rem;color:#dde5f0;font-weight:600;
                          line-height:1.4">{headline}</div>
                    </div>""", unsafe_allow_html=True)

                # Key findings + DQ score side by side
                f_col, dq_col = st.columns([3, 1])

                with f_col:
                    if findings:
                        finds_html = """<div style="background:#080f1a;border:1px solid #111f33;
                            border-radius:10px;padding:14px 18px;height:100%">
                          <div style="font-size:.55rem;color:#2d4a6b;letter-spacing:2px;
                              text-transform:uppercase;font-weight:700;margin-bottom:10px">
                            Key Findings
                          </div>"""
                        for i, f in enumerate(findings[:3]):
                            finds_html += f"""
                          <div style="display:flex;gap:10px;margin-bottom:8px;
                              align-items:flex-start">
                            <span style="font-size:.65rem;color:#60a5fa;font-weight:800;
                                background:#051428;border:1px solid #1e3a5f;border-radius:4px;
                                padding:2px 6px;min-width:20px;text-align:center;
                                margin-top:1px">{i+1}</span>
                            <span style="font-size:.78rem;color:#c9d1d9;line-height:1.5">{f}</span>
                          </div>"""
                        finds_html += "</div>"
                        st.markdown(finds_html, unsafe_allow_html=True)

                with dq_col:
                    if dq:
                        score = dq.get("score", 100)
                        verdict = dq.get("verdict", "")
                        dq_color = "#00e5b4" if score >= 90 else ("#f59e0b" if score >= 70 else "#ef4444")
                        dq_bg = "#001a12" if score >= 90 else ("#1a1200" if score >= 70 else "#1a0505")
                        st.markdown(f"""
                        <div style="background:{dq_bg};border:1px solid {dq_color}25;
                            border-radius:10px;padding:14px;text-align:center;height:100%">
                          <div style="font-size:.55rem;color:#2d4a6b;letter-spacing:2px;
                              text-transform:uppercase;font-weight:700;margin-bottom:8px">
                            Data Quality
                          </div>
                          <div style="font-size:2rem;font-weight:900;color:{dq_color};
                              line-height:1">{score}</div>
                          <div style="font-size:.6rem;color:{dq_color}80;margin-bottom:8px">/100</div>
                          <div style="font-size:.68rem;color:#4a6080;line-height:1.4">{verdict}</div>
                        </div>""", unsafe_allow_html=True)

                # Column insights
                if col_ins:
                    st.markdown("""
                    <div style="font-size:.55rem;color:#2d4a6b;letter-spacing:2px;
                        text-transform:uppercase;font-weight:700;margin:14px 0 8px">
                      Column Insights
                    </div>""", unsafe_allow_html=True)
                    ci_html = "<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px'>"
                    for ci in col_ins[:4]:
                        ci_html += f"""
                        <div style="background:#080f1a;border:1px solid #111f33;
                            border-radius:8px;padding:12px 14px">
                          <div style="font-size:.65rem;color:#a78bfa;font-weight:700;
                              font-family:monospace;margin-bottom:5px">{ci.get('col','')}</div>
                          <div style="font-size:.75rem;color:#8a9ab5;line-height:1.4">{ci.get('insight','')}</div>
                        </div>"""
                    ci_html += "</div>"
                    st.markdown(ci_html, unsafe_allow_html=True)

                # Recommendations
                if recs:
                    recs_html = "<div style='margin-top:12px'>"
                    for rec in recs[:2]:
                        recs_html += f"""
                        <div style="display:flex;gap:10px;align-items:flex-start;
                            background:#0a1500;border:1px solid #00e5b420;
                            border-left:3px solid #00e5b4;border-radius:8px;
                            padding:10px 14px;margin-bottom:6px">
                          <span style="font-size:.8rem">💡</span>
                          <span style="font-size:.75rem;color:#9dc8a8;line-height:1.4">{rec}</span>
                        </div>"""
                    recs_html += "</div>"
                    st.markdown(recs_html, unsafe_allow_html=True)

                st.markdown("<div style='margin:20px 0;border-top:1px solid #0f1f35'></div>", unsafe_allow_html=True)

            # ── 3. Warnings / health — only show if no Gemini insights ─────
            if warnings and not eda:
                warn_html = "<div style='display:flex;flex-direction:column;gap:6px;margin-bottom:20px'>"
                for w in warnings:
                    warn_html += f"""
                    <div style="display:flex;align-items:center;gap:10px;
                        background:#1a1200;border:1px solid #f59e0b25;
                        border-left:3px solid #f59e0b;border-radius:8px;padding:10px 14px">
                      <span style="font-size:.85rem">⚠</span>
                      <span style="font-size:.78rem;color:#d4a843;line-height:1.4">{w}</span>
                    </div>"""
                warn_html += "</div>"
                st.markdown(warn_html, unsafe_allow_html=True)
            elif not eda:
                st.markdown("""
                <div style="display:flex;align-items:center;gap:8px;
                    background:#001a12;border:1px solid #00e5b420;
                    border-left:3px solid #00e5b4;border-radius:8px;
                    padding:10px 14px;margin-bottom:20px">
                  <span style="font-size:.85rem">✓</span>
                  <span style="font-size:.78rem;color:#00e5b4">No data quality issues detected</span>
                </div>""", unsafe_allow_html=True)

            # ── 3. Column grid ───────────────────────────────
            st.markdown("""
            <div style="font-size:.62rem;color:#2d4a6b;letter-spacing:2px;
                text-transform:uppercase;font-weight:700;margin-bottom:14px;
                padding-bottom:8px;border-bottom:1px solid #0f1f35">
              Column Analysis
            </div>""", unsafe_allow_html=True)

            for p in col_profiles:
                cname    = p.get("col", "")
                ctype    = p.get("type", "")
                null_pct = p.get("null_pct", 0)
                n_unique = p.get("unique", 0)

                is_num   = ctype == "numeric"
                badge_c  = "#60a5fa" if is_num else "#a78bfa"
                badge_bg = "#051428" if is_num else "#0e0521"
                badge_bd = "#1e3a5f" if is_num else "#4c1d95"
                badge_l  = "numeric" if is_num else "text"
                icon     = "∑" if is_num else "◈"
                null_c   = "#ef4444" if null_pct > 0 else "#00e5b4"

                # Card header
                st.markdown(f"""
                <div style="background:#080f1a;border:1px solid #111f33;border-radius:14px;
                    margin-bottom:12px;overflow:hidden">
                  <div style="padding:14px 18px;display:flex;align-items:center;
                      justify-content:space-between;background:#0a1422">
                    <div style="display:flex;align-items:center;gap:10px">
                      <div style="width:28px;height:28px;background:{badge_bg};
                          border:1px solid {badge_bd};border-radius:8px;
                          display:flex;align-items:center;justify-content:center;
                          font-size:.85rem;color:{badge_c}">{icon}</div>
                      <span style="font-weight:700;color:#dde5f0;font-size:.9rem;
                          font-family:monospace">{cname}</span>
                      <span style="background:{badge_bg};color:{badge_c};font-size:.55rem;
                          font-weight:700;padding:2px 7px;border-radius:20px;
                          border:1px solid {badge_bd};letter-spacing:.8px;
                          text-transform:uppercase">{badge_l}</span>
                    </div>
                    <div style="display:flex;align-items:center;gap:14px">
                      <span style="font-size:.7rem;color:#2d4a6b">{n_unique:,} unique</span>
                      <span style="font-size:.7rem;color:{null_c};font-weight:600">
                        {"⚠ " if null_pct > 0 else "✓ "}{null_pct}% null
                      </span>
                    </div>
                  </div>""", unsafe_allow_html=True)

                if is_num:
                    mn   = p.get("min")
                    mx   = p.get("max")
                    mean = p.get("mean")
                    s    = p.get("sum")

                    # Stat row
                    stats_html = "<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:8px;padding:14px 18px'>"
                    for lbl, val, accent in [("MIN", mn, "#60a5fa"), ("MAX", mx, "#f472b6"), ("MEAN", mean, "#f59e0b"), ("SUM", s, "#34d399")]:
                        stats_html += f"""
                        <div style="background:#0d1a2a;border:1px solid #111f33;border-radius:10px;
                            padding:12px;text-align:center">
                          <div style="font-size:.55rem;color:#2d4a6b;letter-spacing:1.5px;
                              margin-bottom:6px;font-weight:700">{lbl}</div>
                          <div style="font-size:.95rem;font-weight:800;color:{accent};
                              font-variant-numeric:tabular-nums">{_fmtn(val)}</div>
                        </div>"""
                    stats_html += "</div>"
                    st.markdown(stats_html, unsafe_allow_html=True)

                    # Range bar with mean marker
                    if mn is not None and mx is not None and mx != mn and mean is not None:
                        mean_pct = min(max((mean - mn) / (mx - mn) * 100, 0), 100)
                        st.markdown(f"""
                        <div style="padding:0 18px 16px 18px">
                          <div style="position:relative;background:#0d1a2a;
                              border-radius:6px;height:8px;margin-bottom:8px;overflow:visible">
                            <div style="position:absolute;left:0;top:0;height:100%;
                                width:{mean_pct:.1f}%;
                                background:linear-gradient(90deg,#1e3a5f,#3b82f6);
                                border-radius:6px"></div>
                            <div style="position:absolute;left:{mean_pct:.1f}%;top:-4px;
                                width:4px;height:16px;background:#f59e0b;
                                border-radius:2px;transform:translateX(-50%);
                                box-shadow:0 0 6px #f59e0b80"></div>
                          </div>
                          <div style="display:flex;justify-content:space-between;
                              font-size:.65rem;color:#2d4a6b;font-family:monospace">
                            <span>{_fmtn(mn)}</span>
                            <span style="color:#f59e0b80">▲ avg {_fmtn(mean)}</span>
                            <span>{_fmtn(mx)}</span>
                          </div>
                        </div>""", unsafe_allow_html=True)

                    # Histogram
                    col_vals = [float(r.get(cname)) for r in raw_data if r.get(cname) is not None]
                    try:
                        col_vals = [float(v) for v in col_vals]
                    except Exception:
                        col_vals = []
                    if len(col_vals) >= 3 and mn is not None and mx is not None and mn != mx:
                        import numpy as np
                        import plotly.graph_objects as go
                        n_bins = min(20, max(5, len(col_vals)))
                        counts, edges = np.histogram(col_vals, bins=n_bins)
                        centers = (edges[:-1] + edges[1:]) / 2
                        max_c = max(counts) if max(counts) > 0 else 1
                        opacities = [0.35 + 0.65 * (c / max_c) for c in counts]
                        fig = go.Figure(go.Bar(
                            x=centers, y=counts,
                            marker=dict(
                                color=[f"rgba(96,165,250,{o:.2f})" for o in opacities],
                                line_width=0,
                            ),
                            hovertemplate="<b>%{x:,.2f}</b><br>%{y} rows<extra></extra>",
                        ))
                        if mean is not None:
                            fig.add_vline(x=mean, line_dash="dot", line_color="rgba(245,158,11,0.5)", line_width=1.5)
                        fig.update_layout(
                            height=90, margin=dict(l=8, r=8, t=4, b=20),
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            bargap=0.06,
                            xaxis=dict(showgrid=False, zeroline=False,
                                       tickfont=dict(size=8, color="#2d4a6b"), tickformat=",.0f"),
                            yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                            showlegend=False,
                        )
                        inner_l, inner_m, inner_r = st.columns([1, 30, 1])
                        with inner_m:
                            st.plotly_chart(fig, use_container_width=True, key=f"hist_{prefix}_{cname}")

                else:
                    top_vals = p.get("top_values", [])
                    if top_vals:
                        max_count = max(t["count"] for t in top_vals)
                        all_equal = all(t["count"] == top_vals[0]["count"] for t in top_vals)

                        bars_html = "<div style='padding:14px 18px 16px'>"

                        # Summary line when all values are equal
                        if all_equal and n_unique > 1:
                            bars_html += f"""
                            <div style="display:flex;align-items:center;gap:8px;
                                font-size:.72rem;color:#4a6080;margin-bottom:12px;
                                padding:8px 12px;background:#0d1a2a;border-radius:6px;
                                border-left:3px solid #2d4a6b">
                              <span style="font-size:.8rem">≡</span>
                              All {n_unique} values appear equally — {top_vals[0]['count']}× each ({round(top_vals[0]['count']/max(total_rows,1)*100,1)}%)
                            </div>"""

                        for i, tv in enumerate(top_vals):
                            pct = tv["count"] / max(total_rows, 1) * 100
                            # When all equal: use position-based width so bars are visually distinct
                            if all_equal:
                                bar_w = max(30 - i * 3, 10)  # decreasing: 30%, 27%, 24%...
                            else:
                                bar_w = max(tv["count"] / max_count * 100, 2)
                            # Color intensity based on rank
                            intensity = 1.0 - (i / max(len(top_vals), 1)) * 0.5
                            r = int(91 * intensity)
                            g = int(33 * intensity)
                            b = int(182 * intensity)
                            r2 = int(167 * intensity)
                            g2 = int(139 * intensity)
                            b2 = int(250 * intensity)
                            grad = f"linear-gradient(90deg,rgb({r},{g},{b}),rgb({r2},{g2},{b2}))"

                            # Rank badge
                            rank_bg = "#1a0d33" if i == 0 else "#0d1a2a"
                            rank_c  = "#a78bfa" if i == 0 else "#3d5a7a"

                            bars_html += f"""
                            <div style="margin-bottom:8px;padding:8px 10px;
                                background:{rank_bg};border-radius:8px;
                                border:1px solid {'#4c1d9530' if i==0 else '#0f1f35'}">
                              <div style="display:flex;justify-content:space-between;
                                  align-items:center;margin-bottom:6px">
                                <div style="display:flex;align-items:center;gap:8px">
                                  <span style="font-size:.6rem;color:{rank_c};font-weight:700;
                                      background:#0d1a2a;border:1px solid {rank_c}30;
                                      border-radius:4px;padding:1px 5px;min-width:18px;
                                      text-align:center">#{i+1}</span>
                                  <span style="font-size:.8rem;color:#dde5f0;font-weight:600;
                                      max-width:55%;overflow:hidden;text-overflow:ellipsis;
                                      white-space:nowrap">{tv['value']}</span>
                                </div>
                                <div style="display:flex;align-items:center;gap:6px">
                                  <span style="font-size:.75rem;color:#a78bfa;font-weight:700;
                                      font-variant-numeric:tabular-nums">{tv['count']:,}×</span>
                                  <span style="font-size:.68rem;color:#fff;font-weight:600;
                                      background:{'#4c1d95' if pct > 30 else '#1a2d45'};
                                      border-radius:5px;padding:2px 7px;
                                      min-width:42px;text-align:center">{pct:.1f}%</span>
                                </div>
                              </div>
                              <div style="background:#060e1a;border-radius:3px;height:4px;overflow:hidden">
                                <div style="width:{bar_w:.1f}%;height:100%;
                                    background:{grad};border-radius:3px;
                                    transition:width 0.3s ease"></div>
                              </div>
                            </div>"""

                        if n_unique > len(top_vals):
                            remaining = n_unique - len(top_vals)
                            bars_html += f"""
                            <div style="font-size:.65rem;color:#2d4a6b;margin-top:6px;
                                padding:6px 10px;border-radius:6px;background:#060e1a;
                                text-align:center">
                              + {remaining:,} more unique value{"s" if remaining != 1 else ""}
                            </div>"""
                        bars_html += "</div>"
                        st.markdown(bars_html, unsafe_allow_html=True)
                    else:
                        st.markdown("""
                        <div style="padding:16px 18px;font-size:.75rem;color:#2d4a6b;
                            font-style:italic">No frequency data</div>""",
                        unsafe_allow_html=True)

                st.markdown("</div>", unsafe_allow_html=True)
    with t4:
        if out.get("sql"): st.code(out["sql"], language="sql")
        st.json({k: v for k, v in out.items()
                 if k not in ("data","sql","spec","summary","viz","profile")})


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:

        # ── Logo ─────────────────────────────────────────────
        st.markdown("""
        <div style="padding:1.4rem 1rem 1.2rem 1rem;
            border-bottom:1px solid #0f1f35;margin-bottom:1rem">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px">
            <div style="width:28px;height:28px;background:linear-gradient(135deg,#00e5b4,#3b82f6);
                border-radius:7px;display:flex;align-items:center;justify-content:center;
                font-size:.9rem">⬡</div>
            <div style="font-family:'Outfit',sans-serif;font-size:1.05rem;font-weight:800;
                color:#e2e8f0;letter-spacing:-.3px">DB Assistant</div>
          </div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:.55rem;
              color:#2d4a6b;letter-spacing:2.5px;padding-left:36px">POWERED BY GEMINI</div>
        </div>""", unsafe_allow_html=True)

        # ── User card ─────────────────────────────────────────
        name  = st.session_state.get("user_name", "")
        email = st.session_state.get("user_email", "")
        initials = "".join(w[0].upper() for w in name.split()[:2]) if name else "?"
        st.markdown(f"""
        <div style="background:#0a1929;border:1px solid #1a2d45;border-radius:10px;
            padding:12px 14px;margin-bottom:12px;display:flex;align-items:center;gap:12px">
          <div style="width:36px;height:36px;border-radius:50%;flex-shrink:0;
              background:linear-gradient(135deg,#00e5b4,#3b82f6);
              display:flex;align-items:center;justify-content:center;
              font-size:.8rem;font-weight:800;color:#0a0f1a">{initials}</div>
          <div style="overflow:hidden">
            <div style="font-size:.82rem;font-weight:600;color:#e2e8f0;
                white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{name}</div>
            <div style="font-size:.65rem;color:#4a6080;margin-top:1px;
                white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{email}</div>
          </div>
        </div>""", unsafe_allow_html=True)

        # ── Active DB badge ───────────────────────────────────
        conn_name = st.session_state.get("active_conn_name")
        if conn_name:
            n = len(st.session_state.get("pg_all_tables", []))
            st.markdown(f"""
            <div style="background:linear-gradient(135deg,rgba(0,229,180,.06),rgba(59,130,246,.04));
                border:1px solid rgba(0,229,180,.2);border-radius:10px;
                padding:10px 14px;margin-bottom:14px;position:relative;overflow:hidden">
              <div style="position:absolute;top:0;left:0;width:3px;height:100%;
                  background:linear-gradient(180deg,#00e5b4,#3b82f6)"></div>
              <div style="padding-left:6px">
                <div style="font-size:.58rem;color:#00e5b4;letter-spacing:1.5px;
                    font-weight:700;text-transform:uppercase;margin-bottom:3px">
                  Active Connection</div>
                <div style="font-size:.8rem;font-weight:700;color:#e2e8f0;
                    white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{conn_name}</div>
                <div style="font-size:.65rem;color:#4a6080;margin-top:2px">
                  {n} table{"s" if n != 1 else ""} loaded</div>
              </div>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background:rgba(239,68,68,.05);border:1px solid rgba(239,68,68,.15);
                border-radius:10px;padding:10px 14px;margin-bottom:14px">
              <div style="font-size:.72rem;color:#ef4444">⚠ No active connection</div>
              <div style="font-size:.65rem;color:#4a6080;margin-top:2px">
                Go to My Connections to connect</div>
            </div>""", unsafe_allow_html=True)

        # ── Navigation ────────────────────────────────────────
        st.markdown("""
        <div style="font-size:.58rem;color:#2d4a6b;letter-spacing:2px;font-weight:700;
            text-transform:uppercase;margin-bottom:8px;padding-left:2px">Navigate</div>
        """, unsafe_allow_html=True)

        NAV_ITEMS = [
            ("🔌 My Connections",      "Manage DB connections"),
            ("🐘 PostgreSQL NL Query", "Ask questions in plain English"),
            ("🔎 Direct SQL Query",    "Run raw SQL directly"),
            ("🍃 MongoDB NL Query",    "Natural language Mongo queries"),
            ("🔍 Direct Mongo",        "Run MongoDB queries manually"),
            ("📁 Upload Dataset",      "Upload CSV or Excel files"),
            ("📋 Audit Log",           "View query history"),
            ("🩺 Health",              "System status"),
        ]

        # Use radio but hide it, drive selection via button-style HTML
        page = st.radio("nav", [i[0] for i in NAV_ITEMS],
                        label_visibility="collapsed")

        # ── Sign out ──────────────────────────────────────────
        st.markdown("<div style='margin-top:16px'></div>", unsafe_allow_html=True)
        st.markdown("<hr style='border-color:#0f1f35;margin:0 0 12px 0'>",
                    unsafe_allow_html=True)

        col_a, col_b = st.columns([3, 1])
        with col_a:
            st.markdown(f"""
            <div style="font-size:.7rem;color:#2d4a6b;padding-top:6px">
              Signed in as <span style="color:#4a6080">{email}</span>
            </div>""", unsafe_allow_html=True)
        with col_b:
            if st.button("⏻", help="Sign out", use_container_width=True):
                logout()

    return page


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════
if not is_logged_in():
    page_auth()
    st.stop()

page = render_sidebar()

titles = {
    "🔌 My Connections":      ("🔌 My Database Connections",      "Save multiple DBs · AES-256 encrypted passwords"),
    "🐘 PostgreSQL NL Query": ("🐘 PostgreSQL — Natural Language", "Select tables · ask anything · JOINs auto-generated"),
    "🔎 Direct SQL Query":    ("🔎 Direct SQL",                    "Run raw SELECT on your active database"),
    "🍃 MongoDB NL Query":    ("🍃 MongoDB — Natural Language",    "Gemini converts questions to MongoDB queries"),
    "🔍 Direct Mongo":        ("🔍 Direct MongoDB",                "Raw JSON filter builder"),
    "📁 Upload Dataset":      ("📁 Upload Dataset",                "CSV/Excel → PostgreSQL or MongoDB"),
    "📋 Audit Log":           ("📋 Query Audit Log",              "Every query you've run — who, what, when"),
    "🩺 Health":              ("🩺 System Health",                "Service status monitor"),
}
ttl, sub = titles.get(page, ("DB Assistant",""))
st.markdown(
    f'<div style="padding:.2rem 0 .8rem 0;border-bottom:1px solid #1e2d45;margin-bottom:1rem">' +
    f'<div style="font-family:Outfit,sans-serif;font-size:1.55rem;font-weight:700;color:#e2eaf8;letter-spacing:-.3px">{ttl}</div>' +
    f'<div style="font-family:JetBrains Mono,monospace;font-size:.68rem;color:#4a6080;margin-top:3px">{sub}</div>' +
    '</div>',
    unsafe_allow_html=True
)


if   page == "🔌 My Connections":      page_connections()
elif page == "📋 Audit Log":           page_audit_log()

elif page == "🐘 PostgreSQL NL Query":
    pg_uri, conn_name = load_active_connection()
    if pg_uri:
        # Header row with connection name and refresh
        hc1, hc2 = st.columns([6, 1])
        hc1.success(f"🐘 **{conn_name}**")
        if hc2.button("🔄 Refresh", key="pg_refresh_tables", help="Reload table list from database"):
            st.session_state.pop("pg_uri_for_conn", None)
            st.session_state.pop("pg_all_tables", None)
            st.session_state.pop("pg_table_schemas", None)
            st.session_state.pop("pg_out", None)
            st.rerun()

        # Show available tables as info — no selection needed
        all_tables = st.session_state.get("pg_all_tables", [])
        if all_tables:
            table_names = [t["table"] for t in all_tables]
            st.markdown(
                '<div style="background:rgba(0,229,180,.05);border:1px solid rgba(0,229,180,.15);'
                'border-radius:8px;padding:.6rem 1rem;margin-bottom:.8rem">'
                '<span style="font-family:JetBrains Mono,monospace;font-size:.65rem;color:#00e5b4;'
                'text-transform:uppercase;letter-spacing:1.5px">Available tables · Gemini auto-selects</span><br>'
                '<span style="font-family:JetBrains Mono,monospace;font-size:.72rem;color:#e2eaf8;margin-top:4px;display:block">'
                + "  ·  ".join(f"`{t}`" for t in table_names) +
                '</span></div>',
                unsafe_allow_html=True
            )

        # Query box — always visible, no table selection required
        pg_q = st.text_area(
            "Ask a question", height=90,
            placeholder="e.g.  Total revenue by region  ·  Which gold customers have pending payments?  ·  Top 5 products by sales",
            key="pg_q",
            label_visibility="collapsed"
        )
        lc, rc = st.columns([1, 4])
        pg_lim = lc.number_input("Limit", 1, 1000, 50, key="pg_lim")
        with rc:
            st.write("")
            run = st.button("🚀 Run Query", use_container_width=True, key="btn_pg")

        if run:
            if not pg_q.strip():
                st.error("Please enter a question.")
            else:
                import re as _re
                SPLITTERS = [
                    r"\band also\b", r"\balso show\b", r"\bas well as\b",
                    r"\bfurthermore\b", r"\badditionally\b", r"\bplus\b",
                ]
                questions = [pg_q.strip()]
                for pat in SPLITTERS:
                    parts = _re.split(pat, pg_q, flags=_re.IGNORECASE)
                    if len(parts) > 1:
                        questions = [p.strip().strip("?").strip() for p in parts if p.strip()]
                        break
                if len(questions) == 1 and "?" in pg_q:
                    parts = [p.strip() for p in pg_q.split("?") if p.strip()]
                    if len(parts) > 1:
                        questions = parts

                is_multi = len(questions) > 1

                if is_multi:
                    with st.spinner(f"🤖 Running {len(questions)} separate queries…"):
                        multi_results = []
                        for q in questions[:5]:
                            r = api_post("/pg/nl-query-auto", {
                                "pg_uri":   pg_uri,
                                "question": q,
                                "limit":    int(pg_lim)
                            })
                            multi_results.append({"question": q, "result": r})
                    st.session_state["pg_multi"] = multi_results
                    st.session_state.pop("pg_out", None)
                    st.success(f"✅ Ran {len(multi_results)} queries")
                else:
                    with st.spinner("🤖 Gemini reading all tables and generating SQL…"):
                        out = api_post("/pg/nl-query-auto", {
                            "pg_uri":   pg_uri,
                            "question": pg_q,
                            "limit":    int(pg_lim)
                        })
                    if "error" in out or "detail" in out:
                        st.error(out.get("detail") or out.get("error"))
                    else:
                        st.session_state["pg_out"] = out
                        st.session_state.pop("pg_multi", None)
                        used = out.get("tables_used", [])
                        if len(used) > 1:
                            st.success(f"✅ {out.get('count',0)} rows · JOIN across: {', '.join(t.split('.')[-1] for t in used)}")
                        else:
                            st.success(f"✅ {out.get('count',0)} rows · Table: {', '.join(t.split('.')[-1] for t in used) or 'auto'}")

        # ── Multi-question results ────────────────────────────
        if "pg_multi" in st.session_state:
            for i, item in enumerate(st.session_state["pg_multi"]):
                q   = item["question"]
                res = item["result"]
                st.markdown(
                    f'<div style="background:#111827;border:1px solid #1e2d45;border-radius:10px;' +
                    f'padding:.6rem 1rem;margin:1rem 0 .4rem 0">' +
                    f'<span style="font-family:JetBrains Mono,monospace;font-size:.62rem;' +
                    f'color:#00e5b4;text-transform:uppercase;letter-spacing:1px">Query {i+1}</span>' +
                    f'<div style="font-family:JetBrains Mono,monospace;font-size:.78rem;' +
                    f'color:#e2eaf8;margin-top:3px">{q}</div></div>',
                    unsafe_allow_html=True
                )
                if "error" in res or "detail" in res:
                    st.error(res.get("detail") or res.get("error"))
                else:
                    used = res.get("tables_used", [])
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Rows",   res.get("count", "-"))
                    m2.metric("ms",     res.get("execution_time_ms", "-"))
                    m3.metric("Tables", len(used) if used else "-")
                    if used:
                        badges = " ".join(
                            '<span style="background:rgba(0,229,180,.1);color:#00e5b4;' +
                            'border:1px solid rgba(0,229,180,.25);border-radius:20px;' +
                            'padding:2px 10px;font-family:JetBrains Mono,monospace;' +
                            'font-size:.65rem;font-weight:600">' + t.split(".")[-1] + '</span>'
                            for t in used
                        )
                        st.markdown(badges, unsafe_allow_html=True)
                    if res.get("sql"):
                        with st.expander("📝 SQL", expanded=False):
                            st.code(res["sql"], language="sql")
                    show_results(res, f"pg_m{i}")
                    st.divider()

        # ── Single result ─────────────────────────────────────
        if "pg_out" in st.session_state and "pg_multi" not in st.session_state:
            out = st.session_state["pg_out"]
            used = out.get("tables_used", [])
            m1, m2, m3 = st.columns(3)
            m1.metric("Rows",   out.get("count", "-"))
            m2.metric("ms",     out.get("execution_time_ms", "-"))
            m3.metric("Tables", len(used) if used else "-")
            if used:
                badges = " ".join(
                    '<span style="background:rgba(0,229,180,.1);color:#00e5b4;'
                    'border:1px solid rgba(0,229,180,.25);border-radius:20px;'
                    'padding:2px 10px;font-family:JetBrains Mono,monospace;'
                    'font-size:.65rem;font-weight:600">' + t.split(".")[-1] + '</span>'
                    for t in used
                )
                st.markdown(badges, unsafe_allow_html=True)
            if out.get("sql"):
                with st.expander("📝 SQL generated by Gemini", expanded=False):
                    st.code(out["sql"], language="sql")
            st.divider()
            show_results(out, "pg")

elif page == "🔎 Direct SQL Query":
    pg_uri, conn_name = load_active_connection()
    if pg_uri:
        hc1, hc2 = st.columns([6, 1])
        hc1.success(f"🐘 **{conn_name}**")
        if hc2.button("🔄 Refresh", key="sql_refresh_tables", help="Reload table list from database"):
            st.session_state.pop("pg_uri_for_conn", None)
            st.session_state.pop("pg_all_tables", None)
            st.session_state.pop("pg_table_schemas", None)
            st.rerun()
        all_t = st.session_state.get("pg_all_tables",[])
        if all_t:
            with st.expander("📋 Available Tables"):
                for t in all_t:
                    st.markdown(f"&nbsp;&nbsp;`{t['fqn']}` <span style='font-size:.65rem;color:#566a8a'>~{t['approx_rows']:,}</span>", unsafe_allow_html=True)

        sq_sql = st.text_area("SQL Query", height=160, value="SELECT * FROM public.ecommerce_orders LIMIT 10;", key="sq_sql")
        if st.button("▶ Execute", use_container_width=True):
            if not sq_sql.strip().lower().startswith("select"): st.error("Only SELECT.")
            else:
                with st.spinner("Running…"):
                    out = api_post("/pg/direct-query",{"pg_uri":pg_uri,"sql":sq_sql})
                if "error" in out or "detail" in out: st.error(out.get("detail") or out.get("error"))
                else: st.session_state["sq_out"]=out; st.success(f"✓ {out.get('count',0)} rows")

        if "sq_out" in st.session_state:
            st.columns(2)[0].metric("Rows",st.session_state["sq_out"].get("count","-"))
            st.divider(); show_results(st.session_state["sq_out"],"sq")

elif page == "🍃 MongoDB NL Query":
    all_conns   = st.session_state.get("saved_connections") or api_get("/auth/connections")
    mongo_conns = [c for c in (all_conns or []) if c.get("db_type") == "mongodb"]

    if not mongo_conns:
        st.warning("No MongoDB connections saved yet.")
        st.info("Go to **🔌 My Connections → MongoDB tab** and save a connection first.")
        st.stop()

    conn_names = [c["name"] for c in mongo_conns]
    sel = st.selectbox("🍃 Select MongoDB Connection", conn_names, key="nl_mg_conn")
    chosen = next(c for c in mongo_conns if c["name"] == sel)
    mg_uri = chosen["host"]
    mg_db  = chosen["dbname"]

    st.markdown(
        f'<div style="font-family:IBM Plex Mono,monospace;font-size:.7rem;color:#566a8a;'
        f'margin-bottom:.8rem">📍 {mg_uri[:60]}{"…" if len(mg_uri)>60 else ""} · 🗄 {mg_db}</div>',
        unsafe_allow_html=True
    )

    # ── Load collections ──────────────────────────────────────
    cache_key = f"mg_colls_{chosen['id']}"
    if cache_key not in st.session_state:
        with st.spinner(f"Loading collections from {mg_db}…"):
            resp = api_get(f"/mongo/collections?mongo_uri={mg_uri}&db_name={mg_db}")
            st.session_state[cache_key] = resp.get("collections", []) if isinstance(resp, dict) else []

    colls = st.session_state.get(cache_key, [])

    rl, rr = st.columns([5, 1])
    rl.markdown("#### Collections")
    if rr.button("🔄", key="btn_reload_mg_colls", help="Reload collections"):
        st.session_state.pop(cache_key, None)
        st.rerun()

    if not colls:
        st.warning("No collections found — check your connection.")
        st.stop()

    # ── Collection checkboxes — tick multiple for JOIN ────────
    st.caption("☑ Tick one collection for a simple query · Tick multiple for a $lookup JOIN")
    selected_colls = []
    for coll in colls:
        prev = coll in st.session_state.get("mg_selected_colls", [])
        row_c1, row_c2 = st.columns([0.5, 9])
        checked = row_c1.checkbox("", value=prev, key=f"mg_chk_{coll}",
                                   label_visibility="collapsed")
        if checked:
            selected_colls.append(coll)
        border = "2px solid #00d4a8" if checked else "1px solid #1e2d4a"
        row_c2.markdown(
            f'<div style="background:#0e1420;border:{border};border-radius:8px;'
            f'padding:.45rem .9rem;margin-bottom:2px;font-family:IBM Plex Mono,monospace;'
            f'font-size:.78rem;color:#c8d8f0">🍃 {coll}</div>',
            unsafe_allow_html=True
        )
    st.session_state["mg_selected_colls"] = selected_colls

    if not selected_colls:
        st.info("Select at least one collection above.")
        st.stop()

    # ── Schema preview — side by side ────────────────────────
    st.divider()
    st.markdown(
        '<div style="font-family:Syne,sans-serif;font-size:.85rem;font-weight:700;'
        'color:#c8d8f0;margin-bottom:.6rem">📋 Available Fields</div>',
        unsafe_allow_html=True
    )
    ncols = min(len(selected_colls), 3)
    scols = st.columns(ncols)
    for i, coll in enumerate(selected_colls):
        skey = f"mg_schema_{chosen['id']}_{coll}"
        if skey not in st.session_state:
            prev_resp = api_get(f"/mongo/preview?mongo_uri={mg_uri}&db_name={mg_db}&collection={coll}&limit=10")
            docs = prev_resp.get("documents", []) if isinstance(prev_resp, dict) else []
            # Infer fields from sample documents
            fields = []
            if docs:
                sample = {k: type(v).__name__ for k, v in docs[0].items() if k != "_id"}
                fields = list(sample.items())
            st.session_state[skey] = fields

        fields = st.session_state.get(skey, [])
        rows_html = ""
        for fname, ftype in fields[:20]:
            is_id = fname.endswith("_id") or fname == "id"
            icon  = "🔑" if is_id else "·"
            type_color = "#0099ff" if ftype in ("int","float") else (
                         "#ffa502" if "time" in ftype or "date" in ftype else "#566a8a")
            rows_html += (
                '<div style="display:flex;align-items:center;gap:6px;padding:3px 0;border-bottom:1px solid #0d1929">' +
                '<span style="font-size:.65rem;width:14px;text-align:center">' + icon + '</span>' +
                '<span style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#c8d8f0;flex:1">' + fname + '</span>' +
                '<span style="font-family:IBM Plex Mono,monospace;font-size:.6rem;color:' + type_color +
                ';background:rgba(0,0,0,.3);padding:1px 5px;border-radius:4px">' + ftype + '</span></div>'
            )
        with scols[i % ncols]:
            st.markdown(
                '<div style="background:#0a1020;border:1px solid #1e2d4a;border-radius:12px;padding:.8rem 1rem;margin-bottom:.5rem">' +
                '<div style="font-family:IBM Plex Mono,monospace;font-size:.75rem;font-weight:700;color:#00d4a8;margin-bottom:.5rem;padding-bottom:.4rem;border-bottom:1px solid #1e2d4a">🍃 ' + coll + '</div>' +
                rows_html + '</div>',
                unsafe_allow_html=True
            )

    # JOIN hint if multiple collections selected
    if len(selected_colls) >= 2:
        st.markdown(
            '<div style="background:rgba(59,130,246,.07);border:1px solid rgba(59,130,246,.2);' +
            'border-radius:8px;padding:.6rem 1rem;margin:.5rem 0;font-family:JetBrains Mono,monospace;' +
            'font-size:.72rem;color:#3b82f6">' +
            f'🔗 JOIN mode — Gemini auto-selects the primary collection and writes $lookup across {len(selected_colls)} collections' +
            '</div>',
            unsafe_allow_html=True
        )

    # ── Query box ─────────────────────────────────────────────
    st.markdown(
        '<div style="background:rgba(0,153,255,.06);border:1px solid rgba(0,153,255,.2);'
        'border-radius:12px;padding:.8rem 1.2rem;margin:1rem 0 .8rem 0">' +
        '<div style="font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;color:#0099ff;margin-bottom:.25rem">🤖 Ask Anything</div>' +
        '<div style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#566a8a">' +
        ("Single collection → simple aggregation · Multiple → $lookup JOIN automatically" if len(selected_colls) > 1
         else "Ask a natural language question about your collection") +
        '</div></div>',
        unsafe_allow_html=True
    )

    nl_q = st.text_area("Your question", height=90,
        placeholder="e.g. Total revenue by region | Show orders with customer names and cities | Which customers have pending payments?",
        key="nl_q", label_visibility="collapsed")
    lc, mc, rc = st.columns([1, 1, 3])
    nl_lim  = lc.number_input("Limit", 1, 500, 50, key="nl_lim")
    nl_days = mc.number_input("Days",  1, 3650, 90, key="nl_days")
    with rc:
        run = st.button("🚀 Run Query", use_container_width=True, key="btn_nl")

    if run:
        if not nl_q.strip():
            st.error("Please enter a question.")
        elif len(selected_colls) == 1:
            with st.spinner("🤖 Gemini generating MongoDB query…"):
                out = api_post("/mongo/nl-query", {
                    "mongo_uri": mg_uri, "db_name": mg_db,
                    "collection": selected_colls[0],
                    "question": nl_q, "limit": int(nl_lim),
                    "default_days": int(nl_days)
                })
            if "error" in out or "detail" in out:
                st.error(out.get("detail") or out.get("error"))
            else:
                st.session_state["nl_out"] = out
                st.success(f"✅ {out.get('count', 0)} rows · collection: {selected_colls[0]}")
        else:
            with st.spinner(f"🤖 Gemini generating $lookup JOIN pipeline across {len(selected_colls)} collections…"):
                out = api_post("/mongo/nl-query-join", {
                    "mongo_uri": mg_uri, "db_name": mg_db,
                    "collections": selected_colls,
                    "question": nl_q, "limit": int(nl_lim),
                    "default_days": int(nl_days)
                })
            if "error" in out or "detail" in out:
                st.error(out.get("detail") or out.get("error"))
            else:
                st.session_state["nl_out"] = out
                primary = out.get("primary_collection", selected_colls[0])
                others  = [c for c in selected_colls if c != primary]
                st.success(
                    "✅ " + str(out.get("count", 0)) + " rows · "
                    "Primary: " + primary + " → JOIN: " + ", ".join(others)
                )

    if "nl_out" in st.session_state:
        out = st.session_state["nl_out"]
        is_join = out.get("source") == "mongo_join"
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Rows",  out.get("count", "-"))
        m2.metric("ms",    out.get("execution_time_ms", "-"))
        if is_join:
            m3.metric("Mode",  "JOIN ($lookup)")
            m4.metric("Collections", len(out.get("collections", [])))
        else:
            m3.metric("Type",  (out.get("spec", {}).get("query_type") or "-").upper())
            m4.metric("Date",  out.get("date_field_used") or "None")
        if is_join and out.get("pipeline"):
            import json as _json
            with st.expander("📝 Pipeline generated by Gemini", expanded=False):
                st.code(_json.dumps(out["pipeline"], indent=2), language="json")
            if out.get("debug_sample") and out.get("count", 0) == 0:
                with st.expander("🔬 Sample document values (for debugging)", expanded=True):
                    for cname, doc in out["debug_sample"].items():
                        st.markdown(f"**{cname}** — first document:")
                        st.json(doc)
        st.divider()
        show_results(out, "nl")

elif page == "🔍 Direct Mongo":
    all_conns   = st.session_state.get("saved_connections") or api_get("/auth/connections")
    mongo_conns = [c for c in (all_conns or []) if c.get("db_type") == "mongodb"]

    if not mongo_conns:
        st.warning("No MongoDB connections saved yet.")
        st.info("Go to **🔌 My Connections → MongoDB tab** and add one first.")
        st.stop()

    conn_names = [c["name"] for c in mongo_conns]
    sel2   = st.selectbox("🍃 Select MongoDB Connection", conn_names, key="dm_mg_conn")
    chosen2 = next(c for c in mongo_conns if c["name"] == sel2)
    d_uri  = chosen2["host"]
    d_db   = chosen2["dbname"]

    # Auto-load collections
    cache_key2 = f"mg_colls_{chosen2['id']}"
    if cache_key2 not in st.session_state:
        with st.spinner("Loading collections…"):
            resp2 = api_get(f"/mongo/collections?mongo_uri={d_uri}&db_name={d_db}")
            colls2 = resp2.get("collections", []) if isinstance(resp2, dict) else []
            st.session_state[cache_key2] = colls2
    colls2 = st.session_state.get(cache_key2, [])

    if colls2:
        d_coll = st.selectbox("Collection", colls2, key="dm_coll_sel")
    else:
        d_coll = st.text_input("Collection", value="sales_orders", key="dm_coll_txt")

    q1, q2 = st.columns(2)
    d_f = q1.text_area("Filter JSON",  value="{}",  height=100, key="d_f")
    d_p = q1.text_area("Projection",   value="",    height=80,  key="d_p")
    d_s = q2.text_area("Sort",         value="",    height=80,  key="d_s")
    d_l = q2.number_input("Limit", 1, 500, 10, key="d_l")

    if st.button("🔍 Execute", use_container_width=True, key="btn_dm"):
        try:
            flt = json.loads(d_f or "{}")
        except:
            st.error("Invalid JSON in Filter field.")
            st.stop()
        pl = {"mongo_uri": d_uri, "db_name": d_db, "collection": d_coll,
              "filter": flt, "limit": int(d_l)}
        if d_p.strip():
            try:
                pl["projection"] = json.loads(d_p)
            except:
                st.error("Invalid JSON in Projection field.")
                st.stop()
        if d_s.strip():
            try:
                s_clean = d_s.strip()
                if not s_clean.startswith("{"): s_clean = "{" + s_clean + "}"
                pl["sort"] = json.loads(s_clean)
            except:
                st.error("Invalid JSON in Sort field. Use format: {\"field\": -1}")
                st.stop()
        out = api_post("/mongo/query", pl)
        if "error" in out or "detail" in out:
            st.error(out.get("detail") or out.get("error"))
        else:
            st.session_state["dm_out"] = out
            st.success(f"✅ {out.get('count', 0)} documents")
    if "dm_out" in st.session_state:
        st.columns(2)[0].metric("Docs", st.session_state["dm_out"].get("count", "-"))
        st.divider()
        show_results(st.session_state["dm_out"], "dm")

elif page == "📁 Upload Dataset":
    tab_int, tab_pg, tab_mg, tab_hist = st.tabs([
        "📂 My Datasets (No setup needed)",
        "🐘 External PostgreSQL",
        "🍃 External MongoDB",
        "📋 Upload History"
    ])

    # ═══════════════════════════════════════════════════════════
    # TAB 1 — INTERNAL DATASETS (stored in da_db, per-user)
    # ═══════════════════════════════════════════════════════════
    with tab_int:
        st.markdown("""
        <div style="background:rgba(0,212,168,.06);border:1px solid rgba(0,212,168,.25);
             border-radius:12px;padding:.9rem 1.2rem;margin-bottom:1.2rem">
          <div style="font-family:Syne,sans-serif;font-weight:700;font-size:.9rem;
               color:#00d4a8;margin-bottom:.3rem">📂 Your Private Datasets</div>
          <div style="font-family:IBM Plex Mono,monospace;font-size:.7rem;color:#566a8a;line-height:1.6">
            Upload any CSV or Excel file — it's stored privately in our database under your account.<br>
            No connections needed. Query it with natural language immediately after upload.
          </div>
        </div>""", unsafe_allow_html=True)

        # ── Upload new file ───────────────────────────────────
        st.markdown("#### Upload a new file")
        up_col1, up_col2 = st.columns([2, 1])
        int_file = up_col1.file_uploader("Choose CSV or Excel",
                                         type=["csv","xlsx","xls"],
                                         key="int_file_up")
        int_tbl  = up_col2.text_input("Save as table name",
                                       placeholder="e.g. sales_q1",
                                       key="int_tbl_name")

        if int_file and not int_tbl:
            # Auto-suggest table name from filename
            auto = int_file.name.rsplit(".", 1)[0].lower()
            auto = re.sub(r"[^a-z0-9]+", "_", auto).strip("_")[:40]
            st.caption(f"💡 Suggested name: `{auto}` — type it above or leave blank to use it")
            int_tbl = auto

        if int_file:
            try:
                prev_df = pd.read_csv(io.BytesIO(int_file.getvalue()))                     if int_file.name.endswith(".csv")                     else pd.read_excel(io.BytesIO(int_file.getvalue()))
                st.markdown(f"**Preview** — {len(prev_df):,} rows × {len(prev_df.columns)} columns")
                st.dataframe(prev_df.head(5), use_container_width=True, hide_index=True)
            except Exception:
                pass

        if st.button("⬆️ Upload & Save to My Datasets", use_container_width=True,
                     key="btn_int_upload"):
            if not int_file:
                st.error("Please select a file.")
            else:
                tbl_name = (int_tbl or int_file.name.rsplit(".", 1)[0])
                tbl_name = re.sub(r"[^a-z0-9]+", "_", tbl_name.lower()).strip("_")[:40] or "dataset"
                with st.spinner(f"Uploading **{int_file.name}** as `{tbl_name}`…"):
                    out = api_post_form(
                        "/my-datasets/upload",
                        data={"table_name": tbl_name},
                        files={"file": (int_file.name, int_file.getvalue(), int_file.type)}
                    )
                if "error" in out or "detail" in out:
                    st.error(f"❌ {out.get('detail') or out.get('error')}")
                else:
                    st.success(f"✅ **{int_file.name}** saved as `{tbl_name}` — {out.get('row_count',0):,} rows")
                    st.session_state["int_active_table"] = tbl_name
                    st.session_state.pop("int_datasets", None)
                    # Track in upload history
                    api_post("/auth/uploads/track", {
                        "file_name": int_file.name,
                        "file_size_bytes": len(int_file.getvalue()),
                        "row_count": out.get("row_count", 0),
                        "db_type": "postgresql",
                        "destination": f"{out.get('schema','uploads')}.{tbl_name}",
                        "connection_name": "Internal (My Datasets)",
                        "status": "success",
                    })
                    st.session_state.pop("upload_history", None)
                    st.rerun()

        st.divider()

        # ── My saved datasets list ────────────────────────────
        hdr_c1, hdr_c2 = st.columns([2, 1])
        hdr_c1.markdown("#### My Datasets")
        if hdr_c2.button("🔄 Refresh", key="btn_int_refresh"):
            st.session_state.pop("int_datasets", None)

        if "int_datasets" not in st.session_state:
            resp = api_get("/my-datasets/list")
            st.session_state["int_datasets"] = resp.get("datasets", []) if isinstance(resp, dict) else []

        datasets = st.session_state.get("int_datasets", [])

        if not datasets:
            st.info("No datasets yet — upload a CSV or Excel file above.")
        else:
            # ── Dataset cards (no checkboxes — Gemini auto-picks) ──
            for ds in datasets:
                tname     = ds.get("table_name", "")
                row_count = ds.get("row_count") or 0
                col_count = ds.get("col_count") or 0
                size      = ds.get("size") or "—"

                card_c1, card_c2 = st.columns([9, 1])
                card_c1.markdown(f"""
                <div style="background:#0e1420;border:1px solid #1e2d4a;border-radius:10px;
                     padding:.6rem 1rem;margin-bottom:2px">
                  <span style="font-family:IBM Plex Mono,monospace;font-size:.82rem;
                               font-weight:600;color:#c8d8f0">📊 {tname}</span>
                  <span style="font-family:IBM Plex Mono,monospace;font-size:.67rem;
                               color:#566a8a;margin-left:.8rem">
                    {row_count:,} rows · {col_count} cols · {size}
                  </span>
                </div>""", unsafe_allow_html=True)

                if card_c2.button("🗑", key=f"int_del_{tname}", help=f"Delete {tname}"):
                    api_delete(f"/my-datasets/{tname}")
                    st.session_state.pop("int_datasets", None)
                    # clear cached schema
                    st.session_state.pop(f"int_schema_{tname}", None)
                    st.rerun()

            # ── Pre-load ALL table schemas for the schema panel ──
            all_table_names = [ds.get("table_name", "") for ds in datasets]
            all_schemas = {}
            for tname in all_table_names:
                skey = f"int_schema_{tname}"
                if skey not in st.session_state:
                    sr = api_get(f"/my-datasets/schema?table_name={tname}")
                    st.session_state[skey] = sr.get("columns", []) if isinstance(sr, dict) else []
                all_schemas[tname] = st.session_state[skey]

            # ── Schema panel — clean cards with proper column lists ──
            st.divider()
            st.markdown(
                '<div style="font-family:Syne,sans-serif;font-size:.85rem;font-weight:700;'
                'color:#c8d8f0;margin-bottom:.6rem">📋 Available Columns</div>',
                unsafe_allow_html=True
            )
            ncols = min(len(all_table_names), 3)
            scols = st.columns(ncols)
            for i, tname in enumerate(all_table_names):
                with scols[i % ncols]:
                    cols_info = all_schemas.get(tname, [])
                    # Build column rows: 🔑 only for _id suffix (true PKs), not every col with "id"
                    rows_html = ""
                    for c in cols_info:
                        is_pk = c["name"].endswith("_id") or c["name"] == "id"
                        icon  = "🔑" if is_pk else "·"
                        type_color = "#0099ff" if "int" in c["pg_type"] or "big" in c["pg_type"] else (
                                     "#ffa502" if "time" in c["pg_type"] or "date" in c["pg_type"] else
                                     "#566a8a")
                        rows_html += (
                            '<div style="display:flex;align-items:center;gap:6px;'
                            'padding:3px 0;border-bottom:1px solid #0d1929">'
                            '<span style="font-size:.65rem;width:14px;text-align:center">' + icon + '</span>'
                            '<span style="font-family:IBM Plex Mono,monospace;font-size:.68rem;'
                            'color:#c8d8f0;flex:1">' + c["name"] + '</span>'
                            '<span style="font-family:IBM Plex Mono,monospace;font-size:.6rem;'
                            'color:' + type_color + ';background:rgba(0,0,0,.3);'
                            'padding:1px 5px;border-radius:4px">' + c["pg_type"].replace(" precision","").replace("character varying","varchar") + '</span>'
                            '</div>'
                        )
                    st.markdown(
                        '<div style="background:#0a1020;border:1px solid #1e2d4a;'
                        'border-radius:12px;padding:.8rem 1rem;margin-bottom:.5rem">'
                        '<div style="font-family:IBM Plex Mono,monospace;font-size:.75rem;'
                        'font-weight:700;color:#00d4a8;margin-bottom:.5rem;padding-bottom:.4rem;'
                        'border-bottom:1px solid #1e2d4a">📊 ' + tname + '</div>'
                        + rows_html +
                        '</div>',
                        unsafe_allow_html=True
                    )

            # ── NL Query box ──────────────────────────────────
            st.markdown(
                '<div style="background:rgba(0,153,255,.06);border:1px solid rgba(0,153,255,.2);'
                'border-radius:12px;padding:.8rem 1.2rem;margin:1rem 0 .8rem 0">'
                '<div style="font-family:Syne,sans-serif;font-weight:700;font-size:.85rem;'
                'color:#0099ff;margin-bottom:.25rem">🤖 Ask Anything</div>'
                '<div style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#566a8a">'
                'Gemini reads all your table schemas and auto-selects the right ones · '
                'Single table → simple query · Multiple tables → JOIN automatically'
                '</div></div>',
                unsafe_allow_html=True
            )

            int_q = st.text_area(
                "Your question",
                height=90,
                placeholder="e.g. Total orders per customer | Show completed payments with customer name and city | Which customers have both pending and completed payments?",
                key="int_q_auto",
                label_visibility="collapsed"
            )
            lc, rc = st.columns([1, 4])
            int_lim = lc.number_input("Limit", 1, 500, 50, key="int_lim_auto")
            with rc:
                run_int = st.button("🚀 Run Query", use_container_width=True,
                                    key="int_run_auto")

            if run_int:
                if not int_q.strip():
                    st.error("Please enter a question.")
                else:
                    with st.spinner("🤖 Gemini is selecting tables and generating SQL…"):
                        qout = api_post("/my-datasets/nl-query-auto", {
                            "all_table_names": all_table_names,
                            "question":        int_q,
                            "limit":           int(int_lim),
                        })
                    if "error" in qout or "detail" in qout:
                        err_msg = str(qout.get("detail") or qout.get("error", "Unknown error"))
                        st.error("Query failed: " + err_msg[:500])
                        if "SQL was:" in err_msg:
                            st.code(err_msg.split("SQL was:")[-1].strip()[:500], language="sql")
                        st.session_state.pop("int_result_auto", None)
                    else:
                        st.session_state["int_result_auto"] = qout
                        used = qout.get("tables_used", [])
                        if len(used) > 1:
                            st.success("✅ " + str(qout.get("count", 0)) + " rows · JOIN across: " + ", ".join(used))
                        else:
                            st.success("✅ " + str(qout.get("count", 0)) + " rows · Table: " + (", ".join(used) if used else "auto"))

            if "int_result_auto" in st.session_state:
                qout = st.session_state["int_result_auto"]
                used_tables = qout.get("tables_used", [])
                # Metrics row
                m1, m2, m3 = st.columns(3)
                m1.metric("Rows",   qout.get("count", "-"))
                m2.metric("Time",   str(qout.get("execution_time_ms", "-")) + " ms")
                m3.metric("Tables used", str(len(used_tables)))
                # Show which tables were used as badges
                if used_tables:
                    badges = " ".join(
                        '<span style="background:rgba(0,212,168,.12);color:#00d4a8;'
                        'border:1px solid rgba(0,212,168,.3);border-radius:20px;'
                        'padding:2px 10px;font-family:IBM Plex Mono,monospace;'
                        'font-size:.65rem;font-weight:600">' + t + '</span>'
                        for t in used_tables
                    )
                    st.markdown(badges, unsafe_allow_html=True)
                if qout.get("sql"):
                    with st.expander("📝 SQL generated by Gemini", expanded=False):
                        st.code(qout["sql"], language="sql")
                st.divider()
                show_results(qout, "int_auto")

    # ═══════════════════════════════════════════════════════════
    # TAB 2 — EXTERNAL POSTGRESQL
    # ═══════════════════════════════════════════════════════════
    with tab_pg:
        all_conns  = st.session_state.get("saved_connections") or api_get("/auth/connections")
        pg_conns   = [c for c in (all_conns or []) if c.get("db_type","postgresql") == "postgresql"]

        if not pg_conns:
            st.warning("No PostgreSQL connections saved yet.")
            st.info("Go to **🔌 My Connections** and add a PostgreSQL connection first.")
            st.stop()

        pg_conn_names = [c["name"] for c in pg_conns]
        sel_up = st.selectbox("🐘 Upload to which connection?", pg_conn_names,
                              key="up_pg_conn_sel")
        chosen_pg = next(c for c in pg_conns if c["name"] == sel_up)

        up_pg_uri_resp = api_post("/auth/connections/get-uri",
                                  {"connection_id": chosen_pg["id"]})
        up_pg_uri  = up_pg_uri_resp.get("uri", "")
        conn_name  = chosen_pg["name"]

        if not up_pg_uri:
            st.error("Could not retrieve connection URI.")
            st.stop()

        st.markdown(f"""
        <div style="background:rgba(0,212,168,.06);border:1px solid rgba(0,212,168,.3);
             border-radius:10px;padding:.7rem 1.1rem;margin:.5rem 0 1rem 0">
          <span style="font-family:IBM Plex Mono,monospace;font-size:.75rem;color:#00d4a8">
            ✅ Uploading to: <strong>{conn_name}</strong>
          </span><br>
          <span style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#566a8a">
            📍 {chosen_pg["host"]}:{chosen_pg["port"]} / {chosen_pg["dbname"]}
          </span>
        </div>""", unsafe_allow_html=True)

        uc1, uc2 = st.columns(2)
        up_s = uc1.text_input("Schema",     value="public",    key="up_s")
        up_t = uc2.text_input("Table name", value="my_upload", key="up_t")
        up_f = st.file_uploader("Choose CSV or Excel file",
                                type=["csv","xlsx","xls"], key="up_pg")

        if up_f:
            try:
                preview_df = pd.read_csv(io.BytesIO(up_f.getvalue()))                     if up_f.name.endswith(".csv")                     else pd.read_excel(io.BytesIO(up_f.getvalue()))
                st.markdown(f"**Preview** — {len(preview_df):,} rows × {len(preview_df.columns)} columns")
                st.dataframe(preview_df.head(5), use_container_width=True, hide_index=True)
            except Exception:
                pass

        if st.button("⬆️ Upload to PostgreSQL", use_container_width=True, key="btn_up_pg"):
            if not up_f:
                st.error("Please select a file first.")
            else:
                with st.spinner(f"Uploading **{up_f.name}** → {conn_name} / {up_s}.{up_t}…"):
                    out = api_post_form(
                        "/pg/upload",
                        data={"pg_uri": up_pg_uri, "schema_name": up_s, "table_name": up_t},
                        files={"file": (up_f.name, up_f.getvalue(), up_f.type)}
                    )
                if "error" in out or "detail" in out:
                    err = out.get("detail") or out.get("error")
                    st.error(f"❌ Upload failed: {err}")
                    api_post("/auth/uploads/track", {
                        "file_name": up_f.name, "file_size_bytes": len(up_f.getvalue()),
                        "db_type": "postgresql", "destination": f"{up_s}.{up_t}",
                        "connection_name": conn_name, "status": "error", "error_detail": str(err),
                    })
                else:
                    row_count = out.get("row_count", 0)
                    st.success(f"✅ **{up_f.name}** → `{conn_name} / {up_s}.{up_t}` — {row_count:,} rows")
                    st.info(f"Go to **🐘 PostgreSQL NL Query** to query `{up_t}`")
                    api_post("/auth/uploads/track", {
                        "file_name": up_f.name, "file_size_bytes": len(up_f.getvalue()),
                        "row_count": row_count, "db_type": "postgresql",
                        "destination": f"{up_s}.{up_t}", "connection_name": conn_name, "status": "success",
                    })
                    st.session_state.pop("upload_history", None)
                    st.session_state.pop("pg_all_tables", None)

    # ═══════════════════════════════════════════════════════════
    # TAB 3 — EXTERNAL MONGODB
    # ═══════════════════════════════════════════════════════════
    with tab_mg:
        all_conns2  = st.session_state.get("saved_connections") or api_get("/auth/connections")
        mongo_conns = [c for c in (all_conns2 or []) if c.get("db_type") == "mongodb"]

        mg_uri_default = "mongodb://localhost:27017"
        mg_db_default  = "sales_db"
        mg_conn_name   = "Manual"

        if mongo_conns:
            sel_mg = st.selectbox("Use saved MongoDB connection",
                                  ["— enter manually —"] + [c["name"] for c in mongo_conns],
                                  key="up_mg_sel")
            if sel_mg != "— enter manually —":
                ch = next(c for c in mongo_conns if c["name"] == sel_mg)
                mg_uri_default = ch["host"]
                mg_db_default  = ch["dbname"]
                mg_conn_name   = ch["name"]
            st.write("")

        mg_uri   = st.text_input("MongoDB URI",  value=mg_uri_default, key="mg_uri")
        mc1, mc2 = st.columns(2)
        mg_db    = mc1.text_input("Database",    value=mg_db_default,  key="mg_db")
        mg_coll  = mc2.text_input("Collection",  value="my_upload",    key="mg_coll")
        mg_f     = st.file_uploader("Choose CSV or Excel file",
                                    type=["csv","xlsx","xls"], key="mg_f")

        if mg_f:
            try:
                prev = pd.read_csv(io.BytesIO(mg_f.getvalue()))                     if mg_f.name.endswith(".csv")                     else pd.read_excel(io.BytesIO(mg_f.getvalue()))
                st.markdown(f"**Preview** — {len(prev):,} rows × {len(prev.columns)} columns")
                st.dataframe(prev.head(5), use_container_width=True, hide_index=True)
            except Exception:
                pass

        if st.button("🍃 Import to MongoDB", use_container_width=True, key="btn_up_mg"):
            if not mg_f:
                st.error("Please select a file first.")
            else:
                raw = mg_f.getvalue()
                df_up = pd.read_csv(io.BytesIO(raw))                     if mg_f.name.endswith(".csv")                     else pd.read_excel(io.BytesIO(raw))
                records = df_up.where(pd.notnull(df_up), None).to_dict(orient="records")
                with st.spinner(f"Importing **{mg_f.name}** → {mg_db}.{mg_coll}…"):
                    out = api_post("/mongo/ingest", {
                        "mongo_uri": mg_uri, "db_name": mg_db,
                        "collection": mg_coll, "records": records
                    })
                if "error" in out or "detail" in out:
                    err = out.get("detail") or out.get("error")
                    st.error(f"❌ {err}")
                    api_post("/auth/uploads/track", {
                        "file_name": mg_f.name, "file_size_bytes": len(raw),
                        "db_type": "mongodb", "destination": f"{mg_db}.{mg_coll}",
                        "connection_name": mg_conn_name, "status": "error", "error_detail": str(err),
                    })
                else:
                    inserted = out.get("inserted_count", len(records))
                    st.success(f"✅ **{mg_f.name}** → `{mg_db}.{mg_coll}` — {inserted:,} documents")
                    api_post("/auth/uploads/track", {
                        "file_name": mg_f.name, "file_size_bytes": len(raw),
                        "row_count": inserted, "db_type": "mongodb",
                        "destination": f"{mg_db}.{mg_coll}",
                        "connection_name": mg_conn_name, "status": "success",
                    })
                    st.session_state.pop("upload_history", None)

    # ═══════════════════════════════════════════════════════════
    # TAB 4 — UPLOAD HISTORY
    # ═══════════════════════════════════════════════════════════
    with tab_hist:
        if st.button("🔄 Refresh", key="btn_hist_refresh"):
            st.session_state.pop("upload_history", None)

        if "upload_history" not in st.session_state:
            st.session_state["upload_history"] = api_get("/auth/uploads")

        uploads = st.session_state.get("upload_history", [])

        if not uploads or not isinstance(uploads, list):
            st.info("No uploads yet.")
        else:
            total      = len(uploads)
            success    = sum(1 for u in uploads if u.get("status") == "success")
            total_rows = sum(u.get("row_count") or 0 for u in uploads)
            int_count  = sum(1 for u in uploads if "Internal" in (u.get("connection_name") or ""))
            ext_count  = total - int_count

            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Total Files",   total)
            m2.metric("Successful",    success)
            m3.metric("Total Rows",    f"{total_rows:,}")
            m4.metric("Internal",      int_count)
            m5.metric("External",      ext_count)
            st.divider()

            for u in uploads:
                is_ok   = u.get("status") == "success"
                is_int  = "Internal" in (u.get("connection_name") or "")
                db_icon = "📂" if is_int else ("🐘" if u.get("db_type") == "postgresql" else "🍃")
                sc      = "#00d4a8" if is_ok else "#ff4757"
                sbg     = "rgba(0,212,168,.08)" if is_ok else "rgba(255,71,87,.08)"
                ts      = (u.get("uploaded_at") or "")[:16]
                rows_s  = f"{u.get('row_count'):,} rows" if u.get("row_count") else "—"
                size_s  = f"{u.get('file_size_bytes',0)//1024:,} KB" if u.get("file_size_bytes") else "—"

                cc, cd = st.columns([10, 1])
                with cc:
                    lbl = u.get("connection_name") or "—"
                    st.markdown(f"""
                    <div style="background:#0e1420;border:1px solid #1e2d4a;border-radius:12px;
                                padding:.75rem 1.2rem;margin-bottom:.4rem;border-left:3px solid {sc}">
                      <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px">
                        <span style="font-family:IBM Plex Mono,monospace;font-size:.82rem;
                                     font-weight:600;color:#c8d8f0">{db_icon} {u.get("file_name","")}</span>
                        <span style="background:{sbg};color:{sc};border:1px solid {sc};
                                     border-radius:20px;padding:1px 8px;
                                     font-family:IBM Plex Mono,monospace;font-size:.6rem;
                                     font-weight:700">{"✓ SUCCESS" if is_ok else "✗ FAILED"}</span>
                      </div>
                      <div style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#566a8a">
                        📍 {u.get("destination","—")} &nbsp;·&nbsp; 🔌 {lbl}
                        &nbsp;·&nbsp; 📊 {rows_s} &nbsp;·&nbsp; 💾 {size_s} &nbsp;·&nbsp; 🕐 {ts}
                      </div>
                    </div>""", unsafe_allow_html=True)
                with cd:
                    st.write("")
                    if st.button("🗑", key=f"del_up_{u['id']}"):
                        api_delete(f"/auth/uploads/{u['id']}")
                        st.session_state.pop("upload_history", None)
                        st.rerun()

elif page == "🩺 Health":
    h1, h2 = st.columns(2)
    with h1:
        st.markdown("#### Service Status")
        if st.button("🔄 Check All Services", use_container_width=True):
            statuses = []
            for svc, path in [("FastAPI Backend", "/health"), ("MongoDB", "/mongo/ping"), ("PostgreSQL", "/db/ping")]:
                r = api_get(path)
                ok = list(r.values())[0] in ("ok", "connected") if r else False
                statuses.append((svc, ok))
            for svc, ok in statuses:
                sc  = "#00e5b4" if ok else "#ef4444"
                sbg = "rgba(0,229,180,.08)" if ok else "rgba(239,68,68,.08)"
                st.markdown(
                    f'<div style="background:{sbg};border:1px solid {sc}30;border-radius:8px;' +
                    f'padding:.5rem 1rem;margin-bottom:.4rem;font-family:JetBrains Mono,monospace;font-size:.78rem">' +
                    f'<span style="color:{sc};font-weight:700">{"✓" if ok else "✗"}</span>' +
                    f'&nbsp; <span style="color:#e2eaf8">{svc}</span>' +
                    f'<span style="float:right;color:{sc};font-size:.65rem">{"ONLINE" if ok else "OFFLINE"}</span></div>',
                    unsafe_allow_html=True
                )
    with h2:
        st.markdown("#### Logged-in User")
        me = api_get("/auth/me")
        if "user_id" in me: st.json(me)