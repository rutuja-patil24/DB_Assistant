# 🗄️ DB Assistant — AI-Powered Database Query Tool

> **Natural language queries for PostgreSQL & MongoDB, powered by Google Gemini**

DB Assistant lets you query your databases in plain English. Type *"Show me total revenue by region"* and get results instantly — no SQL required. Built with a multi-agent AI pipeline, real-time EDA profiling, and a sleek dark-themed Streamlit UI.

---

## ✨ Features

- 🤖 **Natural Language → SQL** — Gemini converts plain English to SQL or MongoDB queries
- 🐘 **PostgreSQL NL Query** — Auto-detects tables, JOINs, and filters from your question
- 🍃 **MongoDB NL Query** — Natural language queries across any MongoDB collection
- 🔍 **Direct SQL / Direct Mongo** — Run raw queries with a built-in editor
- 📁 **Upload Datasets** — Upload CSV/Excel files and query them like a database
- 📊 **EDA Profile** — Gemini-powered Exploratory Data Analysis with key findings, data quality scores, and recommendations
- 📈 **Charts & Insights** — Auto-generated visualizations from query results
- 🔐 **Auth System** — JWT-based login with encrypted connection storage
- 📝 **Audit Log** — Full query history with execution times
- 💚 **Health Dashboard** — Service and database health monitoring

---

## 🏗️ Architecture

```
User (Browser)
     │
     ▼
Streamlit Frontend (port 8501)
     │
     ▼
FastAPI Backend (port 8000)
     │
     ├── Auth API          (/auth/*)
     ├── PostgreSQL API    (/pg/*)
     ├── MongoDB API       (/mongo/*)
     └── Datasets API      (/my-datasets/*)
              │
              ▼
     Agent Orchestrator
              │
              ├── SchemaAgent        → reads DB schema
              ├── NLtoSQLAgent       → Gemini: text → SQL
              ├── SafetyAgent        → SQL validation & guard
              ├── ExecutionAgent     → runs query on DB
              ├── ProfilingAgent     → stats per column
              ├── EDAAgent           → Gemini: narrative insights
              ├── InsightAgent       → summary text
              └── VisualizationAgent → chart spec

Internal PostgreSQL (Docker, port 5433)
     └── users, connections, dataset_registry, audit_log

Your Databases (any host)
     ├── PostgreSQL (any URI)
     └── MongoDB    (any URI)
```

---

## 📋 Prerequisites

Before you start, make sure you have:

| Tool | Version | Download |
|------|---------|----------|
| Python | 3.10+ | https://python.org |
| Docker Desktop | Latest | https://docker.com |
| Git | Any | https://git-scm.com |
| Google Gemini API Key | — | https://aistudio.google.com |

---

## 🚀 Quick Start (Step-by-Step)

### Step 1 — Clone the Repository

```bash
git clone https://github.com/rutuja-patil24/DB_Assistant.git
cd DB_Assistant
```

---

### Step 2 — Start the Internal Database (Docker)

The app uses a PostgreSQL container to store users, connections, and uploaded datasets.

```bash
cd infra
docker-compose up -d
```

Verify it is running:

```bash
docker ps
```

You should see `database_assistant_db` running and mapped to port **5433**.

> ⚠️ Keep Docker running whenever you use the app.

---

### Step 3 — Set Up Environment Variables

Go back to the backend directory and create your `.env` file:

```bash
cd ../backend
```

**Windows:**
```cmd
copy nul .env
```

**Mac/Linux:**
```bash
touch .env
```

Open `.env` in any text editor and add:

```env
# ── Internal Database (Docker PostgreSQL) ──────────────────────
DATABASE_URL=postgresql://da_user:da_pass@localhost:5433/da_db

# ── Security ────────────────────────────────────────────────────
SECRET_KEY=your-secret-key-here-change-this
ENCRYPTION_KEY=your-fernet-key-here

# ── Google Gemini ───────────────────────────────────────────────
GEMINI_API_KEY=your-gemini-api-key-here

# ── MongoDB (optional) ──────────────────────────────────────────
MONGO_URI=mongodb://localhost:27017
```

**Generate SECRET_KEY:**
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

**Generate ENCRYPTION_KEY:**
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Get a Gemini API Key:**
1. Go to https://aistudio.google.com/app/apikey
2. Click **Create API Key**
3. Copy and paste into `.env`

---

### Step 4 — Create a Virtual Environment (Recommended)

```bash
# From the backend/ directory
python -m venv venv
```

**Activate it:**

Windows:
```cmd
venv\Scripts\activate
```

Mac/Linux:
```bash
source venv/bin/activate
```

---

### Step 5 — Install Python Dependencies

```bash
# From the backend/ directory (with venv active)
pip install -r requirements.txt
pip install -r requirements_auth.txt
```

---

### Step 6 — Install Streamlit Dependencies

```bash
pip install streamlit requests plotly numpy
```

---

### Step 7 — Start the Backend

```bash
# From the backend/ directory
uvicorn app.main:app --reload --port 8000
```

You should see:
```
INFO:     Uvicorn running on http://127.0.0.1:8000
INFO:     Application startup complete.
```

Test the API is live: http://localhost:8000/docs

---

### Step 8 — Start the Frontend

Open a **new terminal** (keep the backend running in the first one):

```bash
# From the project root (where streamlit_app.py is)
streamlit run streamlit_app.py --server.port 8501
```

Open your browser: **http://localhost:8501**

---

### Step 9 — Register and Log In

1. Click **Register** on the login page
2. Create an account with your email and password
3. Log in

---

### Step 10 — Connect Your Database

1. Click **My Connections** in the sidebar
2. Click **Add Connection**
3. Enter a name and your PostgreSQL or MongoDB URI
4. Click **Save**

Your connection is encrypted before being stored.

---

## 🐳 Full Docker Setup

If you want to run the backend inside Docker as well:

**1. Build the backend image:**

```bash
cd backend
docker build -t db-assistant-backend .
```

**2. Create `docker-compose.full.yml` in the project root:**

```yaml
services:
  db:
    image: postgres:15
    container_name: database_assistant_db
    environment:
      POSTGRES_DB: da_db
      POSTGRES_USER: da_user
      POSTGRES_PASSWORD: da_pass
    ports:
      - "5433:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./infra/db/init.sql:/docker-entrypoint-initdb.d/init.sql

  backend:
    build: ./backend
    container_name: db-assistant-backend
    ports:
      - "8000:8000"
    env_file:
      - ./backend/.env
    depends_on:
      - db
    environment:
      DATABASE_URL: postgresql://da_user:da_pass@db:5432/da_db

volumes:
  postgres_data:
```

**3. Start everything:**

```bash
docker-compose -f docker-compose.full.yml up -d
```

**4. Start the Streamlit frontend locally:**

```bash
streamlit run streamlit_app.py --server.port 8501
```

---

## 📁 Project Structure

```
DB_Assistant/
├── streamlit_app.py                  # Frontend UI
│
├── backend/
│   ├── app/
│   │   ├── main.py                   # FastAPI entry point
│   │   ├── agents/
│   │   │   ├── orchestrator.py       # Pipeline coordinator
│   │   │   ├── eda_agent.py          # Gemini EDA insights
│   │   │   ├── profiling_agent.py    # Column statistics
│   │   │   ├── insight_agent.py      # Summary generation
│   │   │   ├── visualization_agent.py
│   │   │   ├── pg_schema_agent.py    # PostgreSQL schema reader
│   │   │   ├── pg_nl_to_sql_agent.py # NL → SQL
│   │   │   ├── pg_safety_agent.py    # SQL validation
│   │   │   ├── pg_execution_agent.py
│   │   │   ├── schema_agent.py       # Dataset schema reader
│   │   │   ├── nl_to_sql_agent.py
│   │   │   ├── safety_agent.py
│   │   │   ├── execution_agent.py
│   │   │   └── mongo_query_agent.py  # MongoDB NL queries
│   │   ├── api/routes/
│   │   │   ├── auth.py               # JWT auth endpoints
│   │   │   ├── pg_query.py           # PostgreSQL routes
│   │   │   ├── mongo.py              # MongoDB routes
│   │   │   └── internal_datasets.py  # Dataset upload & query
│   │   ├── core/
│   │   │   ├── sql_guard.py          # SQL injection protection
│   │   │   └── db.py                 # Internal DB connection
│   │   ├── services/
│   │   │   └── nl_to_sql.py          # Gemini API wrapper
│   │   └── state/
│   │       └── agent_state.py        # Shared pipeline state
│   ├── requirements.txt
│   ├── requirements_auth.txt
│   └── Dockerfile
│
└── infra/
    ├── docker-compose.yml            # Internal PostgreSQL
    └── db/
        └── init.sql
```

---

## 🔧 Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | ✅ | Internal PostgreSQL (Docker) connection string |
| `SECRET_KEY` | ✅ | JWT signing secret — use 32+ random characters |
| `ENCRYPTION_KEY` | ✅ | Fernet key for encrypting stored DB passwords |
| `GEMINI_API_KEY` | ✅ | Google Gemini API key |
| `MONGO_URI` | ❌ | Default MongoDB URI (optional) |

---

## 🧠 How the AI Pipeline Works

Every query goes through a multi-agent pipeline:

```
Your Question
      │
      ▼
 SchemaAgent        reads available tables & columns
      │
      ▼
 NLtoSQLAgent       sends schema + question to Gemini → SQL
      │
      ▼
 SafetyAgent        validates SQL (SELECT only, blocks DROP/DELETE)
      │
      ▼
 ExecutionAgent     runs the validated SQL on your database
      │
      ▼
 ProfilingAgent     computes per-column stats
      │
      ▼
 EDAAgent           Gemini generates headline, key findings,
                    data quality score, recommendations
      │
      ▼
 InsightAgent       builds plain-text summary
      │
      ▼
 VisualizationAgent picks the best chart type
      │
      ▼
  Results → Table / Charts & Insights / EDA Profile tabs
```

---

## 🛠️ Troubleshooting

### `cryptography.fernet.InvalidToken` on login
Your `ENCRYPTION_KEY` changed after connections were saved. Go to **My Connections**, delete the old connection, and re-add it with the same credentials.

### `relation "dataset_registry" does not exist`
Tables are auto-created on startup. Restart the backend:
```bash
uvicorn app.main:app --reload --port 8000
```

If the error persists, connect to the Docker DB and drop/recreate:
```bash
docker exec -it database_assistant_db psql -U da_user -d da_db
DROP TABLE IF EXISTS dataset_columns CASCADE;
DROP TABLE IF EXISTS dataset_registry CASCADE;
\q
```
Then restart the backend.

### Docker container won't start
```bash
docker-compose down -v
docker-compose up -d
```

### Gemini 429 Rate Limit
The free Gemini tier has rate limits. The app retries automatically (3 attempts: 5s → 15s → 30s delay). If it keeps failing, wait 1 minute and try again, or upgrade your Gemini plan.

### Port 8000 already in use

**Windows:**
```cmd
netstat -ano | findstr :8000
taskkill /PID <PID_NUMBER> /F
```

**Mac/Linux:**
```bash
lsof -ti:8000 | xargs kill -9
```

### Streamlit can't reach the backend
Confirm the backend is running on port 8000. Check the top of `streamlit_app.py`:
```python
API_BASE_URL = "http://localhost:8000"
```

### `ModuleNotFoundError` on startup
Make sure your virtual environment is active and all dependencies are installed:
```bash
venv\Scripts\activate      # Windows
pip install -r requirements.txt
pip install -r requirements_auth.txt
```

---

## 📦 Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Streamlit, Plotly |
| Backend | FastAPI, Python 3.10+ |
| AI | Google Gemini 2.0 Flash |
| Internal DB | PostgreSQL 15 (Docker) |
| Auth | JWT (python-jose), Fernet encryption |
| MongoDB driver | Motor (async) |
| PostgreSQL driver | psycopg2-binary |
| Data processing | Pandas, NumPy |

---

## 🙋 FAQ

**Q: Does it work with cloud databases?**  
Yes — any PostgreSQL or MongoDB accessible from your machine works (AWS RDS, Supabase, MongoDB Atlas, etc.)

**Q: Is my database password safe?**  
Yes — all connection passwords are encrypted with Fernet symmetric encryption before being stored in the internal database.

**Q: Can I upload large CSV/Excel files?**  
Optimized for files up to ~50MB. Larger files may be slow depending on your machine.

**Q: Does it support MySQL or SQLite?**  
Not currently. PostgreSQL and MongoDB only.

**Q: Can multiple users share the same instance?**  
Yes — the auth system supports multiple accounts. Each user's connections and uploaded datasets are isolated.

---

## 👩‍💻 Author

**Rutuja Patil** — [@rutuja-patil24](https://github.com/rutuja-patil24)

---

## 📄 License

This project is for educational and portfolio purposes.
