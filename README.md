<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&size=32&duration=3000&pause=1000&color=00E5B4&center=true&vCenter=true&width=600&lines=DB+Assistant;AI-Powered+Database+Tool;Natural+Language+%E2%86%92+SQL" alt="Typing SVG" />

<br/>

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Gemini](https://img.shields.io/badge/Google_Gemini-2.0_Flash-4285F4?style=for-the-badge&logo=google&logoColor=white)

<br/>

> **Query your databases in plain English. No SQL knowledge required.**  
> Powered by a multi-agent AI pipeline built on Google Gemini 2.0 Flash.

<br/>

[🚀 Quick Start](#-quick-start) • [✨ Features](#-features) • [🏗️ Architecture](#%EF%B8%8F-architecture) • [🛠️ Troubleshooting](#%EF%B8%8F-troubleshooting) • [📦 Tech Stack](#-tech-stack)

</div>

---

## ✨ Features

<table>
<tr>
<td width="50%">

### 🤖 AI Query Engine
- Natural language → SQL via Gemini
- Auto-detects tables & JOINs
- MongoDB NL queries
- Multi-table JOIN planner

</td>
<td width="50%">

### 📊 Smart Analytics
- Gemini-powered EDA with narrative insights
- Data quality scoring (0–100)
- Auto-generated charts & visualizations
- Column-level statistical profiling

</td>
</tr>
<tr>
<td width="50%">

### 🔐 Security First
- JWT-based authentication
- Fernet-encrypted connection storage
- SQL injection guard (SQLGuard)
- SELECT-only query enforcement

</td>
<td width="50%">

### 🗄️ Multi-Source Support
- PostgreSQL (any host/cloud)
- MongoDB (any URI)
- CSV / Excel file uploads
- Multiple simultaneous connections

</td>
</tr>
</table>

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Streamlit Frontend :8501                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │ HTTP
┌───────────────────────────▼─────────────────────────────────────┐
│                    FastAPI Backend :8000                         │
│                                                                  │
│   /auth/*          /pg/*           /mongo/*      /my-datasets/* │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                    Agent Orchestrator                            │
│                                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐    │
│  │ SchemaAgent │→ │ NLtoSQLAgent │→ │    SafetyAgent       │    │
│  │ reads DB    │  │ Gemini→SQL   │  │  SQLGuard validation │    │
│  └─────────────┘  └──────────────┘  └──────────┬──────────┘    │
│                                                 │               │
│  ┌──────────────────────────────────────────────▼──────────┐   │
│  │ ExecutionAgent → ProfilingAgent → EDAAgent → InsightAgent│   │
│  │   runs SQL         col stats     Gemini NLG   summary    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
          │                                      │
┌─────────▼──────────┐              ┌────────────▼───────────────┐
│ Internal PostgreSQL│              │    Your Databases           │
│  Docker :5433      │              │  PostgreSQL • MongoDB       │
│  users, sessions   │              │  (any host or cloud)        │
│  dataset_registry  │              └────────────────────────────┘
└────────────────────┘
```

---

## 📋 Prerequisites

| Tool | Min Version | Link |
|------|------------|------|
| 🐍 Python | 3.10+ | [python.org](https://python.org) |
| 🐳 Docker Desktop | Latest | [docker.com](https://docker.com) |
| 🔑 Gemini API Key | — | [aistudio.google.com](https://aistudio.google.com/app/apikey) |
| 🔧 Git | Any | [git-scm.com](https://git-scm.com) |

---

## 🚀 Quick Start

### `1` — Clone the repo

```bash
git clone https://github.com/rutuja-patil24/DB_Assistant.git
cd DB_Assistant
```

---

### `2` — Start the internal database

```bash
cd infra
docker-compose up -d
```

```bash
docker ps  # ✅ should show database_assistant_db on port 5433
```

---

### `3` — Configure environment variables

```bash
cd ../backend
```

Create a `.env` file:

```env
# ── Internal Database (Docker) ─────────────────────────────────
DATABASE_URL=postgresql://da_user:da_pass@localhost:5433/da_db

# ── Security ────────────────────────────────────────────────────
SECRET_KEY=           # see generation command below
ENCRYPTION_KEY=       # see generation command below

# ── AI ──────────────────────────────────────────────────────────
GEMINI_API_KEY=       # from aistudio.google.com

# ── Optional ────────────────────────────────────────────────────
MONGO_URI=mongodb://localhost:27017
```

**Generate your keys:**

```bash
# SECRET_KEY
python -c "import secrets; print(secrets.token_hex(32))"

# ENCRYPTION_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

### `4` — Set up Python environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac / Linux
source venv/bin/activate
```

```bash
pip install -r requirements.txt
pip install -r requirements_auth.txt
pip install streamlit requests plotly numpy
```

---

### `5` — Start the backend

```bash
# from backend/
uvicorn app.main:app --reload --port 8000
```

> ✅ API docs available at http://localhost:8000/docs

---

### `6` — Start the frontend

Open a **new terminal**:

```bash
# from project root
streamlit run streamlit_app.py --server.port 8501
```

> ✅ Open http://localhost:8501 in your browser

---

### `7` — Register → Connect → Query

1. **Register** an account on the login page
2. Go to **My Connections** → add your PostgreSQL or MongoDB URI
3. Navigate to **PostgreSQL NL Query** or **MongoDB NL Query**
4. Type your question in plain English → hit **Run Query**

---

## 🐳 Full Docker Deployment

Run everything (backend + database) in containers:

**1. Build the backend image:**
```bash
cd backend
docker build -t db-assistant-backend .
```

**2. Create `docker-compose.full.yml` in project root:**

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

**3. Launch:**
```bash
docker-compose -f docker-compose.full.yml up -d

# Then start Streamlit locally
streamlit run streamlit_app.py --server.port 8501
```

---

## 📁 Project Structure

```
DB_Assistant/
│
├── 🖥️  streamlit_app.py              ← Frontend UI
│
├── 📦  backend/
│   ├── app/
│   │   ├── main.py                   ← FastAPI entry point
│   │   │
│   │   ├── 🤖 agents/
│   │   │   ├── orchestrator.py       ← Pipeline coordinator
│   │   │   ├── eda_agent.py          ← Gemini EDA insights ✨
│   │   │   ├── profiling_agent.py    ← Column statistics
│   │   │   ├── insight_agent.py      ← Summary generation
│   │   │   ├── visualization_agent.py← Chart type selector
│   │   │   ├── pg_schema_agent.py    ← PostgreSQL schema reader
│   │   │   ├── pg_nl_to_sql_agent.py ← NL → SQL (PostgreSQL)
│   │   │   ├── pg_safety_agent.py    ← SQL safety check
│   │   │   ├── pg_execution_agent.py ← Query runner
│   │   │   ├── schema_agent.py       ← Dataset schema reader
│   │   │   ├── nl_to_sql_agent.py    ← NL → SQL (datasets)
│   │   │   ├── execution_agent.py    ← Dataset query runner
│   │   │   └── mongo_query_agent.py  ← MongoDB NL agent
│   │   │
│   │   ├── 🛣️  api/routes/
│   │   │   ├── auth.py               ← JWT auth endpoints
│   │   │   ├── pg_query.py           ← PostgreSQL routes
│   │   │   ├── mongo.py              ← MongoDB routes
│   │   │   └── internal_datasets.py  ← Upload & query CSVs
│   │   │
│   │   ├── 🔒 core/
│   │   │   ├── sql_guard.py          ← SQL injection protection
│   │   │   └── db.py                 ← Internal DB connection
│   │   │
│   │   ├── ⚙️  services/
│   │   │   └── nl_to_sql.py          ← Gemini API wrapper
│   │   │
│   │   └── 📋 state/
│   │       └── agent_state.py        ← Shared pipeline state
│   │
│   ├── requirements.txt
│   ├── requirements_auth.txt
│   └── Dockerfile
│
└── 🐳  infra/
    ├── docker-compose.yml
    └── db/init.sql
```

---

## 🧠 AI Pipeline — How It Works

```
 Your Question: "Show total revenue by region for successful payments"
       │
       ▼
 ┌─────────────┐
 │ SchemaAgent │  Reads all tables & columns from your database
 └──────┬──────┘
        │
        ▼
 ┌──────────────┐
 │ NLtoSQLAgent │  Sends schema + question to Gemini → gets SQL
 └──────┬───────┘
        │
        ▼
 ┌─────────────┐
 │ SafetyAgent │  Validates: SELECT only, no DROP/DELETE/ALTER
 └──────┬──────┘
        │
        ▼
 ┌────────────────┐
 │ ExecutionAgent │  Runs the validated SQL on your database
 └──────┬─────────┘
        │
        ▼
 ┌─────────────────┐
 │ ProfilingAgent  │  Computes min/max/mean/nulls per column
 └──────┬──────────┘
        │
        ▼
 ┌──────────┐
 │ EDAAgent │  Gemini generates: headline · key findings ·
 └──────┬───┘  data quality score · column insights · recommendations
        │
        ▼
 ┌──────────────┐   ┌────────────────────┐
 │ InsightAgent │   │VisualizationAgent  │
 │ plain text   │   │ picks chart type   │
 └──────┬───────┘   └────────┬───────────┘
        │                    │
        ▼                    ▼
  ┌─────────────────────────────────┐
  │   Table │ Charts │ EDA Profile  │
  └─────────────────────────────────┘
```

---

## 🔧 Environment Variables

| Variable | Required | Description |
|----------|:--------:|-------------|
| `DATABASE_URL` | ✅ | Internal PostgreSQL connection string (Docker) |
| `SECRET_KEY` | ✅ | JWT signing secret — 32+ random characters |
| `ENCRYPTION_KEY` | ✅ | Fernet key for encrypting stored DB passwords |
| `GEMINI_API_KEY` | ✅ | Google Gemini API key |
| `MONGO_URI` | ❌ | Default MongoDB URI (optional) |

---

## 🛠️ Troubleshooting

<details>
<summary><b>🔴 cryptography.fernet.InvalidToken on startup</b></summary>

Your `ENCRYPTION_KEY` changed after connections were saved.  
Go to **My Connections** → delete the old connection → re-add it.
</details>

<details>
<summary><b>🔴 relation "dataset_registry" does not exist</b></summary>

Tables are auto-created on startup. Restart the backend:
```bash
uvicorn app.main:app --reload --port 8000
```
If it persists, drop and recreate:
```bash
docker exec -it database_assistant_db psql -U da_user -d da_db
DROP TABLE IF EXISTS dataset_columns CASCADE;
DROP TABLE IF EXISTS dataset_registry CASCADE;
\q
```
Then restart.
</details>

<details>
<summary><b>🔴 Gemini 429 Rate Limit error</b></summary>

The free tier has rate limits. The app retries automatically (3× at 5s → 15s → 30s).  
Wait 1 minute and try again, or upgrade your Gemini plan.
</details>

<details>
<summary><b>🔴 Port 8000 already in use</b></summary>

Windows:
```cmd
netstat -ano | findstr :8000
taskkill /PID <PID> /F
```
Mac/Linux:
```bash
lsof -ti:8000 | xargs kill -9
```
</details>

<details>
<summary><b>🔴 Docker container won't start</b></summary>

```bash
docker-compose down -v
docker-compose up -d
```
</details>

<details>
<summary><b>🔴 Streamlit can't reach the backend</b></summary>

Check the top of `streamlit_app.py`:
```python
API_BASE_URL = "http://localhost:8000"
```
Make sure the backend is running on port 8000.
</details>

---

## 📦 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| 🖥️ Frontend | Streamlit + Plotly | UI & interactive charts |
| ⚡ Backend | FastAPI + Python 3.10 | REST API & agent orchestration |
| 🤖 AI | Google Gemini 2.0 Flash | NL→SQL, EDA insights |
| 🗄️ Internal DB | PostgreSQL 15 (Docker) | Users, sessions, datasets |
| 🔐 Auth | JWT + Fernet encryption | Secure auth & credential storage |
| 🍃 MongoDB | Motor (async driver) | MongoDB NL queries |
| 🐘 PostgreSQL | psycopg2-binary | PostgreSQL query execution |
| 📊 Data | Pandas + NumPy | Dataset processing & profiling |

---
