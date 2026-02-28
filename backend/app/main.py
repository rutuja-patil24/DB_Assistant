from __future__ import annotations

import logging
import os
import traceback
from contextlib import asynccontextmanager
from typing import Any, Optional

import motor.motor_asyncio
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from pathlib import Path
load_dotenv(Path(__file__).parent.parent / ".env")

from app.api.routes.auth              import router as auth_router
from app.api.routes.history           import router as history_router
from app.api.routes.mongo             import router as mongo_router
from app.api.routes.pg_query          import router as pg_router
from app.api.routes.internal_datasets import router as datasets_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    mongo_uri = os.getenv("MONGO_URI", "")
    if mongo_uri:
        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(
                mongo_uri, serverSelectionTimeoutMS=3000)
            await client.admin.command("ping")
            logger.info("MongoDB connected")
        except Exception as exc:
            logger.warning(f"MongoDB not reachable at startup: {exc}")
    yield


app = FastAPI(
    title="DB Assistant API",
    version="2.0.0",
    description="Multi-agent natural language database assistant",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(history_router)
app.include_router(pg_router)
app.include_router(mongo_router)
app.include_router(datasets_router)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled error: %s\n%s", exc, traceback.format_exc())
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/db/ping", tags=["ops"])
def db_ping():
    from app.db import get_conn
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok", "message": "PostgreSQL reachable"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unreachable: {exc}")


@app.get("/mongo/ping", tags=["ops"])
async def mongo_ping():
    mongo_uri = os.getenv("MONGO_URI", "")
    if not mongo_uri:
        raise HTTPException(status_code=503, detail="MONGO_URI not configured")
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(
            mongo_uri, serverSelectionTimeoutMS=3000)
        await client.admin.command("ping")
        return {"status": "ok", "message": "MongoDB reachable"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unreachable: {exc}")