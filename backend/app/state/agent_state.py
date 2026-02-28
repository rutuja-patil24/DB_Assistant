# backend/app/state/agent_state.py
from __future__ import annotations
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


@dataclass
class AgentState:
    """
    Shared state passed across ALL agents (blackboard pattern).
    Every agent reads from and writes to this single object.
    The Orchestrator passes it through the pipeline.
    """

    # ── Identity ─────────────────────────────────────────────
    user_id:      str = ""
    workspace_id: str = ""

    # ── Query source: "postgresql" | "mongodb" ───────────────
    source: str = "postgresql"

    # ── PostgreSQL context ───────────────────────────────────
    pg_uri:         Optional[str]        = None   # live connection URI
    tables_schema:  Dict[str, List[Dict]] = field(default_factory=dict)
    # fqn -> [{name, pg_type}]

    # ── MongoDB context ──────────────────────────────────────
    mongo_uri:      Optional[str]  = None
    mongo_db:       Optional[str]  = None
    mongo_collection: Optional[str] = None
    mongo_collections: List[str]   = field(default_factory=list)

    # ── Uploaded-dataset context (internal_datasets) ─────────
    datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # dataset_id -> {table, columns, schema_prompt}
    selected_datasets: List[str] = field(default_factory=list)

    # ── User question & config ───────────────────────────────
    user_question: Optional[str] = None
    limit:         int            = 50

    # ── Enum / categorical values fetched from DB ────────────
    enum_values: Dict[str, List[str]] = field(default_factory=dict)
    # "fqn.colname" -> ["val1","val2",...]

    # ── JOIN hints ────────────────────────────────────────────
    join_hints: List[str] = field(default_factory=list)

    # ── Planning ──────────────────────────────────────────────
    intent:    Optional[str]       = None
    join_plan: Dict[str, Any]      = field(default_factory=dict)

    # ── Generated query ───────────────────────────────────────
    generated_sql:   Optional[str]       = None   # PostgreSQL
    generated_mongo: Optional[Dict]      = None   # MongoDB spec

    # ── Safety ────────────────────────────────────────────────
    safety_passed: bool      = False
    warnings:      List[str] = field(default_factory=list)

    # ── Execution results ─────────────────────────────────────
    results:            List[Dict[str, Any]] = field(default_factory=list)
    columns:            List[str]            = field(default_factory=list)
    execution_error:    Optional[str]        = None
    execution_time_ms:  Optional[int]        = None
    tables_used:        List[str]            = field(default_factory=list)

    # ── Post-processing ───────────────────────────────────────
    profile:      Optional[Dict] = None   # ProfilingAgent
    summary:      Optional[str]  = None   # InsightAgent
    viz:          Optional[Dict] = None   # VisualizationAgent
    eda_insights: Optional[Dict] = None   # EDAAgent