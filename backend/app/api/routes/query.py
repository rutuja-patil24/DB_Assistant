from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from typing import Optional, List

import os

from app.state.agent_state import AgentState
from app.agents.orchestrator import Orchestrator

router = APIRouter(prefix="/query", tags=["query"])
orch = Orchestrator()


class QueryRequest(BaseModel):
    dataset_id: Optional[str] = None          # single dataset mode
    dataset_ids: Optional[List[str]] = None   # multi-dataset mode (joins)
    question: str
    limit: int = 50

    # ✅ TEMP/DEV: allow passing SQL directly for testing ExecutionAgent
    manual_sql: Optional[str] = None


@router.post("/")
def run_query(req: QueryRequest, x_user_id: str = Header(..., alias="X-User-Id")):
    workspace_id = x_user_id

    # normalize selected datasets
    selected = []
    if req.dataset_ids:
        selected = req.dataset_ids
    elif req.dataset_id:
        selected = [req.dataset_id]

    if not selected:
        raise HTTPException(status_code=400, detail="Provide dataset_id or dataset_ids")

    # Build AgentState
    state = AgentState(
        user_id=x_user_id,
        workspace_id=workspace_id,
        user_question=req.question,
        selected_datasets=selected,
    )

    # ✅ DEV shortcut: run schema agent first, then execute manual SQL
    # Enable by setting ALLOW_MANUAL_SQL=1 in your environment
    if req.manual_sql and os.getenv("ALLOW_MANUAL_SQL") == "1":
        try:
            # 1) Load schemas into state.datasets (needed for later steps too)
            state = orch.schema_agent.run(state)

            # 2) Set SQL + mark safety passed (or you can run SafetyAgent too)
            state.generated_sql = req.manual_sql

            # optional: actually run your safety agent check instead of forcing True
            state = orch.safety_agent.run(state)

            # 3) Execute
            state = orch.execution_agent.run(state)

        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Manual SQL pipeline failed: {e}")

    else:
        # Normal pipeline (will work fully after you implement NLToSQL + Execution)
        try:
            state = orch.run_query(state)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Agent pipeline failed: {e}")

    return {
        "workspace_id": workspace_id,
        "user_id": x_user_id,
        "selected_datasets": selected,
        "question": req.question,
        "sql": getattr(state, "generated_sql", None),
        "safety_passed": getattr(state, "safety_passed", False),
        "count": len(getattr(state, "results", []) or []),
        "data": getattr(state, "results", None),
        "execution_error": getattr(state, "execution_error", None),
        "execution_time_ms": getattr(state, "execution_time_ms", None),
    }
