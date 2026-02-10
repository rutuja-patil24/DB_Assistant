from typing import Dict, List, Any
from dataclasses import dataclass, field

@dataclass
class AgentState:
    """
    Shared state passed across agents (workspace-level memory)
    """

    # workspace
    user_id: str
    workspace_id: str

    # datasets
    datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # dataset_id -> {table, schema, profile, summary}

    # query context
    user_question: str | None = None
    selected_datasets: List[str] = field(default_factory=list)

    # planning + reasoning
    intent: str | None = None
    join_plan: Dict[str, Any] | None = None

    # outputs
    generated_sql: str | None = None
    execution_result: Dict[str, Any] | None = None

    # safety & audit
    safety_passed: bool = False
    warnings: List[str] = field(default_factory=list)
