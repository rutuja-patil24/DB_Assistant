# backend/app/agents/orchestrator.py
from __future__ import annotations

import logging
from app.state.agent_state import AgentState

# PostgreSQL pipeline agents
from app.agents.pg_schema_agent    import PgSchemaAgent
from app.agents.pg_nl_to_sql_agent import PgNLToSQLAgent
from app.agents.pg_safety_agent    import PgSafetyAgent
from app.agents.pg_execution_agent import PgExecutionAgent

# Shared post-processing agents (work for both PG and Mongo)
from app.agents.insight_agent       import InsightAgent
from app.agents.visualization_agent import VisualizationAgent

# MongoDB agent (already existed and working)
from app.agents.mongo_query_agent import MongoQueryAgent

# Profiling agent
from app.agents.profiling_agent import ProfilingAgent

# EDA agent
from app.agents.eda_agent import EDAAgent

# Original SchemaAgent kept for uploaded datasets (internal_datasets flow)
from app.agents.schema_agent    import SchemaAgent
from app.agents.nl_to_sql_agent import NLToSQLAgent
from app.agents.safety_agent    import SafetyAgent
from app.agents.execution_agent import ExecutionAgent

logger = logging.getLogger("db_assistant.orchestrator")


class Orchestrator:
    """
    Central Orchestrator — routes work across agents using shared AgentState.

    Three pipelines:
      1. run_pg_query()      — PostgreSQL NL query via pg_uri
      2. run_mongo_query()   — MongoDB NL query via mongo_uri
      3. run_dataset_query() — Uploaded dataset query via dataset_registry
    """

    def __init__(self):
        # PostgreSQL pipeline
        self.pg_schema_agent    = PgSchemaAgent()
        self.pg_nl_to_sql_agent = PgNLToSQLAgent()
        self.pg_safety_agent    = PgSafetyAgent()
        self.pg_execution_agent = PgExecutionAgent()

        # MongoDB pipeline
        self.mongo_query_agent = MongoQueryAgent()

        # Uploaded dataset pipeline
        self.schema_agent    = SchemaAgent()
        self.nl_to_sql_agent = NLToSQLAgent()
        self.safety_agent    = SafetyAgent()
        self.execution_agent = ExecutionAgent()

        # Shared post-processing
        self.profiling_agent     = ProfilingAgent()
        self.eda_agent           = EDAAgent()
        self.insight_agent       = InsightAgent()
        self.visualization_agent = VisualizationAgent()

    # ──────────────────────────────────────────────────────────
    # Pipeline 1: PostgreSQL NL Query
    # ──────────────────────────────────────────────────────────
    def run_pg_query(self, state: AgentState) -> AgentState:
        """
        Full agentic PostgreSQL query pipeline:
        SchemaAgent → NLToSQLAgent → SafetyAgent → ExecutionAgent
        → InsightAgent → VisualizationAgent
        """
        logger.info("Orchestrator: starting PostgreSQL pipeline for: %s", state.user_question)

        # Step 1: Discover schemas + enum values + join hints
        state = self.pg_schema_agent.run(state)
        if state.execution_error:
            return state

        # Step 2: Generate SQL from natural language
        state = self.pg_nl_to_sql_agent.run(state)
        if state.execution_error:
            return state

        # Step 3: Validate SQL safety
        state = self.pg_safety_agent.run(state)
        if state.execution_error:
            return state

        # Step 4: Execute SQL
        state = self.pg_execution_agent.run(state)
        if state.execution_error:
            return state

        # Step 5: Profile results
        state = self.profiling_agent.run(state)

        # Step 6: EDA insights via Gemini
        state = self.eda_agent.run(state)

        # Step 7: Generate summary insights
        state = self.insight_agent.run(state)

        # Step 8: Determine visualization spec
        state = self.visualization_agent.run(state)

        logger.info(
            "Orchestrator: PostgreSQL pipeline complete — %d rows, %dms",
            len(state.results), state.execution_time_ms or 0
        )
        return state

    # ──────────────────────────────────────────────────────────
    # Pipeline 2: MongoDB NL Query (single collection)
    # ──────────────────────────────────────────────────────────
    def run_mongo_query(
        self,
        state: AgentState,
        schema_prompt: str,
        date_field=None,
    ) -> AgentState:
        """
        MongoDB single-collection query pipeline:
        MongoQueryAgent → InsightAgent → VisualizationAgent

        Note: schema_prompt and execution are handled by the caller (mongo.py)
        because MongoDB needs the live pymongo client. The agent handles
        query PLANNING; the route handles execution.
        """
        logger.info("Orchestrator: starting MongoDB pipeline for: %s", state.user_question)

        # Step 1: Plan MongoDB query via MongoQueryAgent
        spec = self.mongo_query_agent.run(
            schema_prompt=schema_prompt,
            question=state.user_question,
            date_field=date_field,
            limit=state.limit,
        )
        state.generated_mongo = spec

        # Steps 2+3 (InsightAgent + VisualizationAgent) run AFTER
        # the route executes the query and puts results in state.results.
        # Call run_post_processing() after execution.
        return state

    # ──────────────────────────────────────────────────────────
    # Pipeline 3: Uploaded Dataset Query
    # ──────────────────────────────────────────────────────────
    def run_dataset_query(self, state: AgentState) -> AgentState:
        """
        Uploaded dataset query pipeline:
        SchemaAgent → NLToSQLAgent → SafetyAgent → ExecutionAgent
        → InsightAgent → VisualizationAgent
        """
        logger.info("Orchestrator: starting dataset pipeline for: %s", state.user_question)

        # Step 1: Load schema from dataset_registry
        state = self.schema_agent.run(state)
        if state.execution_error:
            return state

        # Step 2: Generate SQL
        state = self.nl_to_sql_agent.run(state)
        if state.execution_error:
            return state

        # Step 3: Safety check
        state = self.safety_agent.run(state)
        if state.execution_error:
            return state

        # Step 4: Execute
        state = self.execution_agent.run(state)
        if state.execution_error:
            return state

        # Step 5: Profile results
        state = self.profiling_agent.run(state)

        # Step 6: EDA insights via Gemini
        state = self.eda_agent.run(state)

        # Step 7: Generate summary insights
        state = self.insight_agent.run(state)

        # Step 8: Visualization spec
        state = self.visualization_agent.run(state)

        return state

    # ──────────────────────────────────────────────────────────
    # Shared post-processing (called after MongoDB execution)
    # ──────────────────────────────────────────────────────────
    def run_post_processing(self, state: AgentState) -> AgentState:
        """
        Run ProfilingAgent → EDAAgent → InsightAgent → VisualizationAgent.
        Used by all routes after query execution.
        """
        state = self.profiling_agent.run(state)
        state = self.eda_agent.run(state)
        state = self.insight_agent.run(state)
        state = self.visualization_agent.run(state)
        return state