from app.state.agent_state import AgentState
from app.db import get_conn
from app.services.schema_summary import build_schema_prompt


class SchemaAgent:
    """
    Responsible for discovering table schemas
    and storing them in AgentState (workspace memory).

    For each selected dataset, it stores:
      - quoted table name (for SQL execution)
      - column metadata
      - schema_prompt (for NL → SQL generation)
    """

    def run(self, state: AgentState) -> AgentState:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for dataset_id in state.selected_datasets:
                    # 1️⃣ Resolve schema + table for this dataset
                    cur.execute(
                        """
                        SELECT table_schema_name, table_name
                        FROM dataset_registry
                        WHERE dataset_id = %s AND user_id = %s
                        """,
                        (dataset_id, state.user_id),
                    )
                    r = cur.fetchone()
                    if not r:
                        continue

                    schema_name, table_name = r

                    # 2️⃣ Fetch column metadata
                    cur.execute(
                        """
                        SELECT column_name, pg_type
                        FROM dataset_columns
                        WHERE dataset_id = %s
                        ORDER BY ordinal_position
                        """,
                        (dataset_id,),
                    )

                    columns = [
                        {"name": c[0], "pg_type": c[1]}
                        for c in cur.fetchall()
                    ]

                    # 3️⃣ Build LLM-friendly schema prompt
                    table_fqn_for_prompt = f"{schema_name}.{table_name}"
                    schema_prompt = build_schema_prompt(
                        table_fqn_for_prompt,
                        columns,
                    )

                    # 4️⃣ Write into shared agent state
                    state.datasets[dataset_id] = {
                        # Quoted name for SQL execution
                        "table": f"\"{schema_name}\".\"{table_name}\"",

                        # Column metadata
                        "columns": columns,

                        # Prompt used by NLToSQLAgent
                        "schema_prompt": schema_prompt,
                    }

            return state
        finally:
            conn.close()
