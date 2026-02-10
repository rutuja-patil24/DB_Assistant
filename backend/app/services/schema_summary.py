from typing import List, Dict

def build_schema_prompt(table_fqn: str, columns: List[Dict[str, str]]) -> str:
    """
    table_fqn: e.g. u_user1.ds_a09f7ab4...
    columns: [{"name": "order_id", "pg_type": "bigint"}, ...]
    """
    lines = [f"Table: {table_fqn}", "Columns:"]
    for c in columns:
        lines.append(f"- {c['name']} ({c['pg_type']})")
    return "\n".join(lines)
