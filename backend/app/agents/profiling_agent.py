# backend/app/agents/profiling_agent.py
from __future__ import annotations

import logging
import numbers
from typing import Any, Dict, List, Optional

from app.state.agent_state import AgentState

logger = logging.getLogger("db_assistant.profiling_agent")


def _is_numeric(v: Any) -> bool:
    return isinstance(v, numbers.Number) and not isinstance(v, bool)


def _profile_column(col_name: str, values: List[Any]) -> Dict:
    """
    Compute profile stats for a single column.
    Returns dict with: type, count, nulls, unique, min, max, mean, top_values
    """
    total    = len(values)
    non_null = [v for v in values if v is not None and v != ""]
    nulls    = total - len(non_null)

    if not non_null:
        return {
            "col":    col_name,
            "type":   "unknown",
            "count":  total,
            "nulls":  nulls,
            "unique": 0,
        }

    # Detect type
    numeric_vals = [v for v in non_null if _is_numeric(v)]
    is_num = len(numeric_vals) == len(non_null)

    profile: Dict[str, Any] = {
        "col":   col_name,
        "type":  "numeric" if is_num else "text",
        "count": total,
        "nulls": nulls,
        "null_pct": round(nulls / total * 100, 1) if total > 0 else 0,
        "unique": len(set(str(v) for v in non_null)),
    }

    if is_num:
        profile["min"]  = round(float(min(numeric_vals)), 2)
        profile["max"]  = round(float(max(numeric_vals)), 2)
        profile["mean"] = round(float(sum(numeric_vals) / len(numeric_vals)), 2)
        profile["sum"]  = round(float(sum(numeric_vals)), 2)
    else:
        # Top 3 most frequent values
        from collections import Counter
        counts = Counter(str(v) for v in non_null)
        profile["top_values"] = [
            {"value": v, "count": c}
            for v, c in counts.most_common(3)
        ]

    return profile


def _generate_warnings(col_profiles: List[Dict], total_rows: int) -> List[str]:
    """Generate data quality warnings from column profiles."""
    warnings = []

    for p in col_profiles:
        col   = p["col"]
        nulls = p.get("nulls", 0)
        null_pct = p.get("null_pct", 0)

        # High null rate
        if null_pct >= 50:
            warnings.append(f"⚠️ Column '{col}' has {null_pct}% missing values")
        elif null_pct >= 20:
            warnings.append(f"ℹ️ Column '{col}' has {null_pct}% missing values")

        # All same value (low cardinality)
        if p.get("unique") == 1 and total_rows > 1:
            warnings.append(f"ℹ️ Column '{col}' has only one unique value")

        # Numeric: min == max (no variation)
        if p.get("type") == "numeric":
            if p.get("min") == p.get("max") and total_rows > 1:
                warnings.append(f"ℹ️ Column '{col}' has no variation (all values = {p['min']})")

    return warnings


class ProfilingAgent:
    """
    Agent — Data Profiling.

    Runs AFTER ExecutionAgent. Analyzes query results to produce:
      - Per-column stats (type, nulls, min/max/mean, top values)
      - Data quality warnings
      - Row/column summary

    Reads from:  state.results
    Writes to:   state.profile   — full profile dict
                 state.warnings  — data quality warning strings
    """

    def run(self, state: AgentState) -> AgentState:
        rows = getattr(state, "results", None) or []

        if not rows:
            state.profile = {"total_rows": 0, "columns": [], "warnings": []}
            return state

        if not isinstance(rows[0], dict):
            state.profile = {"total_rows": len(rows), "columns": [], "warnings": []}
            return state

        col_names = list(rows[0].keys())
        total_rows = len(rows)

        # Build per-column value lists
        col_values: Dict[str, List] = {col: [] for col in col_names}
        for row in rows:
            for col in col_names:
                col_values[col].append(row.get(col))

        # Profile each column
        col_profiles = [
            _profile_column(col, col_values[col])
            for col in col_names
        ]

        # Generate warnings
        warnings = _generate_warnings(col_profiles, total_rows)
        state.warnings = warnings

        # Store full profile in state
        state.profile = {
            "total_rows":  total_rows,
            "total_cols":  len(col_names),
            "columns":     col_profiles,
            "warnings":    warnings,
        }

        logger.info(
            "ProfilingAgent: %d rows × %d cols, %d warnings",
            total_rows, len(col_names), len(warnings)
        )

        return state