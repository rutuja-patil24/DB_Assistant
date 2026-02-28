from __future__ import annotations

import numbers
from datetime import datetime
from app.state.agent_state import AgentState


def _looks_like_date(s: str) -> bool:
    if not isinstance(s, str):
        return False
    s = s.strip()
    # quick checks; supports YYYY-MM-DD and ISO-like
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            datetime.strptime(s[:19], fmt)
            return True
        except Exception:
            pass
    return False


class VisualizationAgent:
    """
    Outputs state.viz spec for UI to render.
    Types: bar | pie | line
    """

    def run(self, state: AgentState) -> AgentState:
        rows = getattr(state, "results", None) or []
        state.viz = None

        if not rows:
            return state
        first = rows[0]
        if not isinstance(first, dict):
            return state

        cols = list(first.keys())
        if len(cols) < 2:
            return state

        # Identify numeric measure
        value = None
        for c in cols:
            v = first.get(c)
            if isinstance(v, numbers.Number):
                value = c
                break

        if value is None:
            return state

        # Identify category / time axis
        category = None
        time_col = None
        for c in cols:
            if c == value:
                continue
            v = first.get(c)
            if isinstance(v, str) and _looks_like_date(v):
                time_col = c
                break

        if time_col is None:
            for c in cols:
                if c == value:
                    continue
                v = first.get(c)
                if isinstance(v, str):
                    category = c
                    break
            if category is None:
                # fallback: first non-numeric
                for c in cols:
                    if c == value:
                        continue
                    v = first.get(c)
                    if not isinstance(v, numbers.Number):
                        category = c
                        break

        q = (getattr(state, "user_question", "") or "").lower()

        # Choose chart type
        chart_type = "bar"
        axis = category

        if time_col is not None or any(k in q for k in ["trend", "over time", "daily", "monthly", "by date", "time series"]):
            chart_type = "line"
            axis = time_col
        elif any(k in q for k in ["share", "percentage", "distribution", "proportion", "split"]):
            chart_type = "pie"

        if not axis:
            return state

        state.viz = {
            "type": chart_type,
            "category": axis,   # x-axis for bar/line, labels for pie
            "value": value,
            "title": f"{value} by {axis}"
        }
        return state
