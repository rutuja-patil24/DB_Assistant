# backend/app/agents/insight_agent.py
from __future__ import annotations

import numbers
from app.state.agent_state import AgentState


def _fmt(v) -> str:
    """Format a number with commas, no trailing .0 for integers."""
    try:
        f = float(v)
        if f == int(f):
            return f"{int(f):,}"
        return f"{f:,.2f}"
    except Exception:
        return str(v)


def _label_for_row(row: dict, exclude_col: str) -> str:
    """Build a readable label from non-numeric context columns."""
    label_parts = []
    for k, v in row.items():
        if k == exclude_col:
            continue
        if v is None or v == "":
            continue
        # Skip numeric columns in the label — we want category/name context
        if isinstance(v, numbers.Number):
            continue
        label_parts.append(str(v))
        if len(label_parts) == 2:
            break
    return ", ".join(label_parts)


class InsightAgent:
    """
    Agent — Insight Generation.

    Produces clean, natural-language insight sentences.
    Uses ProfilingAgent data (state.profile) when available.

    Reads from:  state.results, state.profile (optional)
    Writes to:   state.summary  (plain text — no markdown syntax)
    """

    def run(self, state: AgentState) -> AgentState:
        rows    = getattr(state, "results", None) or []
        profile = getattr(state, "profile", None)
        state.summary = None

        if not rows:
            state.summary = "No rows returned for this query."
            return state

        total_rows = len(rows)

        if profile and profile.get("columns"):
            state.summary = self._from_profile(rows, profile, total_rows)
        else:
            state.summary = self._from_rows(rows, total_rows)

        return state

    def _from_profile(self, rows, profile, total_rows) -> str:
        col_profiles = profile["columns"]
        numeric_cols = [p for p in col_profiles if p.get("type") == "numeric"]
        text_cols    = [p for p in col_profiles if p.get("type") == "text"]

        parts = []

        # ── Numeric insights ──────────────────────────────────
        for p in numeric_cols[:2]:
            col  = p["col"]
            mn   = p.get("min")
            mx   = p.get("max")
            mean = p.get("mean")
            total_sum = p.get("sum")

            if mx is None:
                continue

            try:
                top_row = max(rows, key=lambda r, c=col: float(r.get(c) or 0))
                bot_row = min(rows, key=lambda r, c=col: float(r.get(c) or 0))
                top_label = _label_for_row(top_row, col)
                bot_label = _label_for_row(bot_row, col)

                col_display = col.replace("_", " ")

                sentence = f"Highest {col_display} is {_fmt(mx)}"
                if top_label:
                    sentence += f" ({top_label})"

                if mn is not None and mn != mx:
                    sentence += f", lowest is {_fmt(mn)}"
                    if bot_label and bot_label != top_label:
                        sentence += f" ({bot_label})"

                if mean is not None:
                    sentence += f", average is {_fmt(mean)}"

                if total_sum is not None and total_rows > 1:
                    sentence += f", total is {_fmt(total_sum)}"

                parts.append(sentence)

            except Exception:
                if mn is not None and mx is not None:
                    parts.append(
                        f"{col.replace('_',' ')} ranges from {_fmt(mn)} to {_fmt(mx)}"
                        + (f", average {_fmt(mean)}" if mean is not None else "")
                    )

        # ── Text column insight — only if meaningful cardinality ──
        for p in text_cols[:1]:
            top_vals = p.get("top_values", [])
            unique   = p.get("unique", 0)
            col_display = p["col"].replace("_", " ")

            # Skip if every value appears only once (e.g. list of unique names)
            if top_vals and top_vals[0].get("count", 1) > 1:
                tv = top_vals[0]
                parts.append(
                    f"Most frequent {col_display} is '{tv['value']}' "
                    f"({tv['count']} of {total_rows} rows)"
                )
            elif unique > 0 and total_rows > 1:
                parts.append(f"{unique} unique {col_display} values across {total_rows} rows")

        row_label = "row" if total_rows == 1 else "rows"
        header = f"{total_rows} {row_label} returned"
        if parts:
            return header + ". " + ". ".join(parts) + "."
        return header + "."

    def _from_rows(self, rows, total_rows) -> str:
        """Fallback when no profile data."""
        if not isinstance(rows[0], dict):
            return f"{total_rows} rows returned."

        keys = list(rows[0].keys())
        numeric_cols = [k for k in keys if isinstance(rows[0].get(k), numbers.Number)
                        and not isinstance(rows[0].get(k), bool)]

        if not numeric_cols:
            return f"{total_rows} rows returned."

        measure = numeric_cols[0]
        col_display = measure.replace("_", " ")

        try:
            sorted_rows = sorted(
                rows, key=lambda r: float(r.get(measure) or 0), reverse=True
            )
            top    = sorted_rows[0]
            bottom = sorted_rows[-1]

            top_label = _label_for_row(top, measure)
            bot_label = _label_for_row(bottom, measure)

            sentence = f"Highest {col_display} is {_fmt(top.get(measure))}"
            if top_label:
                sentence += f" ({top_label})"

            if len(sorted_rows) > 1:
                sentence += f", lowest is {_fmt(bottom.get(measure))}"
                if bot_label and bot_label != top_label:
                    sentence += f" ({bot_label})"

            row_label = "row" if total_rows == 1 else "rows"
            return f"{total_rows} {row_label} returned. {sentence}."

        except Exception:
            return f"{total_rows} rows returned."