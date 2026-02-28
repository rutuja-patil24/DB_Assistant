# backend/app/agents/eda_agent.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from app.services.nl_to_sql import _call_gemini_text
from app.state.agent_state import AgentState

logger = logging.getLogger("db_assistant.eda_agent")

SYSTEM_PROMPT = """You are an expert data analyst generating EDA (Exploratory Data Analysis) insights.

Given a dataset profile (column stats, distributions, data quality), produce a structured JSON analysis.

You must return ONLY valid JSON — no markdown, no explanation, no extra text.

Output format:
{
  "headline": "One punchy sentence summarizing the most important finding (max 15 words)",
  "key_findings": [
    "Finding 1 — specific, quantitative, actionable (e.g. 'Revenue is concentrated in West region at 62% of total')",
    "Finding 2",
    "Finding 3"
  ],
  "data_quality": {
    "score": 95,
    "issues": ["List any null/missing/zero-variance issues"],
    "verdict": "One sentence health verdict"
  },
  "column_insights": [
    {
      "col": "column_name",
      "insight": "One specific insight about this column with actual numbers"
    }
  ],
  "recommendations": [
    "Actionable recommendation based on the data (e.g. 'Investigate why East region has 40% lower average order value')"
  ]
}

Rules:
- Use ACTUAL numbers from the profile — never say "some" or "many", always give exact figures
- column_insights: only include the 3 most interesting columns (skip ID columns, skip columns with 1 unique value unless it's a quality issue)
- key_findings: exactly 3 findings, ordered by importance
- recommendations: 1-2 max, only if genuinely useful
- Be direct and specific — avoid generic observations like "the data looks clean"
"""


def _build_profile_prompt(
    profile: Dict,
    user_question: Optional[str],
    row_count: int,
) -> str:
    col_profiles = profile.get("columns", [])
    warnings = profile.get("warnings", [])

    lines = []
    lines.append(f"Dataset: {row_count} rows × {len(col_profiles)} columns")
    if user_question:
        lines.append(f"Original query: {user_question}")
    lines.append("")

    lines.append("=== COLUMN PROFILES ===")
    for p in col_profiles:
        col = p["col"]
        ctype = p.get("type", "unknown")
        nulls = p.get("null_pct", 0)
        unique = p.get("unique", 0)

        if ctype == "numeric":
            mn = p.get("min")
            mx = p.get("max")
            mean = p.get("mean")
            total = p.get("sum")
            lines.append(
                f"  [{col}] NUMERIC | unique={unique} | nulls={nulls}% | "
                f"min={mn} max={mx} mean={mean} sum={total}"
            )
        else:
            top = p.get("top_values", [])
            top_str = ", ".join(
                f"'{t['value']}'={t['count']}" for t in top[:3]
            )
            lines.append(
                f"  [{col}] TEXT | unique={unique} | nulls={nulls}% | "
                f"top: {top_str}"
            )

    if warnings:
        lines.append("")
        lines.append("=== DATA QUALITY WARNINGS ===")
        for w in warnings:
            lines.append(f"  {w}")

    return "\n".join(lines)


class EDAAgent:
    """
    Agent — Gemini-powered EDA Analysis.

    Uses Gemini to generate intelligent, narrative insights from
    ProfilingAgent's statistical output.

    Reads from:  state.profile, state.user_question, state.results
    Writes to:   state.eda_insights  — structured dict with headline,
                                       findings, quality score, recommendations
                 state.summary       — plain text headline + key findings
    """

    def run(self, state: AgentState) -> AgentState:
        profile = getattr(state, "profile", None)

        if not profile or not profile.get("columns"):
            logger.info("EDAAgent: no profile data, skipping")
            return state

        rows = getattr(state, "results", None) or []
        row_count = len(rows)

        if row_count == 0:
            return state

        try:
            profile_prompt = _build_profile_prompt(
                profile,
                state.user_question,
                row_count,
            )

            raw = _call_gemini_text(SYSTEM_PROMPT, profile_prompt)

            # Strip markdown fences if present
            clean = raw.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[-1]
                clean = clean.rsplit("```", 1)[0]
            clean = clean.strip()

            insights = json.loads(clean)

            # Store structured insights in state
            state.eda_insights = insights

            # Also update summary with headline + findings for Charts tab
            headline = insights.get("headline", "")
            findings = insights.get("key_findings", [])
            if headline:
                summary_parts = [headline]
                summary_parts.extend(findings[:3])
                state.summary = " | ".join(summary_parts)

            logger.info(
                "EDAAgent: generated insights — headline: %s",
                headline[:60] if headline else "none",
            )

        except Exception as e:
            logger.warning("EDAAgent: Gemini call failed (%s), keeping existing summary", e)
            # Non-fatal — profiling data still available in state.profile

        return state