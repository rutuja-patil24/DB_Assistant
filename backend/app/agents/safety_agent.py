from app.state.agent_state import AgentState
from app.core.sql_guard import assert_safe_select, SQLGuardError


class SafetyAgent:
    """
    Ensures the generated SQL is a safe SELECT query
    before execution.
    """

    def run(self, state: AgentState) -> AgentState:
        state.safety_passed = False

        if not state.generated_sql:
            return state

        try:
            assert_safe_select(state.generated_sql)
            state.safety_passed = True

        except SQLGuardError as e:
            # Surface error into state so API returns clean 400
            state.safety_passed = False
            state.execution_error = f"Safety check failed: {e}"

        return state
