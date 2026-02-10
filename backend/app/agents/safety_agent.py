from app.state.agent_state import AgentState
from app.core.sql_guard import assert_safe_select  # adjust if your path differs

class SafetyAgent:
    def run(self, state: AgentState) -> AgentState:
        if state.generated_sql:
            assert_safe_select(state.generated_sql)
            state.safety_passed = True
        return state
