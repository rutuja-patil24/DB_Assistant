from app.state.agent_state import AgentState
from app.agents.schema_agent import SchemaAgent
from app.agents.profiling_agent import ProfilingAgent
from app.agents.nl_to_sql_agent import NLToSQLAgent
from app.agents.safety_agent import SafetyAgent
from app.agents.execution_agent import ExecutionAgent


class Orchestrator:
    """
    The Brain:
    routes work across agents using shared AgentState
    """

    def __init__(self):
        self.schema_agent = SchemaAgent()
        self.profiling_agent = ProfilingAgent()
        self.nl_to_sql_agent = NLToSQLAgent()
        self.safety_agent = SafetyAgent()
        self.execution_agent = ExecutionAgent()

    def run_query(self, state: AgentState) -> AgentState:
        # 1) load schema + schema summary into state
        state = self.schema_agent.run(state)

        # 2) profiling (optional but recommended)
        state = self.profiling_agent.run(state)

        # 3) NL -> SQL
        state = self.nl_to_sql_agent.run(state)

        # 4) safety validation
        state = self.safety_agent.run(state)

        # 5) execute query
        state = self.execution_agent.run(state)

        return state
