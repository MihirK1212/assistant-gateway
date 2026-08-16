from typing import Any, Dict, List, Optional

from assistant_gateway.agents.claude_utils.agent_interaction import run_claude_agent_for_interactions
from assistant_gateway.schemas import AgentInteraction, AgentOutput
from claude_agent_sdk import ClaudeAgentOptions


class ClaudeAgent:
    def get_mcp_server_options(input_overrides: Optional[Dict[str, Dict[str, Any]]] = None) -> ClaudeAgentOptions:
        raise NotImplementedError(
            "get_mcp_server_options is not implemented"
        )

    def run(
        self, interactions: List[AgentInteraction], input_overrides: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> AgentOutput:
        mcp_server_options = self.get_mcp_server_options(input_overrides)
        return run_claude_agent_for_interactions(mcp_server_options, interactions)
