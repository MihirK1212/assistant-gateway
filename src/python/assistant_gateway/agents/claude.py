from typing import Any, Dict, List, Optional

from assistant_gateway.agents.claude_utils.agent_interaction import run_claude_agent_for_interactions
from assistant_gateway.schemas import AgentInteraction, AgentOutput
from claude_agent_sdk import ClaudeAgentOptions


class ClaudeBaseAgent:
    def get_claude_agent_options(input_overrides: Optional[Dict[str, Dict[str, Any]]] = None) -> ClaudeAgentOptions:
        raise NotImplementedError(
            "get_claude_agent_options is not implemented"
        )

    def run(
        self, interactions: List[AgentInteraction], input_overrides: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> AgentOutput:
        claude_agent_options = self.get_claude_agent_options(input_overrides)
        return run_claude_agent_for_interactions(claude_agent_options, interactions)
