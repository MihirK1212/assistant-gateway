from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Mapping, Optional

from assistant_gateway.agents.base import Agent
from assistant_gateway.chat_orchestrator.chat.store import ChatStore, InMemoryChatStore
from assistant_gateway.chat_orchestrator.core.schemas import (
    BackendServerContext,
    GatewayDefaultFallbackConfig,
    UserContext,
)

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm import ClauqBTM


@dataclass
class AgentConfig:
    name: str
    builder: Callable[
        [
            Optional[UserContext],
            Optional[BackendServerContext],
            Optional[GatewayDefaultFallbackConfig],
        ],
        Agent,
    ]


@dataclass
class GatewayConfig:
    """
    Configuration required to spin up the FastAPI gateway with minimal inputs.

    - fallback_backend_url: used when neither the request nor the agent config provides one.
    - agent_configs: mapping of agent name to configuration.
    - default_agent_name: used when a chat omits the agent name.
    - chat_store: can be overridden; defaults to in-memory.
    - clauq_btm: ClauqBTM instance for background task management.
                 Required for background task execution.
    """

    agent_configs: Mapping[str, AgentConfig]
    chat_store: Optional[ChatStore] = None
    clauq_btm: Optional["ClauqBTM"] = None
    default_fallback_config: Optional[GatewayDefaultFallbackConfig] = None

    def get_chat_store(self) -> ChatStore:
        return self.chat_store or InMemoryChatStore()

    def get_clauq_btm(self) -> "ClauqBTM":
        if self.clauq_btm is None:
            raise ValueError(
                "clauq_btm is not configured. "
                "Please provide a ClauqBTM instance in GatewayConfig."
            )
        return self.clauq_btm

    def get_agent_configs(self) -> Dict[str, AgentConfig]:
        return dict(self.agent_configs)
