from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Mapping, Optional

from assistant_gateway.agents.base import Agent

from assistant_gateway.chat_orchestrator.core.schemas import (
    UserContext,
    BackendServerContext,
    GatewayDefaultFallbackConfig,
)
from assistant_gateway.chat_orchestrator.chat.store import ChatStore, InMemoryChatStore
from assistant_gateway.clauq_btm import QueueManager, InMemoryQueueManager


@dataclass
class AgentConfig:
    """
    Declarative configuration for a single agent.

    `builder` is responsible for constructing the Agent. It will be called with:
    - user_context: Optional[UserContext]
    - backend_server_context: Optional[BackendServerContext]
    - default_fallback_config: Optional[GatewayDefaultFallbackConfig]
    """

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
    - chat_store / queue_manager: can be overridden; defaults are in-memory.
    """

    agent_configs: Mapping[str, AgentConfig]
    chat_store: ChatStore
    queue_manager: QueueManager
    default_fallback_config: Optional[GatewayDefaultFallbackConfig] = None

    def get_chat_store(self) -> ChatStore:
        return self.chat_store or InMemoryChatStore()

    def get_queue_manager(self) -> QueueManager:
        return self.queue_manager or InMemoryQueueManager()

    def get_agent_configs(self) -> Dict[str, AgentConfig]:
        return dict(self.agent_configs)
