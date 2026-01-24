from __future__ import annotations

from typing import Dict, Mapping, Optional

from assistant_gateway.agents.base import Agent

from assistant_gateway.chat_orchestrator.core.config import AgentConfig
from assistant_gateway.chat_orchestrator.core.schemas import (
    BackendServerContext,
    UserContext,
)
from assistant_gateway.chat_orchestrator.core.schemas import (
    GatewayDefaultFallbackConfig,
)


class AgentSessionManager:
    """
    Maintains long-lived agent instances per chat so the same MCP session
    (tools, auth, cached state) can be reused across API calls.
    """

    def __init__(
        self,
        *,
        agent_configs: Mapping[str, AgentConfig],
        default_fallback_config: Optional[GatewayDefaultFallbackConfig] = None,
    ) -> None:
        self._agent_configs: Dict[str, AgentConfig] = agent_configs
        self._default_fallback_config: Optional[GatewayDefaultFallbackConfig] = (
            default_fallback_config
        )
        self._sessions: Dict[str, Agent] = {}

    def get_or_create(
        self,
        *,
        chat_id: str,
        agent_name: str,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> Agent:
        if chat_id not in self._sessions:
            agent_config = self._resolve_agent_config(agent_name)
            self._sessions[chat_id] = self._build_agent(
                config=agent_config,
                user_context=user_context,
                backend_server_context=backend_server_context,
                default_fallback_config=self._default_fallback_config,
            )
        return self._sessions[chat_id]

    def drop(self, chat_id: str) -> None:
        if chat_id in self._sessions:
            del self._sessions[chat_id]

    def _resolve_agent_config(self, agent_name: str) -> AgentConfig:
        if agent_name in self._agent_configs:
            return self._agent_configs[agent_name]
        available = ", ".join(sorted(self._agent_configs.keys()))
        raise ValueError(f"Unknown agent '{agent_name}'. Available agents: {available}")

    def _build_agent(
        self,
        *,
        config: AgentConfig,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
        default_fallback_config: Optional[GatewayDefaultFallbackConfig] = None,
    ) -> Agent:
        return config.builder(
            user_context=user_context,
            backend_server_context=backend_server_context,
            default_fallback_config=default_fallback_config,
        )
