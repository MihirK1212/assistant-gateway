from __future__ import annotations

from typing import Callable, Dict, Mapping

from assistant_gateway.agents.base import Agent


class AgentSessionManager:
    def __init__(
        self,
        *,
        agent_configs: Mapping[str, Callable[[], Agent]],
    ) -> None:
        self._agent_configs: Dict[str, Callable[[], Agent]] = dict(agent_configs)
        self._sessions: Dict[str, Agent] = {}

    def get_or_create(
        self,
        *,
        chat_id: str,
        agent_name: str,
    ) -> Agent:
        if chat_id not in self._sessions:
            factory = self._resolve_factory(agent_name)
            self._sessions[chat_id] = factory()
        return self._sessions[chat_id]

    def drop(self, chat_id: str) -> None:
        if chat_id in self._sessions:
            del self._sessions[chat_id]

    def _resolve_factory(self, agent_name: str) -> Callable[[], Agent]:
        if agent_name in self._agent_configs:
            return self._agent_configs[agent_name]
        available = ", ".join(sorted(self._agent_configs.keys()))
        raise ValueError(f"Unknown agent '{agent_name}'. Available agents: {available}")
