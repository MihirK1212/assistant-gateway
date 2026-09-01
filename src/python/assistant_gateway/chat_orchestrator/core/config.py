from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Mapping, Optional

from assistant_gateway.agents.base import Agent
from assistant_gateway.chat_orchestrator.chat.store import ChatStore, InMemoryChatStore

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm import ClauqBTM


@dataclass
class GatewayConfig:
    """
    Configuration required to spin up the FastAPI gateway.

    - agent_configs: mapping of agent name to a zero-argument factory that
      creates an Agent instance. Agents are created once per chat and cached
      in memory; runtime context (auth tokens, backend URLs, etc.) is supplied
      via input_overrides on each Agent.run() call instead.
    - chat_store: can be overridden; defaults to in-memory.
    - clauq_btm: required for background task execution.
    """

    agent_configs: Mapping[str, Callable[[], Agent]]
    chat_store: Optional[ChatStore] = None
    clauq_btm: Optional["ClauqBTM"] = None

    def get_chat_store(self) -> ChatStore:
        return self.chat_store or InMemoryChatStore()

    def get_clauq_btm(self) -> "ClauqBTM":
        if self.clauq_btm is None:
            raise ValueError(
                "clauq_btm is not configured. "
                "Please provide a ClauqBTM instance in GatewayConfig."
            )
        return self.clauq_btm

    def get_agent_configs(self) -> Dict[str, Callable[[], Agent]]:
        return dict(self.agent_configs)
