from __future__ import annotations

from typing import List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import ChatMetadata
from assistant_gateway.schemas import AgentInteraction


class ChatStore:
    """Abstraction for persisting chat metadata and messages."""

    async def create_chat(self, chat: ChatMetadata) -> ChatMetadata:
        raise NotImplementedError

    async def get_chat(self, chat_id: str) -> Optional[ChatMetadata]:
        raise NotImplementedError

    async def update_chat(self, chat: ChatMetadata) -> ChatMetadata:
        raise NotImplementedError

    async def append_interaction(self, chat_id: str, interaction: AgentInteraction) -> None:
        raise NotImplementedError

    async def list_interactions(self, chat_id: str) -> List[AgentInteraction]:
        raise NotImplementedError
