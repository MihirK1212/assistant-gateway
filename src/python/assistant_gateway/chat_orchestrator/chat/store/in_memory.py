from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import ChatMetadata
from assistant_gateway.schemas import AgentInteraction
from assistant_gateway.chat_orchestrator.chat.store.base import ChatStore


class InMemoryChatStore(ChatStore):
    """
    In-memory implementation that mirrors a document-style NoSQL layout.
    Replace with a real backend (e.g., MongoDB, DynamoDB) without changing the
    public interface.
    """

    def __init__(self) -> None:
        self._chats: Dict[str, ChatMetadata] = {}
        self._interactions: Dict[str, List[AgentInteraction]] = {}
        self._lock = asyncio.Lock()

    async def create_chat(self, chat: ChatMetadata) -> ChatMetadata:
        async with self._lock:
            self._chats[chat.chat_id] = chat
            self._interactions.setdefault(chat.chat_id, [])
        return chat

    async def get_chat(self, chat_id: str) -> Optional[ChatMetadata]:
        async with self._lock:
            return self._chats.get(chat_id)

    async def update_chat(self, chat: ChatMetadata) -> ChatMetadata:
        async with self._lock:
            self._chats[chat.chat_id] = chat
        return chat

    async def append_interaction(self, chat_id: str, interaction: AgentInteraction) -> None:
        async with self._lock:
            self._interactions.setdefault(chat_id, []).append(interaction)

    async def list_interactions(self, chat_id: str) -> List[AgentInteraction]:
        async with self._lock:
            return list(self._interactions.get(chat_id, []))
