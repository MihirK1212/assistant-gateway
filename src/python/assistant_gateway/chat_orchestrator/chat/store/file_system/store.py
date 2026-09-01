from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, List, Optional

from assistant_gateway.chat_orchestrator.chat.store.base import ChatStore
from assistant_gateway.chat_orchestrator.core.schemas import ChatMetadata
from assistant_gateway.schemas import AgentInteraction


class FileSystemChatStore(ChatStore):
    """
    File system implementation that persists chat data to a JSON file.
    """

    def __init__(self, file_path: str | Path | None = None) -> None:
        if file_path is None:
            module_dir = Path(__file__).parent
            file_path = module_dir / "chats.json"
        
        self._file_path = Path(file_path)
        self._lock = asyncio.Lock()
        self._ensure_file_exists()

    async def create_chat(self, chat: ChatMetadata) -> ChatMetadata:
        async with self._lock:
            data = self._read_data()
            data["chats"][chat.chat_id] = self._serialize_chat(chat)
            if chat.chat_id not in data["interactions"]:
                data["interactions"][chat.chat_id] = []
            self._write_data(data)
        return chat

    async def get_chat(self, chat_id: str) -> Optional[ChatMetadata]:
        async with self._lock:
            data = self._read_data()
            chat_data = data["chats"].get(chat_id)
            if chat_data is None:
                return None
            return self._deserialize_chat(chat_data)

    async def update_chat(self, chat: ChatMetadata) -> ChatMetadata:
        async with self._lock:
            data = self._read_data()
            data["chats"][chat.chat_id] = self._serialize_chat(chat)
            self._write_data(data)
        return chat

    async def append_interaction(self, chat_id: str, interaction: AgentInteraction) -> None:
        async with self._lock:
            data = self._read_data()
            if chat_id not in data["interactions"]:
                data["interactions"][chat_id] = []
            data["interactions"][chat_id].append(self._serialize_interaction(interaction))
            self._write_data(data)

    async def list_interactions(self, chat_id: str) -> List[AgentInteraction]:
        async with self._lock:
            data = self._read_data()
            interactions_data = data["interactions"].get(chat_id, [])
            return [self._deserialize_interaction(i) for i in interactions_data]

    def _ensure_file_exists(self) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._file_path.exists():
            self._write_data({"chats": {}, "interactions": {}})

    def _read_data(self) -> Dict:
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except (json.JSONDecodeError, FileNotFoundError):
            return {"chats": {}, "interactions": {}}

    def _write_data(self, data: Dict) -> None:
        with open(self._file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _serialize_chat(self, chat: ChatMetadata) -> Dict:
        return chat.model_dump(mode='json')

    def _deserialize_chat(self, data: Dict) -> ChatMetadata:
        return ChatMetadata(**data)

    def _serialize_interaction(self, interaction: AgentInteraction) -> Dict:
        return interaction.model_dump(mode='json')

    def _deserialize_interaction(self, data: Dict) -> AgentInteraction:
        from assistant_gateway.schemas import AgentOutput, Role, UserInput
        
        role = data.get('role')
        if role == Role.user or role == 'user':
            return UserInput(**data)
        elif role == Role.assistant or role == 'assistant':
            return AgentOutput(**data)
        else:
            return AgentInteraction(**data)

    