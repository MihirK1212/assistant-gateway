from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import ChatMetadata
from assistant_gateway.schemas import AgentInteraction
from assistant_gateway.chat_orchestrator.chat.store.base import ChatStore


class FileSystemChatStore(ChatStore):
    """
    File system implementation that persists chat data to a JSON file.
    Uses a similar structure to the in-memory store but persists to disk.
    """

    def __init__(self, file_path: str | Path | None = None) -> None:
        """
        Initialize the file system chat store.
        
        Args:
            file_path: Path to the JSON file where chat data will be stored.
                      If None, defaults to 'chats.json' in the same directory as this module.
                      If the file doesn't exist, it will be created.
        """
        if file_path is None:
            # Use the directory where this module is located
            module_dir = Path(__file__).parent
            file_path = module_dir / "chats.json"
        
        self._file_path = Path(file_path)
        self._lock = asyncio.Lock()
        self._ensure_file_exists()

    def _ensure_file_exists(self) -> None:
        """Ensure the storage file and its parent directory exist."""
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._file_path.exists():
            self._write_data({"chats": {}, "interactions": {}})

    def _read_data(self) -> Dict:
        """Read and parse the JSON data from the file."""
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except (json.JSONDecodeError, FileNotFoundError):
            # If file is corrupted or missing, return empty structure
            return {"chats": {}, "interactions": {}}

    def _write_data(self, data: Dict) -> None:
        """Write the data structure to the JSON file."""
        with open(self._file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _serialize_chat(self, chat: ChatMetadata) -> Dict:
        """Serialize ChatMetadata to a dictionary with JSON-compatible types."""
        return chat.model_dump(mode='json')

    def _deserialize_chat(self, data: Dict) -> ChatMetadata:
        """Deserialize a dictionary to ChatMetadata."""
        return ChatMetadata(**data)

    def _serialize_interaction(self, interaction: AgentInteraction) -> Dict:
        """Serialize AgentInteraction to a dictionary with JSON-compatible types."""
        return interaction.model_dump(mode='json')

    def _deserialize_interaction(self, data: Dict) -> AgentInteraction:
        """Deserialize a dictionary to the appropriate AgentInteraction subclass."""
        from assistant_gateway.schemas import UserInput, AgentOutput, Role
        
        # Determine which subclass to use based on the role
        role = data.get('role')
        if role == Role.user or role == 'user':
            return UserInput(**data)
        elif role == Role.assistant or role == 'assistant':
            return AgentOutput(**data)
        else:
            # Fallback to base class if role is unknown
            return AgentInteraction(**data)

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
