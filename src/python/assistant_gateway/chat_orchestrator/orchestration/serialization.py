from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from assistant_gateway.chat_orchestrator.core.schemas import (
    BackendServerContext,
    ChatMetadata,
    UserContext,
)


@dataclass
class RunAgentExecutorPayload:
    """
    Strongly-typed payload for the agent executor.

    This class defines the exact structure of data passed to the executor.
    It handles serialization (for Celery) and deserialization (in workers).
    """

    chat: ChatMetadata
    user_context: Optional[UserContext] = None
    backend_server_context: Optional[BackendServerContext] = None

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize the payload for distributed execution (Celery).

        Converts Pydantic models to JSON-serializable dicts.
        """
        return {
            "chat": self.chat.model_dump(mode="json"),
            "user_context": (
                self.user_context.model_dump(mode="json") if self.user_context else None
            ),
            "backend_server_context": (
                self.backend_server_context.model_dump(mode="json")
                if self.backend_server_context
                else None
            ),
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "RunAgentExecutorPayload":
        """
        Deserialize the payload from a dict (received from Celery).

        Converts dicts back to Pydantic models.
        """
        chat_data = data.get("chat")
        if chat_data is None:
            raise ValueError("chat is required in executor payload")

        chat = (
            ChatMetadata.model_validate(chat_data)
            if isinstance(chat_data, dict)
            else chat_data
        )

        user_context = None
        user_ctx_data = data.get("user_context")
        if user_ctx_data:
            user_context = (
                UserContext.model_validate(user_ctx_data)
                if isinstance(user_ctx_data, dict)
                else user_ctx_data
            )

        backend_server_context = None
        backend_ctx_data = data.get("backend_server_context")
        if backend_ctx_data:
            backend_server_context = (
                BackendServerContext.model_validate(backend_ctx_data)
                if isinstance(backend_ctx_data, dict)
                else backend_ctx_data
            )

        return cls(
            chat=chat,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )
