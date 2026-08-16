from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class RunAgentExecutorPayload:
    """
    Handles the serialization and deserialization of the payload for the agent executor.

    Serialized to a plain dict for distributed execution (Celery / ClauqBTM).
    """

    chat_id: str
    agent_name: str
    input_overrides: Optional[Dict[str, Dict[str, Any]]] = field(default=None)

    def serialize(self) -> Dict[str, Any]:
        return {
            "chat_id": self.chat_id,
            "agent_name": self.agent_name,
            "input_overrides": self.input_overrides,
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "RunAgentExecutorPayload":
        chat_id = data.get("chat_id")
        if chat_id is None:
            raise ValueError("chat_id is required in executor payload")

        agent_name = data.get("agent_name")
        if agent_name is None:
            raise ValueError("agent_name is required in executor payload")

        return cls(
            chat_id=chat_id,
            agent_name=agent_name,
            input_overrides=data.get("input_overrides"),
        )
