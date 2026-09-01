from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from assistant_gateway.schemas import AgentInteraction, AgentOutput


class Agent(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    async def run(
        self,
        interactions: List[AgentInteraction],
        input_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> AgentOutput:
        raise NotImplementedError("Subclasses must implement this method")
