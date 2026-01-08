from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from assistant_gateway.schemas import AgentInteraction, AgentOutput 


class Agent(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    async def run(
        self,
        interactions: List[AgentInteraction]
    ) -> AgentOutput:
        raise NotImplementedError("Subclasses must implement this method")
