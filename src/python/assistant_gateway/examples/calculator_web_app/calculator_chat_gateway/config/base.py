import os

from assistant_gateway.chat_orchestrator.chat.store import InMemoryChatStore
from assistant_gateway.chat_orchestrator.core.config import (
    AgentConfig,
    GatewayConfig,
    GatewayDefaultFallbackConfig,
)
from assistant_gateway.chat_orchestrator.tasks_queue_manager import (
    InMemoryTasksQueueManager,
)
from assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.agent import (
    build_calculator_agent,
)

import os 
import dotenv 

dotenv.load_dotenv()


def build_gateway_config() -> GatewayConfig:
    """
    Compose GatewayConfig with:
    - one calculator AgentConfig that uses the dynamic builder above
    - in-memory chat store
    - in-memory queue manager
    """

    default_fallback = GatewayDefaultFallbackConfig(
        fallback_backend_url="http://127.0.0.1:5000/docs"
    )

    return GatewayConfig(
        agent_configs={
            "calculator": AgentConfig(
                name="calculator",
                builder=build_calculator_agent,
            ),
        },
        default_fallback_config=default_fallback,
        chat_store=InMemoryChatStore(),
        queue_manager=InMemoryTasksQueueManager(),
    )
