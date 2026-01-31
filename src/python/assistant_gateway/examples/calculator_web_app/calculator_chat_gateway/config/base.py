import os

from assistant_gateway.chat_orchestrator.chat.store import FileSystemChatStore
from assistant_gateway.chat_orchestrator.core.config import (
    AgentConfig,
    GatewayConfig,
    GatewayDefaultFallbackConfig,
)
from assistant_gateway.clauq_btm import ClauqBTM
from assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.agent import (
    build_calculator_agent,
)

import dotenv

dotenv.load_dotenv()


def build_gateway_config() -> GatewayConfig:
    """
    Compose GatewayConfig with:
    - one calculator AgentConfig that uses the dynamic builder above
    - in-memory chat store
    - ClauqBTM instance for task management (requires Redis for background tasks)

    Note: If Redis is not available, sync tasks will still work but
    background tasks will fail.
    """

    default_fallback = GatewayDefaultFallbackConfig(
        fallback_backend_url="http://127.0.0.1:5000"
    )

    # Create ClauqBTM instance
    # Uses REDIS_URL env var or defaults to localhost
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    clauq_btm = ClauqBTM(redis_url=redis_url)

    return GatewayConfig(
        agent_configs={
            "calculator": AgentConfig(
                name="calculator",
                builder=build_calculator_agent,
            ),
        },
        default_fallback_config=default_fallback,
        chat_store=FileSystemChatStore(),
        clauq_btm=clauq_btm,
    )
