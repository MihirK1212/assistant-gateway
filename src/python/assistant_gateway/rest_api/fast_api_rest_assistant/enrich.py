from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache
from typing import AsyncIterator, List, Optional

from fastapi import FastAPI

from assistant_gateway.rest_api.fast_api_rest_assistant.router import (
    get_orchestrator,
    router as assistant_router,
)
from assistant_gateway.chat_orchestrator.core.config import GatewayConfig
from assistant_gateway.chat_orchestrator.orchestration import ConversationOrchestrator


# Module-level orchestrator reference for lifecycle management
_orchestrator: Optional[ConversationOrchestrator] = None


def enrich_app_with_assistant_router(
    *, app: FastAPI, config: GatewayConfig, api_prefix: str, router_tags: List[str]
) -> FastAPI:
    """
    Enrich a FastAPI app with the assistant router.

    This function:
    1. Creates a ConversationOrchestrator instance
    2. Sets up the lifespan to start/stop the orchestrator
    3. Injects the orchestrator as a dependency

    The orchestrator's start() method is called on app startup to initialize
    the queue manager (required for background task execution).
    """
    global _orchestrator

    gateway_config = config

    @lru_cache()
    def orchestrator_factory() -> ConversationOrchestrator:
        global _orchestrator
        if _orchestrator is None:
            _orchestrator = ConversationOrchestrator(config=gateway_config)
        return _orchestrator

    # Create lifespan that composes with any existing lifespan
    existing_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Start our orchestrator
        orchestrator = orchestrator_factory()
        await orchestrator.start()

        try:
            # Run existing lifespan if present
            if existing_lifespan is not None:
                async with existing_lifespan(app):
                    yield
            else:
                yield
        finally:
            # Stop our orchestrator
            await orchestrator.stop()

    app.router.lifespan_context = lifespan

    app.dependency_overrides[get_orchestrator] = orchestrator_factory
    app.include_router(assistant_router, prefix=api_prefix, tags=router_tags)
    return app
