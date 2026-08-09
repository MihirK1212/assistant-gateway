from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache
from typing import AsyncIterator, List, Optional

from assistant_gateway.chat_orchestrator.core.config import GatewayConfig
from assistant_gateway.chat_orchestrator.orchestration import ConversationOrchestrator
from assistant_gateway.rest_api.fast_api_rest_assistant.router import (
    get_orchestrator,
    router as assistant_router,
)
from fastapi import FastAPI

_orchestrator: Optional[ConversationOrchestrator] = None


def enrich_app_with_assistant_router(
    *, app: FastAPI, config: GatewayConfig, api_prefix: str, router_tags: List[str]
) -> FastAPI:
    global _orchestrator

    gateway_config = config

    @lru_cache()
    def orchestrator_factory() -> ConversationOrchestrator:
        global _orchestrator
        if _orchestrator is None:
            _orchestrator = ConversationOrchestrator(config=gateway_config)
        return _orchestrator

    # "lifespan_context" is used by FastAPI to manage the lifespan of the application
    # it allows us to manage startup and shutdown events
    # we add orchestrator start and stop to the lifespan

    existing_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        orchestrator = orchestrator_factory()
        await orchestrator.start()

        try:
            if existing_lifespan is not None:
                async with existing_lifespan(app):
                    yield
            else:
                yield
        finally:
            await orchestrator.stop()

    app.router.lifespan_context = lifespan

    app.dependency_overrides[get_orchestrator] = orchestrator_factory
    app.include_router(assistant_router, prefix=api_prefix, tags=router_tags)
    return app
