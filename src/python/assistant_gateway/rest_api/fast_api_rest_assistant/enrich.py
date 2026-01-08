from __future__ import annotations

from functools import lru_cache
from typing import List

from fastapi import FastAPI

from assistant_gateway.rest_api.fast_api_rest_assistant.router import (
    get_orchestrator,
    router as assistant_router,
)
from assistant_gateway.chat_orchestrator.core.config import GatewayConfig
from assistant_gateway.chat_orchestrator.orchestration import ConversationOrchestrator


def enrich_app_with_assistant_router(
    *, app: FastAPI, config: GatewayConfig, api_prefix: str, router_tags: List[str]
) -> FastAPI:
    """
    Enrich a FastAPI app with the assistant router.
    """

    gateway_config = config

    @lru_cache()
    def orchestrator_factory() -> ConversationOrchestrator:
        return ConversationOrchestrator(config=gateway_config)

    app.dependency_overrides[get_orchestrator] = orchestrator_factory
    app.include_router(assistant_router, prefix=api_prefix, tags=router_tags)
    return app
