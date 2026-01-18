from __future__ import annotations

import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(CURRENT_DIR)))
sys.path.append(os.path.dirname(CURRENT_DIR))
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
)  # repo root

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from assistant_gateway.rest_api.fast_api_rest_assistant.enrich import (
    enrich_app_with_assistant_router,
)

from assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.base import (
    build_gateway_config,
)


def create_app() -> FastAPI:
    app = FastAPI(title="Calculator Agent Gateway", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    enrich_app_with_assistant_router(
        app=app,
        config=build_gateway_config(),
        api_prefix="/api/v1",
        router_tags=["assistant"],
    )
    return app


app = create_app()
