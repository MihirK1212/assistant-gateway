"""
Dynamic Celery app loader for the gateway runner.

This module is used by the Celery worker to dynamically load the config
and create the Celery app at import time.

The config path is read from the GATEWAY_CONFIG_PATH environment variable.

Usage:
    # Set the env var before starting celery
    export GATEWAY_CONFIG_PATH="myapp.config:build_gateway_config"
    celery -A assistant_gateway.runner.celery_app:celery_app worker

Note: This module is typically not used directly. Instead, use the
runner CLI which sets up the environment and launches both FastAPI
and Celery worker automatically.
"""

from __future__ import annotations

import asyncio
import os
import sys

# Environment variable name for the config path
CONFIG_PATH_ENV_VAR = "GATEWAY_CONFIG_PATH"

# Get the config path from environment
_config_path = os.environ.get(CONFIG_PATH_ENV_VAR)

if _config_path is None:
    raise RuntimeError(
        f"Environment variable '{CONFIG_PATH_ENV_VAR}' is not set. "
        "This variable should contain the module path to your GatewayConfig, "
        "e.g., 'myapp.config:build_gateway_config'"
    )

# Add working directory to path for module resolution
_cwd = os.environ.get("GATEWAY_WORKING_DIR", os.getcwd())
if _cwd not in sys.path:
    sys.path.insert(0, _cwd)

# Import loader here to avoid circular imports
from assistant_gateway.runner.loader import load_config  # noqa: E402
from assistant_gateway.chat_orchestrator.orchestration.orchestrator import (  # noqa: E402
    ConversationOrchestrator,
)

# Load the config
config = load_config(_config_path)

# Validate that clauq_btm is configured
if config.clauq_btm is None:
    raise RuntimeError(
        "GatewayConfig.clauq_btm is not configured. "
        "Background task processing requires a ClauqBTM instance."
    )

# Create and start the orchestrator to register executors
# This is critical - without this, the Celery task won't have registered executors
orchestrator = ConversationOrchestrator(config=config)

# Run the async start() method
asyncio.run(orchestrator.start())

# Export the celery app
celery_app = config.clauq_btm.celery_app
