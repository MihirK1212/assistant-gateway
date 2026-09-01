"""
Used by the launcher script as a bootstrap module for the Celery app.
When this module is imported, the Celery app is created and started.
"""

from __future__ import annotations

import asyncio
import os
import sys

from assistant_gateway.chat_orchestrator.orchestration.orchestrator import (
    ConversationOrchestrator,
)
from assistant_gateway.runner.config import parse_config

GATEWAY_WORKING_DIR_ENV_VAR = "GATEWAY_WORKING_DIR"
cwd = os.environ.get(GATEWAY_WORKING_DIR_ENV_VAR)
if cwd is None:
    raise RuntimeError(
        f"Environment variable '{GATEWAY_WORKING_DIR_ENV_VAR}' is not set. "
        "This variable should contain the working directory of your gateway project."
    )
if cwd not in sys.path:
    sys.path.insert(0, cwd)

CONFIG_PATH_ENV_VAR = "GATEWAY_CONFIG_PATH"
config_path = os.environ.get(CONFIG_PATH_ENV_VAR)
if config_path is None:
    raise RuntimeError(
        f"Environment variable '{CONFIG_PATH_ENV_VAR}' is not set. "
        "This variable should contain the path to your JSON gateway config file."
    )

parsed_config = parse_config(config_path)
gateway_config = parsed_config.gateway_config

if gateway_config.clauq_btm is None:
    raise RuntimeError(
        "GatewayConfig.clauq_btm is not configured. Background task processing requires a ClauqBTM instance."
    )

# create and start the orchestrator to register executors
# without this, the Celery task won't have registered executors
orchestrator = ConversationOrchestrator(config=gateway_config)

asyncio.run(orchestrator.start())

celery_app = gateway_config.clauq_btm.celery_app
