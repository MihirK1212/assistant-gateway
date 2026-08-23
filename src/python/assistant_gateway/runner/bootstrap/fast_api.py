"""
Used by the launcher script as a bootstrap module for the FastAPI app.
When this module is imported, the FastAPI app is created.
"""

from __future__ import annotations

import os
import sys

GATEWAY_WORKING_DIR_ENV_VAR = "GATEWAY_WORKING_DIR"
CONFIG_PATH_ENV_VAR = "GATEWAY_CONFIG_PATH"

cwd = os.environ.get(GATEWAY_WORKING_DIR_ENV_VAR)
if cwd is None:
    raise RuntimeError(
        f"Environment variable '{GATEWAY_WORKING_DIR_ENV_VAR}' is not set. "
        "This variable should contain the working directory of your gateway project."
    )
if cwd not in sys.path:
    sys.path.insert(0, cwd)

config_path = os.environ.get(CONFIG_PATH_ENV_VAR)
if config_path is None:
    raise RuntimeError(
        f"Environment variable '{CONFIG_PATH_ENV_VAR}' is not set. "
        "This variable should contain the path to your JSON gateway config file."
    )

from assistant_gateway.runner.config import parse_config

parsed_config = parse_config(config_path)

if parsed_config.app is None:
    raise RuntimeError(
        "No FastAPI app found in the gateway config. "
        "Ensure the JSON config includes a 'rest_api' section."
    )

app = parsed_config.app
