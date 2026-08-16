"""
FastAPI bootstrap module for the gateway runner.

This module is used as the uvicorn entry point when launching FastAPI via
the launcher. It reads the config path from GATEWAY_CONFIG_PATH, calls
parse_config() to build the app, and exposes it as `app`.

Usage (internal - invoked by launcher.py):
    uvicorn assistant_gateway.runner._fastapi_runner:app --host ... --port ...
"""

from __future__ import annotations

import os
import sys

CONFIG_PATH_ENV_VAR = "GATEWAY_CONFIG_PATH"

_config_path = os.environ.get(CONFIG_PATH_ENV_VAR)

if _config_path is None:
    raise RuntimeError(
        f"Environment variable '{CONFIG_PATH_ENV_VAR}' is not set. "
        "This variable should contain the path to your JSON gateway config file."
    )

_cwd = os.environ.get("GATEWAY_WORKING_DIR", os.getcwd())
if _cwd not in sys.path:
    sys.path.insert(0, _cwd)

from assistant_gateway.runner.parse_config import parse_config

_result = parse_config(_config_path)

if _result.app is None:
    raise RuntimeError(
        "No FastAPI app found in the gateway config. "
        "Ensure the JSON config includes a 'rest_api' section."
    )

app = _result.app
