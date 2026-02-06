"""
Gateway Runner - Simplified launcher for FastAPI + Celery worker.

This module provides a CLI and programmatic interface to launch both
the FastAPI application and Celery worker from a single command.

Usage:
    python -m assistant_gateway.runner \\
        --config myapp.config:build_gateway_config \\
        --app myapp.api:app

Or with module paths:
    python -m assistant_gateway.runner \\
        --config assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.base:build_gateway_config \\
        --app assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.api:app
"""

from assistant_gateway.runner.loader import load_config, load_attribute
from assistant_gateway.runner.cli import main as run_cli

__all__ = ["load_config", "load_attribute", "run_cli"]
