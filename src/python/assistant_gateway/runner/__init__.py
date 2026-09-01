"""
Gateway Runner - Launcher for FastAPI + Celery worker.

Launch both the FastAPI application and Celery worker from a single JSON config file:

    python src/python/assistant_gateway/runner/launcher.py --config myapp.json
"""

from assistant_gateway.runner.config import ParseResult, parse_config
from assistant_gateway.runner.loader import load_attribute

__all__ = ["load_attribute", "parse_config", "ParseResult"]
