"""
Celery Worker Configuration for the Calculator Web App.

This module exports the Celery app for running background task workers.

IMPORTANT: Understanding the Distributed Architecture
=====================================================

Running this worker creates a SEPARATE ClauqBTM instance from the one in the
API process (uvicorn). This is expected and correct for Celery's distributed
task processing model.

How it works:
    
    ┌─────────────────────────────┐     ┌─────────────────────────────┐
    │   API Process (uvicorn)     │     │  Worker Process (celery)    │
    ├─────────────────────────────┤     ├─────────────────────────────┤
    │  build_gateway_config()     │     │  build_gateway_config()     │
    │         ↓                   │     │         ↓                   │
    │  ClauqBTM instance #1       │     │  ClauqBTM instance #2       │
    │  - celery_app #1            │     │  - celery_app #2            │
    │  - executor_registry #1     │     │  - executor_registry #2     │
    │         ↓                   │     │         ↓                   │
    │  Enqueues tasks to Redis ───┼────►│  Picks up tasks from Redis  │
    │                             │     │  Executes via registry      │
    └─────────────────────────────┘     └─────────────────────────────┘
                    │                               │
                    └───────────┬───────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │        Redis          │
                    │  (shared state/queue) │
                    └───────────────────────┘

Why this works:
    1. Redis is the shared state - Both instances connect to the same Redis
       broker/backend, so tasks enqueued by API are visible to workers and
       task status updates are stored in Redis (not in-memory).
    
    2. Same executor registration - Both call build_gateway_config() which
       ensures identical executors are registered in both processes.
    
    3. Same Celery task name - The task "clauq.execute_task" is registered
       identically in both apps.
    
    4. This is how Celery is designed - Celery expects separate app instances
       in separate processes communicating via a message broker.

What would break it:
    If the worker didn't call build_gateway_config() or registered different
    executors, it would fail with:
        RuntimeError: Executor 'xxx' not found in registry.

Usage:
    cd src/python
    celery -A assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.celery_worker:celery_app worker --loglevel=info --pool=solo

    Note: On Windows, use --pool=solo or --pool=threads since the default
    prefork pool doesn't work on Windows.
"""

import sys
import os


# Add the package to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.base import (
    build_gateway_config,
)

# Build the config - this registers executors and creates ClauqBTM
config = build_gateway_config()

# Export the celery app for the worker
celery_app = config.clauq_btm.celery_app
