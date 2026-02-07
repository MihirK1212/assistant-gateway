# Calculator Web App Example

This example demonstrates a complete chat-based calculator using the Assistant Gateway.

## TLDR Quick Start Guide

Follow these steps to run the complete calculator web app example:

### Prerequisites: Configure Environment Variables

Before starting the services, set up your environment variables. If using Claude models, create a `.env` file or export the following variables:

```bash
export ANTHROPIC_API_KEY=your_api_key_here
export CLAUDE_MODEL=claude-3-5-sonnet-20241022  # Default model to use
```

Or create a `.env` file in the project directory:

```env
ANTHROPIC_API_KEY=your_api_key_here
CLAUDE_MODEL=claude-3-5-sonnet-20241022
```

### 1. Start the Calculator Backend API

Navigate to the calculator API directory and start the backend service:

```bash
cd src/python/assistant_gateway/examples/calculator_web_app/calculator_api
fastapi dev api.py --port 5000
```

### 2. Start the Celery Worker

In a separate terminal, start the Celery worker for background task processing:

```bash
python -m assistant_gateway.runner \
    --app assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.api:app \
    --config assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.gateway_config:config \
    --celery-only
```

### 3. Start the Gateway API

In another terminal, start the FastAPI gateway server:

```bash
python -m assistant_gateway.runner \
    --app assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.api:app \
    --config assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.gateway_config:config \
    --fastapi-only
```

### 4. Launch the Streamlit Frontend

Finally, start the Streamlit web interface:

```bash
streamlit run calculator_chat_app.py
```

**All services should now be running:**
- Calculator API: `http://localhost:5000`
- Gateway API: `http://localhost:8000`
- Streamlit App: `http://localhost:8501`
  

## Detailed Quick Start (Recommended)

The simplest way to run this example is using the Gateway Runner:

```bash
cd src/python

# Run both FastAPI and Celery worker with a single command
python -m assistant_gateway.runner \
    --config assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.base:build_gateway_config \
    --app assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.api:app
```

This single command:
1. Validates your config
2. Starts the FastAPI server on `http://127.0.0.1:8000`
3. Starts the Celery worker for background task processing

### Runner Options

```bash
# Run only FastAPI (for development without background tasks)
python -m assistant_gateway.runner \
    --config ...:build_gateway_config \
    --app ...:app \
    --fastapi-only

# Run only Celery worker (useful when scaling workers separately)
python -m assistant_gateway.runner \
    --config ...:build_gateway_config \
    --app ...:app \
    --celery-only

# Custom settings
python -m assistant_gateway.runner \
    --config ...:build_gateway_config \
    --app ...:app \
    --port 9000 \
    --celery-pool threads \
    --celery-concurrency 4
```

## Manual Setup (Alternative)

If you prefer to run the services separately:

### 1. Start Redis

```bash
docker run -d -p 6379:6379 redis:alpine
```

### 2. Start the FastAPI Server

```bash
cd src/python
uvicorn assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.api:app --reload
```

### 3. Start the Celery Worker

```bash
cd src/python
celery -A assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.celery_worker:celery_app worker -E

# On Windows, add --pool=solo
celery -A assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.celery_worker:celery_app worker -E --pool=solo
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Your Application                            │
├─────────────────────────────────────────────────────────────────┤
│  config/base.py          │  api.py                              │
│  - build_gateway_config()│  - FastAPI app                       │
│  - Agent configuration   │  - Uses enrich_app_with_assistant_   │
│  - ClauqBTM setup       │    router() for chat endpoints       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Gateway Runner                                 │
│  python -m assistant_gateway.runner                              │
│  - Launches FastAPI + Celery from single command                │
│  - Handles process lifecycle                                     │
│  - Auto-configures based on your config module                  │
└─────────────────────────────────────────────────────────────────┘
                    │                    │
                    ▼                    ▼
        ┌───────────────────┐  ┌────────────────────┐
        │   FastAPI Server  │  │   Celery Worker    │
        │   (uvicorn)       │  │   (background)     │
        └───────────────────┘  └────────────────────┘
                    │                    │
                    └────────┬───────────┘
                             ▼
                    ┌────────────────┐
                    │     Redis      │
                    │  (task queue)  │
                    └────────────────┘
```

## Project Structure

```
calculator_chat_gateway/
├── api.py              # FastAPI app definition
├── celery_worker.py    # (Optional) Manual Celery setup
└── config/
    ├── base.py         # GatewayConfig builder
    └── agent.py        # Agent and tool definitions
```

## Creating Your Own Gateway

1. **Create a config module** that exports `build_gateway_config()`:

```python
# myapp/config.py
from assistant_gateway.chat_orchestrator.core.config import GatewayConfig, AgentConfig
from assistant_gateway.clauq_btm import ClauqBTM

def build_gateway_config() -> GatewayConfig:
    return GatewayConfig(
        agent_configs={
            "my_agent": AgentConfig(
                name="my_agent",
                builder=build_my_agent,
            ),
        },
        clauq_btm=ClauqBTM(redis_url="redis://localhost:6379/0"),
    )
```

2. **Create a FastAPI app** that uses `enrich_app_with_assistant_router`:

```python
# myapp/api.py
from fastapi import FastAPI
from assistant_gateway.rest_api.fast_api_rest_assistant.enrich import (
    enrich_app_with_assistant_router,
)
from myapp.config import build_gateway_config

app = FastAPI()
enrich_app_with_assistant_router(
    app=app,
    config=build_gateway_config(),
    api_prefix="/api/v1",
    router_tags=["assistant"],
)
```

3. **Run with the Gateway Runner**:

```bash
python -m assistant_gateway.runner \
    --config myapp.config:build_gateway_config \
    --app myapp.api:app
```

That's it! No need to write a separate Celery worker file.
