"""
cd ~/projects/assistant-gateway/src/python

source venv/bin/activate


Step 1: start the actual calculator API
mihir@Mihir:~/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calculator_api$ fastapi dev api.py

Step 2: start celery workers
python  assistant_gateway/runner/launcher.py --config /home/mihir/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calculator_chat_gateway/config.json --celery

Step 3: start the fastapi server
python  assistant_gateway/runner/launcher.py --config /home/mihir/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calculator_chat_gateway/config.json --fastapi

Create a chat:
/api/v1/chats
{
  "user_id": "karandik",
  "agent_name": "calculator"
}

Send a sync message:
/api/v1/chats/{chat_id}/messages
{
  "content": "what is the mihir custom transform of 11",
  "run_mode": "sync",
  "input_overrides": {
    "__global__": {
        "backend_url": "http://127.0.0.1:8000"
     }
  }
}

Send a background message:
/api/v1/chats/{chat_id}/messages
{
  "content": "mihir custom log 'hello this is a brand new day'",
  "run_mode": "background",
  "queue_id": "medium",
  "input_overrides": {
    "__global__": {
        "backend_url": "http://127.0.0.1:8000"
     }
  }
}
"""