# start the actual calculator rest API
~/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calculator_api$ 
fastapi dev api.py --port 5000

# start the chat gateway rest API
python assistant_gateway/runner/launcher.py --config /home/mihir/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calculator_chat_gateway/config.json --fastapi

# start the celery workers
python assistant_gateway/runner/launcher.py --config /home/mihir/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calculator_chat_gateway/config.json --celery

# start the representative streamlit app
~/projects/assistant-gateway/src/python/assistant_gateway/examples/calculator_web_app/calc_chat_web_app$ 
streamlit run calculator_chat_app.py



send message:
chat_id b490e977-596b-4b52-92f6-a6e299937b85
{
  "content": "mihir custom log the message: 'printing using low queue'",
  "run_mode": "background",
  "queue_id": "low",
  "input_overrides": {
    "__global__": {
      "backend_url": "http://127.0.0.1:5000"
    }
  }
}