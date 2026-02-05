# chat_event_listener.py

import asyncio
import json
import sys
import websockets


async def listen(chat_id: str):
    url = f"ws://127.0.0.1:8000/api/v1/chats/{chat_id}/events"

    async with websockets.connect(url) as ws:
        print(f"Connected to {url}")

        try:
            async for message in ws:
                event = json.loads(message)
                print("Received event:")
                print("event_type: ", event["event_type"], "task_id: ", event["task_id"], "queue_id: ", event["queue_id"], "status: ", event["status"])

        except websockets.ConnectionClosed:
            print("Connection closed")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python chat_event_listener.py <chat_id>")
        sys.exit(1)

    chat_id = sys.argv[1]
    asyncio.run(listen(chat_id))