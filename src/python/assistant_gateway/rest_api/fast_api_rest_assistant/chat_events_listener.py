# chat_event_listener.py

import asyncio
import json
import sys

import websockets


async def listen(queue_id: str = None):
    if queue_id:
        url = f"ws://127.0.0.1:8000/api/v1/events?queue_id={queue_id}"
    else:
        url = f"ws://127.0.0.1:8000/api/v1/events"

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
    queue_id = None
    if len(sys.argv) > 1:
        queue_id = sys.argv[1]
    
    if queue_id:
        print(f"Usage: python chat_event_listener.py [queue_id]")
        print(f"Listening to queue: {queue_id}")
    else:
        print(f"Usage: python chat_event_listener.py [queue_id]")
        print(f"Listening to all events (no queue_id specified)")
    
    asyncio.run(listen(queue_id))