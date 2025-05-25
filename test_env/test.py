import asyncio
import websockets
import json

async def subscriber():
    uri = "ws://127.0.0.1:57603/subscribe"  # Connect to Load Balancer, not directly to topic
    subscribe_msg = {
        "subscriber_id": "client-001",  # must be unique per connection
        "topic": "test-topic"
    }

    try:
        async with websockets.connect(uri) as websocket:
            # Step 1: Send initial subscription message
            await websocket.send(json.dumps(subscribe_msg))
            print(f"✅ Subscribed to topic: {subscribe_msg['topic']}\n")

            # Step 2: Listen for messages
            while True:
                msg = await websocket.recv()
                print(f"📩 Received: {msg}")
    except Exception as e:
        print(f"❌ Connection closed or error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(subscriber())
