# locust.py

from locust import HttpUser, task, between, events, User
from locust_plugins.users.socketio  import SocketIOUser
import json
import time
import uuid
import websocket
import threading
import random

TOPICS = [f"test-topic-{i}" for i in range(10)]
PUBLISH_ENDPOINT = "http://127.0.0.1:51009/publish"
FLUXNODE_WS_URL = "ws://127.0.0.1:51009/subscribe"  # Update with actual pod IP or NodePort

class PublisherUser(HttpUser):
    wait_time = between(0.001, 0.005)

    weight=0.2

    @task
    def publish(self):
        msg_str = json.dumps({
            "id": str(uuid.uuid4()),
            "content": "test-message",
            "timestamp": time.time()
        })

        TOPIC = random.choice(TOPICS)

        payload = {
            "topic": TOPIC,
            "message": msg_str
        }

        self.client.post(PUBLISH_ENDPOINT, json=payload)


class SubscriberUser(User):
    weight=0.8
    
    def on_start(self):
        self.subscriber_id = f"locust-sub-{uuid.uuid4()}"
        try:
            self.ws = websocket.WebSocket()
            self.ws.connect(FLUXNODE_WS_URL)

            TOPIC = random.choice(TOPICS)

            subscribe_msg = json.dumps({
                "subscriber_id": self.subscriber_id,
                "topic": TOPIC
            })
            self.ws.send(subscribe_msg)

            self.environment.events.request.fire(
                request_type="ws",
                name="subscribe",
                response_time=0,
                response_length=0
            )

            # Start receiving in background thread
            self.recv_thread = threading.Thread(target=self.receive_loop, daemon=True)
            self.recv_thread.start()

        except Exception as e:
            self.environment.events.request.fire(
                request_type="ws",
                name="subscribe_err",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task
    def idle(self):
        time.sleep(1)  # Keeps Locust alive

    def receive_loop(self):
        while True:
            try:
                msg = self.ws.recv()
                recv_time = time.time()
                if msg:
                    data = json.loads(msg)
                    sent_time = data.get("timestamp", recv_time)
                    latency_ms = (recv_time - sent_time) * 1000

                    self.environment.events.request.fire(
                        request_type="ws",
                        name="recv_message",
                        response_time=latency_ms,
                        response_length=len(msg)
                    )
            except Exception as e:
                self.environment.events.request.fire(
                    request_type="ws",
                    name="recv_message_err",
                    response_time=0,
                    response_length=0,
                    exception=e
                )
                break
