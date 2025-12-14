import json
import requests

SERVER_URL = "http://127.0.0.1:8000/chat"  # For local deployment.
HEADERS = {"Content-Type": "application/json"}


def chat(user_request: str, thread_id: str | None = None) -> None:
    """Send a chat request to the agent and stream the response."""
    payload = {"user_request": user_request}
    if thread_id:
        payload["thread_id"] = thread_id

    with requests.post(SERVER_URL, headers=HEADERS, json=payload, stream=True) as resp:
        resp.raise_for_status()
        # Capture thread_id for multi-turn conversations.
        server_thread = resp.headers.get("X-Thread-Id")
        if not thread_id and server_thread:
            print(f"[thread_id: {server_thread}]")
        # Stream SSE events.
        for line in resp.iter_lines():
            if not line:
                continue
            txt = line.decode("utf-8")
            if txt.startswith("data: "):
                txt = txt[len("data: ") :]
            print(txt, flush=True)


# Test the agent.
chat("What's the weather in Palo Alto?")
