"""Shared fixtures for benchmark tests."""

import asyncio
import json
import socket
import threading
from collections.abc import Generator
from typing import Any

import aiohttp.web
import pytest


@pytest.fixture()
def free_port() -> int:
    """Find and return a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_sse_chunk(data: dict[str, Any]) -> bytes:
    """Encode a dict as an SSE data line."""
    return f"data: {json.dumps(data)}\n\n".encode()


async def mock_chat_handler(
    request: aiohttp.web.Request,
) -> aiohttp.web.StreamResponse:
    """Mock OpenAI streaming chat completions endpoint."""
    body = await request.json()
    assert body["model"] is not None
    assert body["stream"] is True

    resp = aiohttp.web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/event-stream"},
    )
    await resp.prepare(request)

    words = ["this", " is", " a", " test"]
    for word in words:
        chunk = {
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": word},
                    "finish_reason": None,
                }
            ],
        }
        await resp.write(make_sse_chunk(chunk))

    finish_chunk = {
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "choices": [
            {
                "index": 0,
                "delta": {},
                "finish_reason": "stop",
            }
        ],
    }
    await resp.write(make_sse_chunk(finish_chunk))

    usage_chunk = {
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "choices": [],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 14,
        },
    }
    await resp.write(make_sse_chunk(usage_chunk))

    await resp.write(b"data: [DONE]\n\n")

    return resp


def _run_server(
    loop: asyncio.AbstractEventLoop,
    runner: aiohttp.web.AppRunner,
    port: int,
    started: threading.Event,
) -> None:
    asyncio.set_event_loop(loop)
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", port)
    loop.run_until_complete(site.start())
    started.set()
    loop.run_forever()


@pytest.fixture()
def mock_server(free_port: int) -> Generator[str, None, None]:
    """Start a mock SSE server in a background thread and yield its base URL."""
    port = free_port
    app = aiohttp.web.Application()
    app.router.add_post("/v1/chat/completions", mock_chat_handler)

    loop = asyncio.new_event_loop()
    runner = aiohttp.web.AppRunner(app)
    loop.run_until_complete(runner.setup())

    started = threading.Event()
    thread = threading.Thread(
        target=_run_server, args=(loop, runner, port, started), daemon=True
    )
    thread.start()
    assert started.wait(timeout=5), "Mock server failed to start"

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.run_until_complete(runner.cleanup())
    loop.close()
