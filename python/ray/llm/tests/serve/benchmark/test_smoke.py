"""Tests for benchmark smoke mode."""

import asyncio
import json
import socket
import threading
import types

import aiohttp
import aiohttp.web
import pytest

from ray.llm._internal.serve.benchmark.multiturn_bench import (
    TurnResult,
    send_chat_completion,
    run_smoke,
)


def _find_free_port() -> int:
    """Find and return a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_sse_chunk(data: dict) -> bytes:
    """Encode a dict as an SSE data line."""
    return f"data: {json.dumps(data)}\n\n".encode()


async def _mock_chat_handler(
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

    # Send several content chunks
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
        await resp.write(_make_sse_chunk(chunk))

    # Send finish chunk (no content)
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
    await resp.write(_make_sse_chunk(finish_chunk))

    # Send usage chunk
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
    await resp.write(_make_sse_chunk(usage_chunk))

    # Send [DONE]
    await resp.write(b"data: [DONE]\n\n")

    return resp


def _run_server(loop: asyncio.AbstractEventLoop, runner, port: int) -> None:
    """Run the aiohttp server in a background thread."""
    asyncio.set_event_loop(loop)
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", port)
    loop.run_until_complete(site.start())
    loop.run_forever()


@pytest.fixture()
def mock_server():
    """Start a mock SSE server in a background thread and yield its base URL."""
    port = _find_free_port()
    app = aiohttp.web.Application()
    app.router.add_post("/v1/chat/completions", _mock_chat_handler)

    loop = asyncio.new_event_loop()
    runner = aiohttp.web.AppRunner(app)
    loop.run_until_complete(runner.setup())

    thread = threading.Thread(target=_run_server, args=(loop, runner, port), daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.run_until_complete(runner.cleanup())
    loop.close()


@pytest.mark.asyncio
async def test_send_chat_completion(mock_server: str) -> None:
    """send_chat_completion should parse SSE stream and return correct TurnResult."""
    messages = [{"role": "user", "content": "hello"}]
    async with aiohttp.ClientSession() as session:
        result = await send_chat_completion(
            session=session,
            base_url=mock_server,
            model="test-model",
            messages=messages,
            chunk_size=2,
        )

    assert isinstance(result, TurnResult)
    assert result.generated_text == "this is a test"
    assert result.input_tokens == 10
    assert result.output_tokens == 4
    assert result.ttft_ms > 0
    assert result.latency_ms > 0
    assert result.fc_ms > 0
    assert isinstance(result.tpot_ms, float)


def test_run_smoke(mock_server: str) -> None:
    """run_smoke should return 0 and print JSON metrics."""
    args = types.SimpleNamespace(
        base_url=mock_server,
        model="test-model",
        chunk_size=2,
    )
    exit_code = run_smoke(args)
    assert exit_code == 0


def test_run_smoke_connection_error() -> None:
    """run_smoke should return 1 when the server is unreachable."""
    port = _find_free_port()
    args = types.SimpleNamespace(
        base_url=f"http://127.0.0.1:{port}",
        model="test-model",
        chunk_size=16,
    )
    exit_code = run_smoke(args)
    assert exit_code == 1
