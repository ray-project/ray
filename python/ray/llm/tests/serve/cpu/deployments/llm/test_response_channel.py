import json
import sys

import pytest

from ray.llm._internal.serve.core.server.response_channel import (
    ResponseChannel,
    _to_json_line,
)


class _Model:
    def model_dump_json(self):
        return '{"choices":[{"text":"m"}]}'


class _CapturingClient:
    """Consumes the streamed request body so we can assert what was sent."""

    def __init__(self):
        self.url = None
        self.body = b""

    async def post(self, url, content=None):
        self.url = url
        if hasattr(content, "__aiter__"):
            async for chunk in content:
                self.body += chunk


def test_to_json_line_normalizes_inputs():
    assert _to_json_line(_Model()) == '{"choices":[{"text":"m"}]}'
    assert _to_json_line({"a": 1}) == '{"a": 1}'
    # SSE framing on a raw string is stripped (HAProxy re-adds it).
    assert _to_json_line('data: {"a": 1}\n\n') == '{"a": 1}'


@pytest.mark.asyncio
async def test_channel_streams_newline_delimited_json_in_one_post():
    client = _CapturingClient()
    channel = ResponseChannel("rid-1", "http://haproxy:9000", client)

    await channel.write({"choices": [{"text": "a"}]})
    await channel.write('data: {"choices":[{"text":"b"}]}\n\n')
    await channel.write(_Model())
    await channel.close()

    assert client.url == "http://haproxy:9000/internal/response/rid-1"
    lines = [ln for ln in client.body.decode().split("\n") if ln]
    assert [json.loads(ln) for ln in lines] == [
        {"choices": [{"text": "a"}]},
        {"choices": [{"text": "b"}]},
        {"choices": [{"text": "m"}]},
    ]


@pytest.mark.asyncio
async def test_channel_posts_to_the_threads_internal_port():
    # HAProxy tags the id with the client's thread; the leaf posts back to that
    # thread's internal ingest port (frontend + offset(200) + thread - 1) so the
    # push and the client stream share one per-thread queue. Thread 3, port 8000
    # -> 8202.
    client = _CapturingClient()
    channel = ResponseChannel("t3-1-2-3", "http://127.0.0.1:8000", client)

    await channel.write({"a": 1})
    await channel.close()

    assert client.url == "http://127.0.0.1:8202/internal/response/t3-1-2-3"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
