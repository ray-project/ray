"""Response identity only; stream parsing is tested in the native C suite."""

import json
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import Depends, FastAPI
from starlette.responses import StreamingResponse

from ray.llm._internal.serve.observability.metrics import llm_metrics


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,content_type",
    [(200, "text/event-stream"), (200, "application/json"), (404, "text/event-stream")],
)
async def test_stream_headers_preserve_response(monkeypatch, status, content_type):
    tags = {
        "model_name": "model\u2603\n",
        "ReplicaId": "replica",
        "engine_worker_id": "worker",
        "engine": "0",
    }
    identity = MagicMock(return_value=tags)
    monkeypatch.setattr(llm_metrics, "get_llm_metric_tags", identity)
    app = FastAPI()
    active = False
    completed = 0
    chunks = [b"data: first\n\n", b"data: second\n\n", b"data: [DONE]\n\n"]

    async def dependency():
        nonlocal active, completed
        active = True
        try:
            yield
        finally:
            active = False
            completed += 1

    async def handler(_=Depends(dependency)):
        async def body():
            for chunk in chunks:
                assert active, "FastAPI's request exit stack must outlive the body"
                yield chunk

        return StreamingResponse(body(), status_code=status, media_type=content_type)

    for path in ["/v1/completions", "/v1/chat/completions", "/other"]:
        app.post(path)(handler)
    llm_metrics.add_llm_stream_metrics_headers(app, model_name="model")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path in ["/v1/completions", "/v1/chat/completions", "/other"]:
            response = await client.post(path)
            assert response.status_code == status
            assert response.content == b"".join(chunks)
            # Identity is only expected for successful text/event-stream
            # responses (stream=True); JSON/nonstreaming and errors omit it.
            header = response.headers.get(llm_metrics.LLM_METRICS_HEADER)
            expected = (
                path != "/other"
                and status == 200
                and content_type == "text/event-stream"
            )
            assert bool(header) == expected
            if header:
                assert header.isascii() and "\n" not in header
                assert json.loads(header) == tags
    assert not active and completed == 3
    identity.assert_called_once_with("model", "0")
