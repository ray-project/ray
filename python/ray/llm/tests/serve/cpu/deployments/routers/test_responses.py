import sys

import pytest
from pydantic import BaseModel, ConfigDict, Field

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ResponsesRequest,
    ResponsesResponse,
)
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


class _AliasEvent(BaseModel):
    """Minimal event whose serialization alias differs from its field name,
    so a dropped ``by_alias=True`` in the encoder would change the output."""

    model_config = ConfigDict(populate_by_name=True)

    type: str = "response.created"
    seq: int = Field(0, alias="sequence_number")


class _StubServingResponses:
    """Stand-in for vLLM's OpenAIServingResponses: yields typed events."""

    def __init__(self, events):
        self._events = events

    async def create_responses(self, request, raw_request=None):
        async def _gen():
            for event in self._events:
                yield event

        return _gen()


def test_responses_request_has_request_id():
    req = ResponsesRequest(model="m", input="hello")
    assert isinstance(req.request_id, str)
    assert req.request_id


def test_responses_response_importable():
    assert ResponsesResponse.__name__ == "ResponsesResponse"


@pytest.mark.asyncio
async def test_engine_responses_non_streaming(llm_config):
    engine = MockVLLMEngine(llm_config)
    await engine.start()

    request = ResponsesRequest(model=llm_config.model_id, input="hello", stream=False)
    chunks = [c async for c in engine.responses(request)]

    assert len(chunks) == 1
    assert isinstance(chunks[0], ResponsesResponse)
    assert chunks[0].status == "completed"


@pytest.mark.asyncio
async def test_engine_responses_streaming_yields_sse_strings(llm_config):
    engine = MockVLLMEngine(llm_config)
    await engine.start()

    request = ResponsesRequest(model=llm_config.model_id, input="hello", stream=True)
    chunks = [c async for c in engine.responses(request)]

    assert chunks
    assert all(isinstance(c, str) for c in chunks)
    assert all(c.startswith("event: ") and "\ndata: " in c for c in chunks)


@pytest.mark.asyncio
async def test_vllm_engine_responses_streaming_sse_encoding():
    """Exercise the real ``VLLMEngine.responses`` SSE encoder (not the mock).

    Bypasses ``__init__``; the streaming path only touches
    ``_oai_serving_responses``. Pins ``by_alias=True`` and the exact SSE framing.
    """
    engine = VLLMEngine.__new__(VLLMEngine)
    engine._oai_serving_responses = _StubServingResponses(
        [_AliasEvent(type="response.created", seq=7)]
    )

    request = ResponsesRequest(model="m", input="hello", stream=True)
    chunks = [c async for c in engine.responses(request)]

    assert len(chunks) == 1
    chunk = chunks[0]

    expected_json = _AliasEvent(type="response.created", seq=7).model_dump_json(
        indent=None, by_alias=True
    )
    # Exact framing, including the blank-line terminator.
    assert chunk == f"event: response.created\ndata: {expected_json}\n\n"
    # by_alias=True must emit the alias, not the Python field name.
    assert '"sequence_number":7' in chunk
    assert '"seq"' not in chunk


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
