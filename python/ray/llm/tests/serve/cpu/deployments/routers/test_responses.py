import sys

import pytest
import requests
from pydantic import BaseModel, ConfigDict, Field

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ResponsesRequest,
    ResponsesResponse,
    to_model_metadata,
)
from ray.llm._internal.serve.core.ingress.ingress import (
    DEFAULT_ENDPOINTS,
    CallMethod,
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.ingress.utils import (
    NON_STREAMING_RESPONSE_TYPES,
    _openai_json_wrapper,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
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


def test_responses_route_is_registered():
    assert "responses" in DEFAULT_ENDPOINTS
    assert CallMethod.RESPONSES.value == "responses"


def test_responses_response_counts_as_non_streaming():
    assert ResponsesResponse in NON_STREAMING_RESPONSE_TYPES


@pytest.mark.asyncio
async def test_openai_json_wrapper_can_skip_done_sentinel():
    async def gen():
        yield "event: response.completed\ndata: {}\n\n"

    out = [c async for c in _openai_json_wrapper(gen(), append_done=False)]
    assert out == ["event: response.completed\ndata: {}\n\n"]

    out_with_done = [c async for c in _openai_json_wrapper(gen())]
    assert out_with_done[-1] == "data: [DONE]\n\n"


@pytest.fixture(name="ingress_llm_config")
def create_ingress_llm_config():
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_model_id",
        ),
    )


@pytest.fixture(name="base_url")
def create_ingress(ingress_llm_config: LLMConfig):
    """Serve the real ingress in front of an LLMServer backed by the mock engine."""
    ServerDeployment = serve.deployment(LLMServer)

    ingress_options = OpenAiIngress.get_deployment_options(
        llm_configs=[ingress_llm_config]
    )
    ingress_cls = make_fastapi_ingress(OpenAiIngress)
    RouterDeployment = serve.deployment(ingress_cls, **ingress_options)
    server = ServerDeployment.bind(ingress_llm_config, engine_cls=MockVLLMEngine)
    router = RouterDeployment.bind(
        llm_deployments={ingress_llm_config.model_id: server},
        model_cards={
            ingress_llm_config.model_id: to_model_metadata(
                ingress_llm_config.model_id, ingress_llm_config
            )
        },
    )
    serve.run(router)

    yield "http://localhost:8000/v1"

    serve.shutdown()


def test_responses_endpoint_non_streaming(base_url, ingress_llm_config):
    """POST /v1/responses returns a single Responses object."""
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "stream": False,
        },
        timeout=60,
    )

    assert response.status_code == 200, response.text
    parsed = ResponsesResponse.model_validate(response.json())
    assert parsed.status == "completed"


def test_responses_endpoint_streaming(base_url, ingress_llm_config):
    """Streaming yields SSE events and terminates on response.completed.

    The Responses API has no ``data: [DONE]`` sentinel, so the endpoint must
    pass append_done=False through to the SSE wrapper.
    """
    with requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "stream": True,
        },
        stream=True,
        timeout=60,
    ) as response:
        assert response.status_code == 200, response.text
        assert response.headers["content-type"].startswith("text/event-stream")
        body = response.text

    frames = [frame for frame in body.split("\n\n") if frame.strip()]
    assert frames
    assert frames[0].startswith("event: response.created")
    assert frames[-1].startswith("event: response.completed")
    assert "[DONE]" not in body


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
