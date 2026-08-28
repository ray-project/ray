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
    _enforce_stateless_responses_request,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.ingress.utils import (
    NON_STREAMING_RESPONSE_TYPES,
    _openai_json_wrapper,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine
from ray.llm.tests.serve.mocks.mock_vllm_engine import (
    MockVLLMEngine,
    StoreEchoingMockEngine,
)


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


def test_store_defaults_to_true_but_is_not_explicitly_set():
    """Pin the schema behaviour the guard keys off.

    If a future vLLM marks ``store`` as explicitly set, the guard would start
    rejecting every SDK request again, and this fails first.
    """
    body = ResponsesRequest(model="m", input="hi")
    assert body.store is True
    assert "store" not in body.model_fields_set


def test_only_explicitly_stateful_requests_are_rejected():
    """``store`` defaults to true, so the default alone must not be rejected."""
    for body in (
        ResponsesRequest(model="m", input="hi"),
        ResponsesRequest(model="m", input="hi", store=False),
        ResponsesRequest(model="m", input="hi", store=None),
    ):
        assert _enforce_stateless_responses_request(body) is None

    for body in (
        ResponsesRequest(model="m", input="hi", store=True),
        ResponsesRequest(
            model="m", input="hi", store=False, previous_response_id="r_1"
        ),
        # vLLM rejects background with store=false, so store is left defaulted;
        # the omitted-store body above is accepted, so background is the trigger.
        ResponsesRequest(model="m", input="hi", background=True),
    ):
        err = _enforce_stateless_responses_request(body)
        assert err is not None
        assert err.status_code == 501


def test_accepted_requests_are_pinned_unstored():
    """vLLM stores an unset ``store`` once the operator enables its store."""
    for body in (
        ResponsesRequest(model="m", input="hi"),
        ResponsesRequest(model="m", input="hi", store=False),
        ResponsesRequest(model="m", input="hi", store=None),
    ):
        assert _enforce_stateless_responses_request(body) is None
        assert body.store is False


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
    server = ServerDeployment.bind(
        ingress_llm_config, engine_cls=StoreEchoingMockEngine
    )
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


def test_responses_endpoint_rejects_store_true(base_url, ingress_llm_config):
    """A stored response would be invisible to the replica serving the follow-up."""
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "store": True,
        },
        timeout=60,
    )

    assert response.status_code == 501, response.text
    assert "store" in response.text


def test_responses_endpoint_rejects_previous_response_id(base_url, ingress_llm_config):
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "store": False,
            "previous_response_id": "resp_1",
        },
        timeout=60,
    )

    assert response.status_code == 501, response.text


def test_responses_endpoint_rejects_background(base_url, ingress_llm_config):
    """A background response is retrieved later by id, from another replica.

    vLLM only accepts ``background`` when ``store`` is true, so ``store`` is
    omitted rather than pinned false. The same body without ``background`` is a
    200 (see the omitted-store test), which is what isolates this trigger.
    """
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "background": True,
        },
        timeout=60,
    )

    assert response.status_code == 501, response.text


def test_responses_endpoint_allows_omitted_store(base_url, ingress_llm_config):
    """``store`` defaults to true in the OpenAI schema and the SDK omits unset
    params, so rejecting the default would make the endpoint unusable."""
    response = requests.post(
        f"{base_url}/responses",
        json={"model": ingress_llm_config.model_id, "input": "hello"},
        timeout=60,
    )

    assert response.status_code == 200, response.text
    assert ResponsesResponse.model_validate(response.json()).status == "completed"


def test_responses_endpoint_allows_explicit_store_false(base_url, ingress_llm_config):
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "store": False,
        },
        timeout=60,
    )

    assert response.status_code == 200, response.text
    assert ResponsesResponse.model_validate(response.json()).status == "completed"


def test_responses_endpoint_allows_explicit_store_null(base_url, ingress_llm_config):
    """``null`` is falsy, so it is not an opt-in to server-side state."""
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            "store": None,
        },
        timeout=60,
    )

    assert response.status_code == 200, response.text
    assert ResponsesResponse.model_validate(response.json()).status == "completed"


@pytest.mark.parametrize(
    "store_field",
    [{}, {"store": False}, {"store": None}],
    ids=["omitted", "explicit_false", "explicit_null"],
)
def test_engine_never_receives_store_true(base_url, ingress_llm_config, store_field):
    """Every request that gets past the guard must reach the engine unstored.

    vLLM only forces ``store`` off while ``VLLM_ENABLE_RESPONSES_API_STORE`` is
    unset, so relying on the engine would leak state once an operator flips it.
    """
    response = requests.post(
        f"{base_url}/responses",
        json={
            "model": ingress_llm_config.model_id,
            "input": "hello",
            **store_field,
        },
        timeout=60,
    )

    assert response.status_code == 200, response.text
    parsed = ResponsesResponse.model_validate(response.json())
    assert parsed.metadata == {"observed_store": "False"}


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
