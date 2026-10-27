"""KV-aware P/D: application graphs, the router flow, the ZMQ prefill call, and
per-stage scoring."""

import asyncio
import base64
import contextlib
import json
import os
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.datastructures import Headers

from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorInfo,
    ErrorResponse,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DECODE_SELECTION_SVC_ENV,
    DEFAULT_KV_EVENTS_PORT_BASE,
    DEFAULT_KV_TOKEN_PORT_BASE,
    KV_EVENTS_PORT_BASE_KEY,
    KV_LIFECYCLE_OWNER_HEADER,
    KV_TOKEN_KEY_HEADER,
    KV_TOKEN_PORT_BASE_KEY,
    KV_TRANSFER_PARAMS_HEADER,
    PD_BACKEND_APP_NAME,
    PD_PREFILL_ENDPOINT_KEY,
    PREFILL_SELECTION_SVC_ENV,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    _MODEL_NAME,
    _ROUTING_GROUP,
    KVTokenTracker,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    get_request_lifecycle_owner,
)
from ray.llm._internal.serve.routing_policies.kv_aware.network_utils import (
    ZmqRpcError,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.pd_prefill import (
    PrefillClient,
    PrefillRequest,
    PrefillService,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.pd_router import (
    LLMPDRouter,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDPrefillServer,
)
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)

BLOCK_SIZE = 16
PROMPT = list(range(1, 257))  # 16 blocks
A, B = get_worker_id("a"), get_worker_id("b")


def _kv_config(model_id="model", env_vars=None):
    return LLMConfig(
        model_loading_config={"model_id": model_id},
        deployment_config={
            "request_router_config": {
                "request_router_class": "ray.serve.llm.request_router.KVAwareRouter"
            },
        },
        engine_kwargs={
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            }
        },
        runtime_env={"env_vars": env_vars} if env_vars else None,
    )


def _router_env_vars(router_app):
    return router_app._bound_deployment.ray_actor_options["runtime_env"]["env_vars"]


def test_build_pd_apps(monkeypatch):
    from ray.llm._internal.serve.serving_patterns.prefill_decode import builder
    from ray.serve._private import constants

    monkeypatch.setattr(builder, "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING", True)
    monkeypatch.setattr(constants, "RAY_SERVE_ENABLE_HA_PROXY", True)
    args = {"prefill_config": _kv_config(), "decode_config": _kv_config()}

    backend = builder.build_pd_openai_app(args)
    assert backend._bound_deployment.name == "Decode:model"
    assert backend._ingress_request_router is None
    assert backend._router_application_target
    # D's KV ports must not collide with a colocated P's defaults.
    decode_config = backend._bound_deployment.init_kwargs["llm_config"]
    ports = decode_config.experimental_configs
    assert ports[KV_EVENTS_PORT_BASE_KEY] != DEFAULT_KV_EVENTS_PORT_BASE
    assert ports[KV_TOKEN_PORT_BASE_KEY] != DEFAULT_KV_TOKEN_PORT_BASE

    router = builder.build_pd_router_app(args)
    assert router._bound_deployment.name == "LLMRouter"
    kwargs = router._bound_deployment.init_kwargs
    assert kwargs["prefill_deployment"] == "Prefill:model"
    assert kwargs["decode_deployment"] == "Decode:model"


@pytest.mark.parametrize(
    "inherited_cache_mib, decode_cache_mib, prefill_cache_mib, expected_cache_mib",
    [
        (None, None, None, "1024"),
        ("32", None, None, "32"),
        ("32", "64", None, "64"),
        ("32", "64", "0", "0"),
    ],
    ids=["default", "inherited", "decode", "prefill"],
)
def test_router_runtime_env(
    monkeypatch,
    inherited_cache_mib,
    decode_cache_mib,
    prefill_cache_mib,
    expected_cache_mib,
):
    """The router tokenizes for both stages, so it gets both configs' env vars,
    prefill's taking precedence, and a tokenizer cache budget."""
    from ray.llm._internal.serve.serving_patterns.prefill_decode import builder

    if inherited_cache_mib is None:
        monkeypatch.delenv("FASTOKENS_SEGMENT_CACHE", raising=False)
    else:
        monkeypatch.setenv("FASTOKENS_SEGMENT_CACHE", inherited_cache_mib)
    decode_env = {"VLLM_USE_FASTOKENS": "1", "SHARED": "decode"}
    prefill_env = {"PREFILL_ONLY": "1", "SHARED": "prefill"}
    if decode_cache_mib is not None:
        decode_env["FASTOKENS_SEGMENT_CACHE"] = decode_cache_mib
    if prefill_cache_mib is not None:
        prefill_env["FASTOKENS_SEGMENT_CACHE"] = prefill_cache_mib
    args = {
        "prefill_config": _kv_config(env_vars=prefill_env),
        "decode_config": _kv_config(env_vars=decode_env),
    }

    env_vars = _router_env_vars(builder.build_pd_router_app(args))

    assert env_vars["FASTOKENS_SEGMENT_CACHE"] == expected_cache_mib
    assert env_vars["VLLM_USE_FASTOKENS"] == "1"
    assert env_vars["PREFILL_ONLY"] == "1"
    assert env_vars["SHARED"] == "prefill"
    assert env_vars["RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP"] == "0"


@pytest.mark.asyncio
async def test_router_tokenizer_reuses_session_prefix(monkeypatch):
    """Later turns of a session reuse the prefix the router tokenized for an
    earlier turn, with prefill's cache budget, and route on the same tokens an
    uncached tokenizer produces."""
    fastokens = pytest.importorskip("fastokens")
    from transformers import AutoTokenizer
    from vllm.tokenizers.registry import cached_get_tokenizer

    from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (
        Tokenizer,
    )
    from ray.llm._internal.serve.serving_patterns.prefill_decode import builder

    model = os.environ.get("RAY_TOKEN_CACHE_TEST_MODEL", "Qwen/Qwen3-0.6B")
    args = {
        "prefill_config": _kv_config(model, {"FASTOKENS_SEGMENT_CACHE": "1"}),
        "decode_config": _kv_config(
            model, {"VLLM_USE_FASTOKENS": "1", "FASTOKENS_SEGMENT_CACHE": "64"}
        ),
    }
    # The router replica runs with its deployment's runtime env.
    for name, value in _router_env_vars(builder.build_pd_router_app(args)).items():
        monkeypatch.setenv(name, value)
    fastokens.unpatch_transformers()
    reference = AutoTokenizer.from_pretrained(model)
    # vLLM reuses a tokenizer per model, which an earlier test may have loaded
    # without fastokens.
    cached_get_tokenizer.cache_clear()
    try:
        router = _pd_router({"remote_engine_id": "p-1"})
        # LLMRouter builds its tokenizer from the decode config.
        router._tokenizer = Tokenizer(args["decode_config"])
        backend = router._tokenizer._renderer.renderer.tokenizer.backend_tokenizer
        if not hasattr(backend, "segment_cache_stats"):
            pytest.skip("fastokens does not provide the native segment cache API")
        assert backend.segment_cache_stats()["max_bytes"] == 1 << 20

        context = "shared reference material " * 50
        first_turn = [
            {"role": "system", "content": context},
            {"role": "user", "content": "First question."},
        ]
        second_turn = first_turn + [
            {"role": "assistant", "content": "First answer."},
            {"role": "user", "content": "Follow-up question."},
        ]

        async def route(messages):
            await router.route_request(
                _Request(
                    {"model": model, "messages": messages},
                    "/v1/chat/completions",
                    headers={"x-session-id": "session-1"},
                )
            )
            # The router tokenizes once, and P and D run on those tokens.
            expected = reference.apply_chat_template(
                messages, tokenize=True, add_generation_prompt=True, return_dict=True
            )["input_ids"]
            assert router._pick_replica.call_args.args[2] == list(expected)

        await route(first_turn)
        before = backend.segment_cache_stats()
        await route(second_turn)
        after = backend.segment_cache_stats()
        assert after["hit_bytes"] - before["hit_bytes"] >= len(context)
    finally:
        fastokens.unpatch_transformers()
        cached_get_tokenizer.cache_clear()


# ---- Router flow -------------------------------------------------------------

P_ENDPOINT = "tcp://p-1:7000"
P_OWNER = "tcp://router:1"
D_OWNER = "tcp://router:2"
REQUEST_ID = "req-1"
CHAT = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}


class _Request:
    """The /internal/route request HAProxy sends to the router."""

    def __init__(self, body, path, headers=None, truncated=False):
        self._body = json.dumps(body).encode()
        headers = {SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER: path, **(headers or {})}
        if truncated:
            headers["x-body-truncated"] = "1"
        self.headers = Headers(headers)

    async def body(self):
        return self._body


def _pd_router(prefill_result=None, prefill_error=None, p_metadata=None):
    """An LLMPDRouter whose P selection books ``REQUEST_ID`` on a replica
    serving ``P_ENDPOINT``, whose P call returns ``prefill_result`` (or raises
    ``prefill_error``), and whose D pick returns replica "decode-1"."""
    router = LLMPDRouter.__new__(LLMPDRouter)
    router._base_model_id = None
    router._handle = MagicMock()
    router._prefill = MagicMock()
    router._tokenizer = MagicMock(tokenize=AsyncMock(return_value=[1, 2, 3]))
    router._push_prompt_tokens = MagicMock(return_value="token-key")
    router._choose_replica = AsyncMock(
        return_value=SimpleNamespace(
            replica_id="prefill-1",
            replica_metadata=(
                {PD_PREFILL_ENDPOINT_KEY: P_ENDPOINT}
                if p_metadata is None
                else p_metadata
            ),
            _request_metadata=SimpleNamespace(request_id=REQUEST_ID),
        )
    )
    router._pick_replica = AsyncMock(return_value=("h", 1, "decode-1", "tcp://t"))
    router._prefill_client = MagicMock(
        prefill=AsyncMock(return_value=prefill_result, side_effect=prefill_error)
    )
    router._prefill_tracker = MagicMock(lifecycle_endpoint=P_OWNER)
    router._kv_token_tracker = MagicMock(lifecycle_endpoint=D_OWNER)
    return router


@pytest.mark.asyncio
async def test_route_runs_prefill_then_picks_decode():
    router = _pd_router({"remote_engine_id": "p-1"})

    decision = await router.route_request(_Request(CHAT, "/v1/chat/completions"))

    # P is selected with the prompt token IDs, which book its prefill load.
    _, request, token_ids = router._choose_replica.call_args.args
    assert isinstance(request, ChatCompletionRequest)
    assert token_ids == [1, 2, 3]
    # Then prefill runs on that replica over ZMQ, under the booking's id, and
    # P reports its lifecycle to the prefill tracker.
    endpoint, prefill_request = router._prefill_client.prefill.call_args.args
    assert endpoint == P_ENDPOINT
    assert prefill_request.request_id == REQUEST_ID
    assert prefill_request.lifecycle_owner == P_OWNER
    assert prefill_request.token_ids == [1, 2, 3]
    assert json.loads(prefill_request.body) == CHAT
    assert prefill_request.path == "/v1/chat/completions"
    # D is selected after prefill, and HAProxy stamps the decision's headers.
    assert router._pick_replica.call_args.args[2] == [1, 2, 3]
    headers = decision.pop(RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD)
    assert decision == {"application": PD_BACKEND_APP_NAME, "replica_id": "decode-1"}
    assert json.loads(base64.b64decode(headers[KV_TRANSFER_PARAMS_HEADER])) == {
        "remote_engine_id": "p-1"
    }
    assert headers[KV_TOKEN_KEY_HEADER] == "token-key"
    assert headers[KV_LIFECYCLE_OWNER_HEADER] == D_OWNER
    router._prefill_tracker.release_request.assert_not_called()


@pytest.mark.asyncio
async def test_session_binds_each_stage():
    """A session binds to a P replica and to a D replica separately: both
    selections carry the session id."""
    router = _pd_router({})

    await router.route_request(
        _Request(CHAT, "/v1/chat/completions", headers={"x-session-id": "s-1"})
    )

    router._prefill.options.assert_called_once_with(session_id="s-1")
    router._handle.options.assert_called_once_with(session_id="s-1")
    assert router._choose_replica.call_args.args[0] is router._prefill.options()
    assert router._pick_replica.call_args.args[0] is router._handle.options()


@pytest.mark.asyncio
async def test_route_returns_prefill_error():
    error = ErrorResponse(error=ErrorInfo(message="too long", type="Bad", code=400))
    router = _pd_router(error)

    response = await router.route_request(_Request(CHAT, "/v1/chat/completions"))

    assert response.status_code == 400
    assert json.loads(response.body)["error"]["message"] == "too long"
    router._pick_replica.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error, raised",
    [
        (ConnectionError("P died"), RuntimeError),
        (asyncio.TimeoutError(), RuntimeError),
        (ZmqRpcError("boom"), ZmqRpcError),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_failed_prefill_frees_its_booking(error, raised):
    """P may never report a prefill that failed, so the router frees P's
    booking itself. A lost P replica is a retryable routing failure (503)."""
    router = _pd_router(prefill_error=error)

    with pytest.raises(raised):
        await router.route_request(_Request(CHAT, "/v1/chat/completions"))

    router._prefill_tracker.release_request.assert_called_once_with(REQUEST_ID)
    router._pick_replica.assert_not_called()


@pytest.mark.asyncio
async def test_prefill_replica_without_endpoint_fails_the_route():
    router = _pd_router(p_metadata={})

    with pytest.raises(RuntimeError, match="does not serve P/D prefill calls"):
        await router.route_request(_Request(CHAT, "/v1/chat/completions"))

    router._prefill_client.prefill.assert_not_called()
    router._prefill_tracker.release_request.assert_called_once_with(REQUEST_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body, path",
    [(CHAT, "/v1/models"), ({"model": "m", "messages": "bad"}, "/v1/chat/completions")],
)
async def test_route_sends_other_requests_to_decode(body, path):
    router = _pd_router({})

    decision = await router.route_request(_Request(body, path))

    router._choose_replica.assert_not_called()
    router._prefill_client.prefill.assert_not_called()
    assert decision["replica_id"] == "decode-1"
    assert (
        KV_TRANSFER_PARAMS_HEADER
        not in decision[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD]
    )


@pytest.mark.asyncio
async def test_route_rejects_truncated_body():
    router = _pd_router({})

    with pytest.raises(HTTPException) as exc_info:
        await router.route_request(
            _Request(CHAT, "/v1/chat/completions", truncated=True)
        )

    assert exc_info.value.status_code == 413


# ---- The prefill call over ZMQ ----------------------------------------------


@contextlib.asynccontextmanager
async def _prefill_service(run_prefill):
    service = PrefillService(run_prefill)
    endpoint = await service.start()
    client = PrefillClient(timeout_s=10)
    try:
        yield client, endpoint
    finally:
        await client.close()
        await service.close()


def _prefill_request(path="/v1/completions", body=None, **fields):
    body = body or {"model": "m", "prompt": "hi", "max_tokens": 8}
    return PrefillRequest(
        path=path,
        body=json.dumps(body).encode(),
        token_ids=[7, 8, 9],
        request_id=REQUEST_ID,
        lifecycle_owner=P_OWNER,
        multiplexed_model_id="m:adapter",
        headers={"x-request-id": REQUEST_ID},
        **fields,
    )


@pytest.mark.asyncio
async def test_prefill_request_runs_in_serve_request_context():
    """P runs the request under a Serve request context with its request id
    and adapter, reports lifecycle to its owner, and returns the KV transfer
    params."""
    seen = {}

    async def run_prefill(request, raw_request_info, token_ids):
        seen.update(
            request=request,
            headers=raw_request_info.headers,
            token_ids=token_ids,
            request_id=get_serve_request_id(),
            adapter=serve.get_multiplexed_model_id(),
            owner=get_request_lifecycle_owner(),
        )
        return {"remote_engine_id": "p-1", "remote_block_ids": [1, 2]}

    async with _prefill_service(run_prefill) as (client, endpoint):
        result = await client.prefill(endpoint, _prefill_request())

    assert result == {"remote_engine_id": "p-1", "remote_block_ids": [1, 2]}
    assert isinstance(seen["request"], CompletionRequest)
    assert seen["request"].max_tokens == 8
    assert seen["token_ids"] == [7, 8, 9]
    assert seen["headers"] == {"x-request-id": REQUEST_ID}
    assert seen["request_id"] == REQUEST_ID
    assert seen["adapter"] == "m:adapter"
    assert seen["owner"] == P_OWNER
    # The binding ends with the call.
    assert get_request_lifecycle_owner() is None


@pytest.mark.asyncio
async def test_prefill_request_returns_p_errors():
    error = ErrorResponse(error=ErrorInfo(message="too long", type="Bad", code=400))

    async def run_prefill(request, raw_request_info, token_ids):
        return error

    async with _prefill_service(run_prefill) as (client, endpoint):
        result = await client.prefill(endpoint, _prefill_request())
        assert result.model_dump() == error.model_dump()
        # A body P cannot run fails the call.
        with pytest.raises(ZmqRpcError, match="Not a P/D request"):
            await client.prefill(endpoint, _prefill_request(path="/v1/models"))


@pytest.mark.asyncio
async def test_prefill_server_advertises_its_endpoint(monkeypatch):
    """A KV-aware prefill replica serves prefill calls and advertises the
    endpoint in its replica metadata, next to the connector's metadata."""
    server = PDPrefillServer.__new__(PDPrefillServer)
    server._llm_config = SimpleNamespace(
        kv_connector_backend=MagicMock(replica_metadata=lambda: {"peer": "x"})
    )
    monkeypatch.setattr(
        "ray.llm._internal.serve.core.server.llm_server.LLMServer.start",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server."
        "is_kv_aware",
        lambda config: True,
    )
    await server.start()
    try:
        metadata = await server.record_replica_metadata()
        assert metadata["peer"] == "x"
        assert metadata[PD_PREFILL_ENDPOINT_KEY].startswith("tcp://")
    finally:
        await server._prefill_service.close()


# ---- Per-stage scoring on the real selection service -----------------------


class _LocalTracker(KVTokenTracker):
    """Real selection service without Serve membership tracking."""

    def __init__(self, **kwargs):
        from unittest.mock import patch

        with patch.object(serve, "get_replica_context"):
            super().__init__(**kwargs)

    def _start_replica_tracking(self):
        pass


@contextlib.asynccontextmanager
async def _stage_tracker(selection_svc_env):
    """Yield a tracker over workers "a" and "b" and their KV-event publishers."""
    pytest.importorskip("dynamo.llm")
    kv_events = pytest.importorskip("vllm.distributed.kv_events")

    tracker = _LocalTracker(
        session_affinity_ttl_s=0, selection_svc_env=selection_svc_env
    )
    tracker._register_block_size(BLOCK_SIZE, "a")
    publishers = {}
    try:
        for name in ("a", "b"):
            port, replay_port = find_free_port(), find_free_port()
            publishers[name] = kv_events.ZmqEventPublisher(
                0, endpoint=f"tcp://*:{port}", replay_endpoint=f"tcp://*:{replay_port}"
            )
            await tracker._upsert_worker(
                get_worker_id(name),
                name,
                {
                    "dp_rank": 0,
                    "max_num_batched_tokens": 8192,
                    "endpoint": f"tcp://127.0.0.1:{port}",
                    "replay_endpoint": f"tcp://127.0.0.1:{replay_port}",
                },
            )
        yield tracker, publishers
    finally:
        tracker._svc.shutdown()
        for publisher in publishers.values():
            publisher.shutdown()


async def _cache(tracker, publisher, worker_id, token_ids):
    """Publish ``token_ids`` as cached KV blocks and wait until they are indexed."""
    from vllm.distributed.kv_events import BlockStored, KVEventBatch

    num_blocks = len(token_ids) // BLOCK_SIZE
    batch = KVEventBatch(
        ts=time.time(),
        events=[
            BlockStored(
                block_hashes=list(range(num_blocks)),
                parent_block_hash=None,
                token_ids=token_ids,
                block_size=BLOCK_SIZE,
                lora_id=None,
                medium="GPU",
                lora_name=None,
            )
        ],
    )

    async def indexed():
        # PUB/SUB drops messages sent before the subscriber connects.
        publisher.publish(batch)
        scores = await tracker._svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "routing_group": _ROUTING_GROUP,
                "token_ids": PROMPT,
            }
        )
        return any(
            worker["worker_id"] == worker_id and worker["device_blocks"] == num_blocks
            for worker in scores["workers"]
        )

    await async_wait_for_condition(indexed, timeout=10, retry_interval_ms=50)


@pytest.mark.asyncio
@pytest.mark.parametrize("queued_tokens, expected", [(96, "a"), (192, "b")])
async def test_prefill_selection(queued_tokens, expected):
    """P picks the lowest queued prefill plus uncached tokens.

    "a" has half the prompt cached and ``queued_tokens`` in flight; "b" is idle
    and cold. Cost in blocks: a = queued / 16 + 8, b = 16.
    """
    async with _stage_tracker(PREFILL_SELECTION_SVC_ENV) as (tracker, pubs):
        await _cache(tracker, pubs["a"], A, PROMPT[:128])
        queued = list(range(10_000, 10_000 + queued_tokens))
        await tracker.select_worker("queued", queued, [A], 16)
        selection = await tracker.select_worker("new", PROMPT, [A, B], 16)
    assert selection["worker_id"] == get_worker_id(expected)


@pytest.mark.asyncio
async def test_decode_selection_ignores_cache():
    """D picks by decode load only: "a" has the whole prompt cached but is busy,
    so idle "b" wins."""
    async with _stage_tracker(DECODE_SELECTION_SVC_ENV) as (tracker, pubs):
        await _cache(tracker, pubs["a"], A, PROMPT)
        await tracker.select_worker("decoding", list(range(10_000, 10_064)), [A], 16)
        selection = await tracker.select_worker("new", PROMPT, [A, B], 16)
    assert selection["worker_id"] == B


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
