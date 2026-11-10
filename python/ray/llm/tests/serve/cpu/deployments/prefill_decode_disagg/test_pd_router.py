"""KV-aware P/D: application graphs, the router flow, the prefill handle call,
and per-stage scoring."""

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

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition
from ray.exceptions import ActorDiedError, ActorUnavailableError, RayTaskError
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorInfo,
    ErrorResponse,
)
from ray.llm._internal.serve.core.protocol import RawRequestInfo
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
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.pd_router import (
    LLMPDRouter,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDPrefillServer,
)
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)
from ray.serve.exceptions import DeploymentUnavailableError, ReplicaUnavailableError

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


_HANG = object()


class _Response:
    """A dispatched prefill call that returns ``result``, raises ``error``, or
    never finishes (``result=_HANG``)."""

    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error
        self.cancelled = False

    def __await__(self):
        return self._get().__await__()

    async def _get(self):
        if self._error is not None:
            raise self._error
        if self._result is _HANG:
            await asyncio.Event().wait()
        return self._result

    def cancel(self):
        self.cancelled = True


def _pd_router(prefill_result=None, prefill_error=None, reserve_error=None):
    """An LLMPDRouter whose P handle selects replica "prefill-1" (or raises
    ``reserve_error``), whose handle call to it returns ``prefill_result`` (or
    raises ``prefill_error``), and whose D pick returns replica "decode-1"."""
    router = LLMPDRouter.__new__(LLMPDRouter)
    router._base_model_id = None
    router._handle = MagicMock()
    router._prefill = MagicMock()
    router._tokenizer = MagicMock(tokenize=AsyncMock(return_value=[1, 2, 3]))
    router._push_prompt_tokens = MagicMock(return_value="token-key")
    router.reservations = []

    @contextlib.asynccontextmanager
    async def reserve(prefill, pd_request, token_ids):
        router.reservations.append((prefill, pd_request, token_ids))
        if reserve_error is not None:
            raise reserve_error
        yield SimpleNamespace(replica_id="prefill-1")

    router._reserve_prefill_replica = reserve
    router.response = _Response(prefill_result, prefill_error)
    for prefill in (router._prefill, router._prefill.options.return_value):
        prefill.prefill.dispatch.return_value = router.response
    router._pick_replica = AsyncMock(return_value=("h", 1, "decode-1", "tcp://t"))
    router._prefill_tracker = MagicMock(lifecycle_endpoint=P_OWNER)
    router._kv_token_tracker = MagicMock(lifecycle_endpoint=D_OWNER)
    return router


async def _route(router, request):
    """Route ``request`` as the Serve request ``REQUEST_ID``, the id the P
    handle call books and runs under."""
    serve.context._serve_request_context.set(
        serve.context._RequestContext(request_id=REQUEST_ID)
    )
    return await router.route_request(request)


@pytest.mark.asyncio
async def test_route_runs_prefill_then_picks_decode():
    router = _pd_router({"remote_engine_id": "p-1"})
    request = _Request(CHAT, "/v1/chat/completions")

    decision = await _route(router, request)

    # P is selected with the prompt token IDs, which book its prefill load.
    ((prefill, pd_request, token_ids),) = router.reservations
    assert prefill is router._prefill.prefill
    assert isinstance(pd_request, ChatCompletionRequest)
    assert token_ids == [1, 2, 3]
    # Then a handle call runs prefill on that replica with the token IDs, and P
    # reports its lifecycle to the prefill tracker.
    args, kwargs = prefill.dispatch.call_args
    selection, call_request, raw_request_info, call_token_ids = args
    assert selection.replica_id == "prefill-1"
    assert call_request is pd_request
    assert raw_request_info.headers == dict(request.headers)
    assert call_token_ids == [1, 2, 3]
    assert kwargs == {"lifecycle_owner": P_OWNER}
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

    await _route(
        router, _Request(CHAT, "/v1/chat/completions", headers={"x-session-id": "s-1"})
    )

    router._prefill.options.assert_called_once_with(session_id="s-1")
    router._handle.options.assert_called_once_with(session_id="s-1")
    assert router.reservations[0][0] is router._prefill.options().prefill
    assert router._pick_replica.call_args.args[0] is router._handle.options()


@pytest.mark.asyncio
async def test_route_returns_prefill_error():
    error = ErrorResponse(error=ErrorInfo(message="too long", type="Bad", code=400))
    router = _pd_router(error)

    response = await _route(router, _Request(CHAT, "/v1/chat/completions"))

    assert response.status_code == 400
    assert json.loads(response.body)["error"]["message"] == "too long"
    router._pick_replica.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error, raised",
    [
        (ActorDiedError(), RuntimeError),
        (ActorUnavailableError("unreachable", None), RuntimeError),
        (ReplicaUnavailableError("prefill-1"), RuntimeError),
        (RayTaskError("prefill", "", ValueError("boom")), RuntimeError),
        (asyncio.TimeoutError(), RuntimeError),
        (ValueError("bad"), ValueError),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_failed_prefill_frees_its_booking(error, raised):
    """P may never report a prefill that failed, so the router frees P's
    booking itself. A lost or failed P replica is a retryable routing failure
    (503)."""
    router = _pd_router(prefill_error=error)

    with pytest.raises(raised):
        await _route(router, _Request(CHAT, "/v1/chat/completions"))

    router._prefill_tracker.release_request.assert_called_once_with(REQUEST_ID)
    router._pick_replica.assert_not_called()
    # A cancelled call cancels the prefill on P.
    assert router.response.cancelled == (raised is asyncio.CancelledError)


@pytest.mark.asyncio
async def test_failed_prefill_selection_frees_its_booking():
    """Selection books P's load before it reserves a slot on the replica, so a
    failed selection frees the booking too."""
    router = _pd_router(reserve_error=DeploymentUnavailableError(DeploymentID("p")))

    with pytest.raises(RuntimeError, match="Prefill on .* failed"):
        await _route(router, _Request(CHAT, "/v1/chat/completions"))

    router._prefill.prefill.dispatch.assert_not_called()
    router._prefill_tracker.release_request.assert_called_once_with(REQUEST_ID)


@pytest.mark.asyncio
async def test_prefill_timeout_cancels_the_call():
    """Past HAProxy's routing timeout, the router cancels the prefill on P,
    frees its booking, and fails the route as retryable."""
    router = _pd_router(_HANG)
    router._prefill_timeout_s = 0.1

    with pytest.raises(RuntimeError, match="TimeoutError"):
        await _route(router, _Request(CHAT, "/v1/chat/completions"))

    assert router.response.cancelled
    router._prefill_tracker.release_request.assert_called_once_with(REQUEST_ID)
    router._pick_replica.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body, path",
    [(CHAT, "/v1/models"), ({"model": "m", "messages": "bad"}, "/v1/chat/completions")],
)
async def test_route_sends_other_requests_to_decode(body, path):
    router = _pd_router({})

    decision = await _route(router, _Request(body, path))

    assert router.reservations == []
    router._prefill.prefill.dispatch.assert_not_called()
    assert decision["replica_id"] == "decode-1"
    assert (
        KV_TRANSFER_PARAMS_HEADER
        not in decision[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD]
    )


@pytest.mark.asyncio
async def test_route_rejects_truncated_body():
    router = _pd_router({})

    with pytest.raises(HTTPException) as exc_info:
        await _route(router, _Request(CHAT, "/v1/chat/completions", truncated=True))

    assert exc_info.value.status_code == 413


# ---- The prefill handle call ------------------------------------------------


class _PrefillBackend:
    """Connector backend stand-in: prefill runs the request as is."""

    def prepare_prefill_request(self, request, peer):
        return request.model_copy()

    def replica_metadata(self):
        return {}


def _completion(prompt="hi"):
    return CompletionRequest(model="m", prompt=prompt, max_tokens=8)


def _prefill_server(engine):
    """A PDPrefillServer whose engine is ``engine(request, raw_request_info)``,
    an async generator."""
    server = PDPrefillServer.__new__(PDPrefillServer)
    server._llm_config = SimpleNamespace(kv_connector_backend=_PrefillBackend())

    async def completions(request, raw_request_info=None):
        return engine(request, raw_request_info)

    server.completions = completions
    return server


@pytest.mark.asyncio
async def test_prefill_reports_lifecycle_to_its_owner():
    """P runs prefill on the router's token IDs with the engine reporting the
    request's lifecycle to the router's prefill tracker, and returns the KV
    transfer params."""
    seen = {}

    async def engine(request, raw_request_info):
        seen.update(prompt=request.prompt, owner=get_request_lifecycle_owner())
        yield SimpleNamespace(kv_transfer_params={"remote_engine_id": "p-1"})

    server = _prefill_server(engine)
    result = await server.prefill(
        _completion(), RawRequestInfo(headers={}), [7, 8, 9], lifecycle_owner=P_OWNER
    )

    assert result == {"remote_engine_id": "p-1"}
    assert seen == {"prompt": [7, 8, 9], "owner": P_OWNER}
    # The binding ends with the call.
    assert get_request_lifecycle_owner() is None


@pytest.mark.asyncio
async def test_prefill_returns_p_errors():
    error = ErrorResponse(error=ErrorInfo(message="too long", type="Bad", code=400))

    async def engine(request, raw_request_info):
        yield error

    server = _prefill_server(engine)
    result = await server.prefill(
        _completion(), RawRequestInfo(headers={}), [7, 8, 9], lifecycle_owner=P_OWNER
    )

    assert result is error
    assert get_request_lifecycle_owner() is None


@serve.deployment
class _PrefillDeployment(PDPrefillServer):
    """A PDPrefillServer whose engine records what each request ran with, and
    whose behavior follows the request's ``x-prefill`` header: ``error``
    returns P's error, ``fail`` raises, and ``hang`` never finishes."""

    def __init__(self):
        self.engine = None
        self._llm_config = SimpleNamespace(kv_connector_backend=_PrefillBackend())
        self._seen = []
        self._cancelled = []

    async def completions(self, request, raw_request_info=None):
        mode = raw_request_info.headers.get("x-prefill")
        request_id = get_serve_request_id()
        self._seen.append(
            {
                "request": request.model_dump(include={"model", "max_tokens"}),
                "prompt": request.prompt,
                "headers": raw_request_info.headers,
                "request_id": request_id,
                "adapter": serve.get_multiplexed_model_id(),
                "owner": get_request_lifecycle_owner(),
            }
        )
        if mode == "fail":
            raise ValueError("prefill failed")

        async def stream():
            if mode == "error":
                yield ErrorResponse(
                    error=ErrorInfo(message="too long", type="Bad", code=400)
                )
                return
            if mode == "hang":
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self._cancelled.append(request_id)
                    raise
            yield SimpleNamespace(kv_transfer_params={"remote_engine_id": "p-1"})

        return stream()

    def seen(self):
        return self._seen

    def cancelled(self):
        return self._cancelled

    def current_actor(self):
        return ray.get_runtime_context().current_actor


@pytest.fixture(scope="module")
def prefill_handle():
    """A real Serve P deployment, routed by the default request router."""
    module = sys.modules[__name__]
    ray.cloudpickle.register_pickle_by_value(module)
    started_ray = not ray.is_initialized()
    if started_ray:
        ray.init()
    try:
        yield serve.run(
            _PrefillDeployment.bind(), name=PD_BACKEND_APP_NAME, route_prefix=None
        )
    finally:
        serve.shutdown()
        if started_ray:
            ray.shutdown()
        ray.cloudpickle.unregister_pickle_by_value(module)


def _handle_router(prefill_handle, timeout_s=30):
    """An LLMPDRouter that runs prefill on a real Serve P deployment."""
    router = LLMPDRouter.__new__(LLMPDRouter)
    router._prefill = prefill_handle
    router._prefill_tracker = MagicMock(lifecycle_endpoint=P_OWNER)
    router._prefill_timeout_s = timeout_s
    return router


async def _run_prefill(router, request_id, mode=None, adapter=""):
    """Run prefill as the route request ``request_id``."""
    serve.context._serve_request_context.set(
        serve.context._RequestContext(request_id=request_id)
    )
    request = _Request(
        {"model": "m", "prompt": "hi", "max_tokens": 8},
        "/v1/completions",
        headers={"x-prefill": mode} if mode else None,
    )
    return await router._run_prefill(request, _completion(), [7, 8, 9], adapter, None)


async def _seen(prefill_handle, request_id):
    (seen,) = [
        s for s in await prefill_handle.seen.remote() if s["request_id"] == request_id
    ]
    return seen


@pytest.mark.asyncio
async def test_prefill_handle_call_runs_under_the_booked_request(prefill_handle):
    """The handle call runs prefill on P under the route request's id, which
    P's KVAwareRouter booked the selection under, with the request's adapter,
    and P reports its lifecycle to the router's prefill tracker."""
    router = _handle_router(prefill_handle)

    result = await _run_prefill(router, "req-ok", adapter="m:adapter")

    assert result == {"remote_engine_id": "p-1"}
    seen = await _seen(prefill_handle, "req-ok")
    assert seen["request"] == {"model": "m", "max_tokens": 8}
    assert seen["prompt"] == [7, 8, 9]
    assert seen["headers"][SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER.lower()] == (
        "/v1/completions"
    )
    assert seen["adapter"] == "m:adapter"
    assert seen["owner"] == P_OWNER
    router._prefill_tracker.release_request.assert_not_called()


@pytest.mark.asyncio
async def test_prefill_handle_call_returns_p_errors(prefill_handle):
    router = _handle_router(prefill_handle)

    result = await _run_prefill(router, "req-error", mode="error")

    assert isinstance(result, ErrorResponse)
    assert result.error.message == "too long"
    router._prefill_tracker.release_request.assert_not_called()


@pytest.mark.asyncio
async def test_failed_prefill_handle_call_frees_its_booking(prefill_handle):
    """P failing the call is a retryable routing failure (503)."""
    router = _handle_router(prefill_handle)

    with pytest.raises(RuntimeError, match="Prefill on .* failed") as exc_info:
        await _run_prefill(router, "req-fail", mode="fail")

    # P's error arrives as the handle call's error.
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert "prefill failed" in str(exc_info.value.__cause__)
    router._prefill_tracker.release_request.assert_called_once_with("req-fail")


@pytest.mark.asyncio
async def test_timed_out_prefill_handle_call_cancels_on_p(prefill_handle):
    """Past the routing timeout, the router frees P's booking and the prefill
    stops on P."""
    router = _handle_router(prefill_handle, timeout_s=3)

    with pytest.raises(RuntimeError, match="TimeoutError"):
        await _run_prefill(router, "req-hang", mode="hang")

    router._prefill_tracker.release_request.assert_called_once_with("req-hang")
    await _wait_until_cancelled_on_p(prefill_handle, "req-hang")


async def _wait_until_started_on_p(prefill_handle, request_id):
    async def started():
        seen = await prefill_handle.seen.remote()
        return any(s["request_id"] == request_id for s in seen)

    await async_wait_for_condition(started, timeout=30)


async def _wait_until_cancelled_on_p(prefill_handle, request_id):
    async def cancelled():
        return request_id in await prefill_handle.cancelled.remote()

    await async_wait_for_condition(cancelled, timeout=30)


@pytest.mark.asyncio
async def test_prefill_handle_calls_run_concurrently(prefill_handle):
    """A slow prefill doesn't delay another one. Cancelling a call frees P's
    booking and stops the prefill on P."""
    router = _handle_router(prefill_handle)
    slow = asyncio.create_task(_run_prefill(router, "req-slow", mode="hang"))
    await _wait_until_started_on_p(prefill_handle, "req-slow")

    assert await _run_prefill(router, "req-fast") == {"remote_engine_id": "p-1"}
    assert not slow.done()

    slow.cancel()
    with pytest.raises(asyncio.CancelledError):
        await slow
    router._prefill_tracker.release_request.assert_called_once_with("req-slow")
    await _wait_until_cancelled_on_p(prefill_handle, "req-slow")


@pytest.mark.asyncio
async def test_lost_p_replica_fails_the_prefill_handle_call(prefill_handle):
    """A P replica that dies mid-call fails the call as a retryable routing
    failure (503), and the router frees P's booking.

    Kills the module's P replica, so it runs after the other handle call tests.
    """
    router = _handle_router(prefill_handle)
    replica = await prefill_handle.current_actor.remote()
    call = asyncio.create_task(_run_prefill(router, "req-lost", mode="hang"))
    await _wait_until_started_on_p(prefill_handle, "req-lost")

    ray.kill(replica, no_restart=True)

    with pytest.raises(RuntimeError, match="Prefill on .* failed"):
        await asyncio.wait_for(call, 30)
    router._prefill_tracker.release_request.assert_called_once_with("req-lost")


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


def _active_requests(tracker):
    """Active requests per worker in the tracker's selection service."""
    return {
        load["worker_id"]: load["active_requests"]
        for entry in tracker._svc.loads()
        for load in entry["loads"]
        if load["active_requests"]
    }


@pytest.mark.asyncio
async def test_reselecting_a_routing_call_replaces_its_booking():
    """Serve selects again for a routing call whose replica couldn't take it.
    The new selection replaces that call's booking. Another call still can't
    book a request id that is booked."""
    async with _stage_tracker(PREFILL_SELECTION_SVC_ENV) as (tracker, _):
        await tracker.select_worker(
            "req", PROMPT, [A], 16, internal_request_id="call-1"
        )
        assert _active_requests(tracker) == {A: 1}

        selection = await tracker.select_worker(
            "req", PROMPT, [B], 16, internal_request_id="call-1"
        )
        assert selection["worker_id"] == B
        assert _active_requests(tracker) == {B: 1}

        with pytest.raises(Exception, match="already reserved"):
            await tracker.select_worker(
                "req", PROMPT, [A], 16, internal_request_id="call-2"
            )
        assert _active_requests(tracker) == {B: 1}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
