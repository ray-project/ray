"""KV-aware P/D: application graphs, the router flow, and per-stage scoring."""

import asyncio
import base64
import contextlib
import json
import sys
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.datastructures import Headers

from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ErrorInfo,
    ErrorResponse,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DECODE_SELECTION_SVC_ENV,
    DEFAULT_KV_EVENTS_PORT_BASE,
    DEFAULT_KV_TOKEN_PORT_BASE,
    KV_EVENTS_PORT_BASE_KEY,
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

from ray.llm._internal.serve.routing_policies.kv_aware.vllm.pd_router import (
    LLMPDRouter,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)

BLOCK_SIZE = 16
PROMPT = list(range(1, 257))  # 16 blocks
A, B = get_worker_id("a"), get_worker_id("b")


def _kv_config():
    return LLMConfig(
        model_loading_config={"model_id": "model"},
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
    )


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


class _Request:
    """The /internal/route request HAProxy sends to the router."""

    def __init__(self, body, path, truncated=False):
        self._body = json.dumps(body).encode()
        headers = {SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER: path}
        if truncated:
            headers["x-body-truncated"] = "1"
        self.headers = Headers(headers)

    async def body(self):
        return self._body


def _pd_router(prefill_result):
    """An LLMPDRouter whose P call returns ``prefill_result`` and whose D pick
    returns replica "decode-1"."""
    router = LLMPDRouter.__new__(LLMPDRouter)
    router._base_model_id = None
    router._handle = MagicMock()
    router._tokenizer = MagicMock(tokenize=AsyncMock(return_value=[1, 2, 3]))
    router._push_prompt_tokens = MagicMock(return_value="token-key")
    router._pick_replica = AsyncMock(return_value=("h", 1, "decode-1", "tcp://t"))
    response = asyncio.get_running_loop().create_future()
    response.set_result(prefill_result)
    router._prefill = MagicMock()
    router._prefill.prefill.remote.return_value = response
    return router


CHAT = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}


@pytest.mark.asyncio
async def test_route_runs_prefill_then_picks_decode():
    router = _pd_router({"remote_engine_id": "p-1"})

    decision = await router.route_request(_Request(CHAT, "/v1/chat/completions"))

    request, _ = router._prefill.prefill.remote.call_args.args
    assert isinstance(request, ChatCompletionRequest)
    assert router._prefill.prefill.remote.call_args.kwargs == {
        "request_token_ids": [1, 2, 3]
    }
    assert router._pick_replica.call_args.args[2] == [1, 2, 3]
    headers = decision.pop(RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD)
    assert decision == {"application": PD_BACKEND_APP_NAME, "replica_id": "decode-1"}
    assert json.loads(base64.b64decode(headers[KV_TRANSFER_PARAMS_HEADER])) == {
        "remote_engine_id": "p-1"
    }
    assert headers[KV_TOKEN_KEY_HEADER] == "token-key"


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
    "body, path",
    [(CHAT, "/v1/models"), ({"model": "m", "messages": "bad"}, "/v1/chat/completions")],
)
async def test_route_sends_other_requests_to_decode(body, path):
    router = _pd_router({})

    decision = await router.route_request(_Request(body, path))

    router._prefill.prefill.remote.assert_not_called()
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


class _LocalTracker(KVTokenTracker):
    """Real selection service without Serve membership tracking."""

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
    async with _stage_tracker(PREFILL_SELECTION_SVC_ENV) as (tracker, publishers):
        await _cache(tracker, publishers["a"], A, PROMPT[:128])
        queued = list(range(10_000, 10_000 + queued_tokens))
        await tracker.select_worker("queued", queued, [A], 16)
        selection = await tracker.select_worker("new", PROMPT, [A, B], 16)
    assert selection["worker_id"] == get_worker_id(expected)


@pytest.mark.asyncio
async def test_decode_selection_ignores_cache():
    """D picks by decode load only: "a" has the whole prompt cached but is busy,
    so idle "b" wins. Aggregated scoring would pick "a"."""
    async with _stage_tracker(DECODE_SELECTION_SVC_ENV) as (tracker, publishers):
        await _cache(tracker, publishers["a"], A, PROMPT)
        await tracker.select_worker("decoding", list(range(10_000, 10_064)), [A], 16)
        selection = await tracker.select_worker("new", PROMPT, [A, B], 16)
    assert selection["worker_id"] == B


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
