"""Release tests for SGLang Prefill-Decode disaggregation on Ray Serve.

SGLang PD now travels the generic decode-as-orchestrator graph
(``build_pd_openai_app`` -> ``PDDecodeServer`` -> ``PDPrefillServer``). All the
SGLang-specific coordination (bootstrap host/port/room) lives in a single
connector, ``SGLangConnectorBackend``, selected automatically from
``disaggregation_transfer_backend``.

A two-GPU node is required for the end-to-end tests (prefill on GPU 0, decode on
GPU 1). They use the real NIXL KV transport — SGLang's "fake" transport has no
bootstrap-server class, so a prefill-mode engine cannot start under it.
"""

import sys
import concurrent.futures
from types import SimpleNamespace
from typing import Optional

import pytest
from openai import OpenAI

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.engines.sglang.kv_transfer.pd_connector import (
    BOOTSTRAP_PORT_BASE_KEY,
    DEFAULT_BOOTSTRAP_PORT_BASE,
    SGLangConnectorBackend,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
)
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig
from ray.serve.schema import ApplicationStatus

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b-sglang-pd"


def _app_is_running():
    try:
        return (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].status
            == ApplicationStatus.RUNNING
        )
    except (KeyError, AttributeError):
        return False


def _sglang_config(base_gpu_id: int) -> dict:
    return LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 1,
                "max_replicas": 1,
            }
        },
        engine_kwargs={
            # disaggregation_mode is set automatically by the builder.
            "disaggregation_transfer_backend": "nixl",
            "tp_size": 1,
            "mem_fraction_static": 0.4,
            "base_gpu_id": base_gpu_id,
        },
        llm_engine="SGLang",
    ).model_dump()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sglang_pd_client():
    """Start a SGLang PD deployment using the real NIXL KV transport.

    Requires a node with at least 2 GPUs (prefill on GPU 0, decode on GPU 1).
    NIXL is pre-installed in the llm-cu130 BYOD image.
    """

    app = build_pd_openai_app(
        {
            "prefill_config": _sglang_config(base_gpu_id=0),
            "decode_config": _sglang_config(base_gpu_id=1),
        }
    )
    serve.run(app, blocking=False)
    wait_for_condition(_app_is_running, timeout=300)

    client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")
    yield client

    serve.shutdown()


# ---------------------------------------------------------------------------
# Tests — real NIXL transport (two-GPU node required)
# ---------------------------------------------------------------------------


def test_sglang_pd_chat(sglang_pd_client):
    """Verify chat completions work end-to-end over NIXL KV transfer."""

    resp = sglang_pd_client.chat.completions.create(
        model=RAY_MODEL_ID,
        messages=[{"role": "user", "content": "What is the capital of France?"}],
        max_tokens=64,
        temperature=0.0,
    )
    assert resp.choices[0].message.content.strip()


def test_sglang_pd_completions(sglang_pd_client):
    """Verify completions work end-to-end over NIXL KV transfer."""

    resp = sglang_pd_client.completions.create(
        model=RAY_MODEL_ID,
        prompt="The capital of France is",
        max_tokens=64,
        temperature=0.0,
    )
    assert resp.choices[0].text.strip()


def test_sglang_pd_streaming_chat(sglang_pd_client):
    """Verify streaming chat completions produce incremental chunks."""

    stream = sglang_pd_client.chat.completions.create(
        model=RAY_MODEL_ID,
        messages=[{"role": "user", "content": "Count to 5"}],
        max_tokens=64,
        temperature=0.0,
        stream=True,
    )

    chunks = list(stream)
    assert len(chunks) > 1, "Expected multiple streaming chunks"

    collected_text = ""
    finish_reason = None
    for chunk in chunks:
        delta = chunk.choices[0].delta
        if delta.content is not None:
            collected_text += delta.content
        if chunk.choices[0].finish_reason is not None:
            finish_reason = chunk.choices[0].finish_reason

    assert collected_text.strip(), "Streaming produced no text"
    assert finish_reason is not None, "Final chunk must have a finish_reason"


def test_sglang_pd_streaming_completions(sglang_pd_client):
    """Verify streaming completions produce incremental chunks."""

    stream = sglang_pd_client.completions.create(
        model=RAY_MODEL_ID,
        prompt="The capital of France is",
        max_tokens=32,
        temperature=0.0,
        stream=True,
    )

    chunks = list(stream)
    assert len(chunks) > 1, "Expected multiple streaming chunks"

    collected_text = ""
    finish_reason = None
    for chunk in chunks:
        if chunk.choices[0].text is not None:
            collected_text += chunk.choices[0].text
        if chunk.choices[0].finish_reason is not None:
            finish_reason = chunk.choices[0].finish_reason

    assert collected_text.strip(), "Streaming produced no text"
    assert finish_reason is not None, "Final chunk must have a finish_reason"


def test_sglang_pd_concurrent_requests(sglang_pd_client):
    """Verify multiple concurrent requests each complete successfully.

    Each request gets its own unique bootstrap_room — if rooms collide,
    SGLang's bootstrap server would mix up KV caches between requests.
    """

    def send_request(i):
        return sglang_pd_client.chat.completions.create(
            model=RAY_MODEL_ID,
            messages=[{"role": "user", "content": f"Say the number {i}"}],
            max_tokens=10,
            temperature=0.0,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(send_request, i) for i in range(4)]
        results = [f.result() for f in futures]

    for resp in results:
        assert resp.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Unit tests — no GPU required (connector-level)
# ---------------------------------------------------------------------------


def _connector() -> SGLangConnectorBackend:
    cfg = LLMConfig(
        model_loading_config={"model_id": RAY_MODEL_ID, "model_source": MODEL_ID},
        llm_engine="SGLang",
        engine_kwargs={"disaggregation_transfer_backend": "nixl"},
    )
    return SGLangConnectorBackend(cfg)


def _req(rid: Optional[str] = None) -> SimpleNamespace:
    """A stand-in request. ``rid`` is None by default, matching real clients."""
    return SimpleNamespace(rid=rid, model_copy=lambda deep: SimpleNamespace())


def test_sglang_pd_flags_on():
    backend = _connector()
    assert backend.requires_peer_binding is True
    assert backend.concurrent_handoff is True


def test_sglang_pd_bootstrap_field_injection():
    """Both prefill and decode requests carry the PREFILL bootstrap address.

    The bootstrap server runs on the prefill worker; ``peer`` is always the
    prefill replica's metadata, so both sides use that address. Both
    ``prepare_*`` calls run on the SAME request object (as in pd_server) and
    must agree on ``bootstrap_room``.
    """
    backend = _connector()
    peer = {"bootstrap_host": "10.0.0.5", "bootstrap_port": 9201}
    request = _req()

    prefill_req = backend.prepare_prefill_request(request=request, peer=peer)
    assert prefill_req.bootstrap_host == "10.0.0.5"
    assert prefill_req.bootstrap_port == 9201

    decode_req = backend.prepare_decode_request(
        request=request, peer=peer, prefill_response=None
    )
    assert decode_req.bootstrap_host == "10.0.0.5"
    assert decode_req.bootstrap_port == 9201
    assert prefill_req.bootstrap_room == decode_req.bootstrap_room


def test_sglang_pd_bootstrap_room_uniqueness():
    """Distinct requests get distinct rooms even when no client sets ``rid``.

    ``rid`` is optional and defaults to None, so this is the common path: if
    rooms were derived from it, every concurrent request would share one room
    and the bootstrap server could mix KV caches.
    """
    backend = _connector()
    peer = {"bootstrap_host": "10.0.0.5", "bootstrap_port": 9201}
    rooms = {
        backend.prepare_prefill_request(request=_req(), peer=peer).bootstrap_room
        for _ in range(1000)
    }
    assert len(rooms) == 1000, "bootstrap_room values are not unique"


def test_sglang_pd_bootstrap_room_honors_client_rid():
    """A client-supplied ``rid`` still drives the room, and stays stable."""
    backend = _connector()
    peer = {"bootstrap_host": "10.0.0.5", "bootstrap_port": 9201}

    room_a = backend.prepare_prefill_request(
        request=_req("r1"), peer=peer
    ).bootstrap_room
    room_b = backend.prepare_prefill_request(
        request=_req("r1"), peer=peer
    ).bootstrap_room
    room_c = backend.prepare_prefill_request(
        request=_req("r2"), peer=peer
    ).bootstrap_room

    assert room_a == room_b
    assert room_a != room_c


def test_sglang_pd_replica_metadata_publishes_address():
    backend = _connector()
    backend._bootstrap_host = "10.0.0.7"
    backend._bootstrap_port = 8211
    assert backend.replica_metadata() == {
        "bootstrap_host": "10.0.0.7",
        "bootstrap_port": 8211,
    }


def test_sglang_pd_missing_peer_address_raises():
    backend = _connector()
    with pytest.raises(ValueError):
        backend.prepare_prefill_request(request=_req("r1"), peer={})


def test_sglang_pd_builder_sets_disaggregation_mode():
    """The builder sets disaggregation_mode and accepts SGLang configs without
    kv_transfer_config."""
    from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
        PDServingArgs,
    )

    args = PDServingArgs.model_validate(
        {
            "prefill_config": _sglang_config(base_gpu_id=0),
            "decode_config": _sglang_config(base_gpu_id=1),
        }
    )
    assert args.prefill_config.engine_kwargs["disaggregation_mode"] == "prefill"
    assert args.decode_config.engine_kwargs["disaggregation_mode"] == "decode"
    # Decode's bootstrap BASE is shifted (per-replica offset added later in the
    # connector's setup()), so colocated decode replicas get distinct ports.
    assert (
        args.decode_config.experimental_configs[BOOTSTRAP_PORT_BASE_KEY]
        == DEFAULT_BOOTSTRAP_PORT_BASE + 1000
    )


def test_sglang_pd_rejects_data_parallel():
    """SGLang P/D with data_parallel_size>1 fails fast (unsupported)."""
    import copy

    from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
        PDServingArgs,
    )

    prefill = copy.deepcopy(_sglang_config(base_gpu_id=0))
    decode = copy.deepcopy(_sglang_config(base_gpu_id=1))
    decode["engine_kwargs"]["data_parallel_size"] = 2

    with pytest.raises(NotImplementedError, match="data_parallel_size"):
        PDServingArgs.model_validate(
            {"prefill_config": prefill, "decode_config": decode}
        )


def test_sglang_pd_rejects_mixed_engines():
    """Prefill and decode must use the same llm_engine."""
    import copy

    from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
        PDServingArgs,
    )

    sglang = _sglang_config(base_gpu_id=1)
    vllm = copy.deepcopy(sglang)
    vllm["llm_engine"] = "vLLM"
    vllm["engine_kwargs"] = {"kv_transfer_config": {"kv_connector": "NixlConnector"}}

    with pytest.raises(ValueError, match="same llm_engine"):
        PDServingArgs.model_validate({"prefill_config": vllm, "decode_config": sglang})


def test_sglang_connector_requires_bootstrap_fields():
    """The connector fails early if the request model lacks bootstrap fields."""
    from ray.llm._internal.serve.core.configs.openai_api_models import (
        ChatCompletionRequest,
    )

    if "bootstrap_room" in ChatCompletionRequest.model_fields:
        # SGLang-only env: the guard passes silently.
        SGLangConnectorBackend._check_request_model_has_bootstrap_fields()
    else:
        # vLLM installed: the guard must raise.
        with pytest.raises(RuntimeError, match="bootstrap_room"):
            SGLangConnectorBackend._check_request_model_has_bootstrap_fields()


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
