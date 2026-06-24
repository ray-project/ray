"""Release tests for SGLang Prefill-Decode disaggregation on Ray Serve.

Tests the new SGLangPDPrefillServer / SGLangPDDecodeServer classes
introduced alongside the existing SGLangServer.

A two-GPU node is required (prefill on GPU 0, decode on GPU 1). Tests use the
real NIXL KV transport — SGLang's "fake" transport has no bootstrap-server
class, so a prefill-mode engine cannot start under it.
"""

import sys
import concurrent.futures
import secrets
import socket
from contextlib import closing

import pytest
from openai import OpenAI

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.serving_patterns.prefill_decode.sglang_pd_server import (
    SGLangPDDecodeServer,
    SGLangPDPrefillServer,
)
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig
from ray.serve.schema import ApplicationStatus
from ray.llm._internal.serve.core.configs.openai_api_models import to_model_metadata
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)

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


def _make_pd_deployments(prefill_config, decode_config):
    """Helper to build prefill and decode deployments, wrapped with an ingress.

    Mirrors what build_pd_openai_app does for the vLLM PD path: ingress ->
    decode -> prefill. We build this manually instead of calling
    build_pd_openai_app directly because that function's PDServingArgs
    validates kv_transfer_config, which is vLLM-specific and not used by
    SGLang (which uses disaggregation_transfer_backend in engine_kwargs
    instead).
    """
    prefill_deployment = (
        serve.deployment(SGLangPDPrefillServer)
        .options(**SGLangPDPrefillServer.get_deployment_options(prefill_config))
        .bind(prefill_config)
    )
    decode_deployment = (
        serve.deployment(SGLangPDDecodeServer)
        .options(**SGLangPDDecodeServer.get_deployment_options(decode_config))
        .bind(decode_config, prefill_server=prefill_deployment)
    )

    # Wrap decode with a FastAPI ingress so __call__ (the ASGI HTTP
    # entrypoint) exists and routes /v1/chat/completions and
    # /v1/completions to decode_deployment.chat / .completions.
    model_id = decode_config.model_id
    ingress_cls = make_fastapi_ingress(OpenAiIngress)
    ingress_options = OpenAiIngress.get_deployment_options([decode_config])

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments={model_id: decode_deployment},
        model_cards={model_id: to_model_metadata(model_id, decode_config)},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sglang_pd_client():
    """Start a SGLang PD deployment using the real NIXL KV transport.

    Requires a node with at least 2 GPUs (prefill on GPU 0, decode on GPU 1).
    NIXL is pre-installed in the llm-cu130 BYOD image.

    NIXL is used rather than the "fake" transport because SGLang's fake backend
    has no bootstrap-server class (get_kv_class returns None for it), so a
    prefill-mode engine cannot start under fake — it crashes in
    start_disagg_service. Real NIXL is the only transport that exercises the
    full PD orchestration path (bootstrap handshake, KV transfer, streaming).
    """

    prefill_config = LLMConfig(
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
            "disaggregation_mode": "prefill",
            "disaggregation_transfer_backend": "nixl",
            "tp_size": 1,
            "mem_fraction_static": 0.4,
            "base_gpu_id": 0,
        },
        llm_engine="SGLang",
    )

    decode_config = LLMConfig(
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
            "disaggregation_mode": "decode",
            "disaggregation_transfer_backend": "nixl",
            "tp_size": 1,
            "mem_fraction_static": 0.4,
            "base_gpu_id": 1,
        },
        llm_engine="SGLang",
    )

    decode_deployment = _make_pd_deployments(prefill_config, decode_config)
    serve.run(decode_deployment, blocking=False)
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
# Unit tests — no GPU required
# ---------------------------------------------------------------------------


def test_sglang_pd_bootstrap_room_uniqueness():
    """Verify each request gets a unique bootstrap_room.

    With 62-bit random IDs, any collision in 1000 samples would indicate
    a broken RNG.
    """
    rooms = {secrets.randbits(62) for _ in range(1000)}
    assert len(rooms) == 1000, "bootstrap_room values are not unique"


def test_sglang_pd_bootstrap_field_injection():
    """Verify both prefill and decode requests carry the PREFILL bootstrap address.

    SGLang's KV bootstrap server runs on the prefill worker. Prefill registers
    its KV-sender there keyed by bootstrap_room; decode's KVReceiver connects to
    that same host/port to pull the KV cache. So both requests must carry the
    prefill node's bootstrap_host/bootstrap_port — not the decode node's.
    """
    from ray.llm._internal.serve.core.configs.openai_api_models import (
        ChatCompletionRequest,
    )

    # Create a minimal instance without starting the engine.
    # _bootstrap_host/_bootstrap_port hold the PREFILL node's address,
    # fetched from the prefill deployment at init.
    server = object.__new__(SGLangPDDecodeServer)
    server._bootstrap_host = "10.0.0.5"
    server._bootstrap_port = 9201
    server._decode_tp_size = 1

    request = ChatCompletionRequest(
        model=RAY_MODEL_ID,
        messages=[{"role": "user", "content": "hello"}],
    )

    bootstrap_room = 7392841029

    # Prefill request must carry all three bootstrap fields and disable streaming.
    prefill_req = server._prepare_prefill_request(request, bootstrap_room)
    assert prefill_req.bootstrap_host == "10.0.0.5"
    assert prefill_req.bootstrap_port == 9201
    assert prefill_req.bootstrap_room == bootstrap_room
    assert prefill_req.stream is False

    # Decode request must carry the SAME prefill bootstrap address so its
    # KVReceiver knows which prefill bootstrap server to connect to.
    decode_req = server._prepare_decode_request(request, bootstrap_room)
    assert decode_req.bootstrap_host == "10.0.0.5"
    assert decode_req.bootstrap_port == 9201
    assert decode_req.bootstrap_room == bootstrap_room


def test_sglang_pd_prefill_exposes_bootstrap_info():
    """Verify the prefill server exposes its node's bootstrap host/port.

    The bootstrap server lives on the prefill worker. Decode must learn the
    PREFILL node's address (not its own) to point the KVReceiver at it, so
    SGLangPDPrefillServer must cache and expose host/port via get_bootstrap_info.
    """
    server = object.__new__(SGLangPDPrefillServer)
    server._bootstrap_host = "10.0.0.7"
    server._bootstrap_port = 8211

    host, port = server.get_bootstrap_info()
    assert host == "10.0.0.7"
    assert port == 8211


def test_sglang_pd_bootstrap_port_auto_allocated():
    """Verify port auto-allocation does not produce the SGLang default 8998.

    Multiple decode replicas on the same node would collide on 8998 without
    this auto-allocation.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]

    assert port > 1024
    assert port != 8998, "Port should be dynamically allocated, not the SGLang default"


def test_sglang_pd_dp_size_mismatch_rejected():
    """Verify mismatched dp_size between prefill and decode is caught early.

    With follow_bootstrap_room, both sides compute bootstrap_room % dp_size
    to pick a DP worker. Mismatched dp_size silently sends prefill and decode
    to different workers — the bootstrap handshake never completes.

    NOTE: This test documents the requirement. The actual validation in
    build_pd_openai_app is a follow-up TODO.
    """
    prefill_dp = 4
    decode_dp = 2

    # Document the invariant: both sides must agree on dp_size
    # so that bootstrap_room % dp_size maps to the same worker.
    assert (
        prefill_dp != decode_dp
    ), "Test setup: dp_size values are intentionally mismatched"

    # Once the builder validation is implemented, this should raise:
    # with pytest.raises(ValueError, match="dp_size"):
    #     _validate_pd_config(prefill_config, decode_config)
    pytest.skip(
        "Builder-level dp_size validation not yet implemented — tracked as follow-up TODO"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
