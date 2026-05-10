import sys

import httpx
import pytest
from openai import OpenAI

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.engines.sglang import SGLangServer
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.schema import ApplicationStatus

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b-sglang"


def _app_is_running():
    try:
        return (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].status
            == ApplicationStatus.RUNNING
        )
    except (KeyError, AttributeError):
        return False


@pytest.fixture(scope="module")
def sglang_client():
    """Start an SGLang server once for all tests in this module."""
    llm_config = LLMConfig(
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
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 1,
            "mem_fraction_static": 0.8,
        },
    )

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(_app_is_running, timeout=300)

    client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")
    yield client

    serve.shutdown()


def test_sglang_serve_e2e(sglang_client):
    """Verify chat and completions endpoints work end-to-end."""
    chat_resp = sglang_client.chat.completions.create(
        model=RAY_MODEL_ID,
        messages=[{"role": "user", "content": "What is the capital of France?"}],
        max_tokens=64,
        temperature=0.0,
    )
    assert chat_resp.choices[0].message.content.strip()

    comp_resp = sglang_client.completions.create(
        model=RAY_MODEL_ID,
        prompt="The capital of France is",
        max_tokens=64,
        temperature=0.0,
    )
    assert comp_resp.choices[0].text.strip()


def test_sglang_streaming_chat(sglang_client):
    """Verify streaming chat completions produce incremental chunks."""
    stream = sglang_client.chat.completions.create(
        model=RAY_MODEL_ID,
        messages=[{"role": "user", "content": "Count to 5"}],
        max_tokens=64,
        temperature=0.0,
        stream=True,
    )

    chunks = list(stream)
    assert len(chunks) > 1, "Expected multiple streaming chunks"

    # First chunk must include the assistant role.
    first_delta = chunks[0].choices[0].delta
    assert first_delta.role == "assistant"

    # Collect all content fragments.
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


def test_sglang_streaming_completions(sglang_client):
    """Verify streaming completions produce incremental chunks."""
    stream = sglang_client.completions.create(
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


def test_sglang_tokenize(sglang_client):
    """Verify tokenize endpoint works."""
    resp = httpx.post(
        "http://localhost:8000/tokenize",
        json={"model": RAY_MODEL_ID, "prompt": "Hello world"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "tokens" in data
    assert "count" in data
    assert "max_model_len" in data
    assert isinstance(data["tokens"], list)
    assert len(data["tokens"]) > 0
    assert data["count"] == len(data["tokens"])
    assert data["max_model_len"] > 0


def test_sglang_detokenize(sglang_client):
    """Verify detokenize endpoint works and round-trips with tokenize."""
    # First tokenize
    tok_resp = httpx.post(
        "http://localhost:8000/tokenize",
        json={"model": RAY_MODEL_ID, "prompt": "Hello world"},
    )
    assert tok_resp.status_code == 200
    tokens = tok_resp.json()["tokens"]

    # Then detokenize
    detok_resp = httpx.post(
        "http://localhost:8000/detokenize",
        json={"model": RAY_MODEL_ID, "tokens": tokens},
    )
    assert detok_resp.status_code == 200
    data = detok_resp.json()
    assert "text" in data
    assert "Hello world" in data["text"]


def test_sglang_batched_completions(sglang_client):
    """Verify that batched completions (multiple prompts) return one choice per prompt."""
    prompts = [
        "The capital of France is",
        "The capital of Germany is",
        "The capital of Japan is",
    ]
    batch_resp = sglang_client.completions.create(
        model=RAY_MODEL_ID,
        prompt=prompts,
        max_tokens=16,
        temperature=0.0,
    )

    assert len(batch_resp.choices) == len(prompts)

    for i, choice in enumerate(batch_resp.choices):
        assert choice.index == i
        assert choice.text.strip()

    assert batch_resp.usage.total_tokens > 0


@pytest.fixture(scope="module")
def sglang_embedding_client():
    """Start an SGLang server with is_embedding enabled for embedding tests."""
    llm_config = LLMConfig(
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
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 1,
            "mem_fraction_static": 0.8,
            "is_embedding": True,
        },
    )

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(_app_is_running, timeout=300)

    client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")
    yield client

    serve.shutdown()


def test_sglang_embeddings(sglang_embedding_client):
    """Verify embeddings endpoint works with single and batch inputs."""
    # Single input
    emb_resp = sglang_embedding_client.embeddings.create(
        model=RAY_MODEL_ID,
        input="Hello world",
    )
    assert emb_resp.data
    assert len(emb_resp.data) == 1
    assert emb_resp.data[0].embedding
    assert len(emb_resp.data[0].embedding) > 0
    assert emb_resp.usage.prompt_tokens > 0

    # Batch input
    emb_batch_resp = sglang_embedding_client.embeddings.create(
        model=RAY_MODEL_ID,
        input=["Hello world", "How are you"],
    )
    assert len(emb_batch_resp.data) == 2
    assert emb_batch_resp.data[0].embedding
    assert emb_batch_resp.data[1].embedding


def test_sglang_serve_e2e_multi_gpu():
    """Verify SGLang multi-GPU deployment works with tp_size=2.

    Requires a node with at least 2 GPUs. Confirms that:
    - Placement group bundles are correctly constructed as [{"GPU": 1, "CPU": 1}, {"GPU": 1}]
    - The model loads and serves inference correctly across both GPUs.
    """
    llm_config = LLMConfig(
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
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 2,
            "mem_fraction_static": 0.8,
        },
    )

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    try:
        wait_for_condition(_app_is_running, timeout=300)

        deployment_options = SGLangServer.get_deployment_options(llm_config)
        expected_bundles = [{"GPU": 1, "CPU": 1}, {"GPU": 1}]
        assert deployment_options["placement_group_bundles"] == expected_bundles, (
            f"Expected placement group bundles {expected_bundles}, "
            f"got {deployment_options['placement_group_bundles']}"
        )

        client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

        chat_resp = client.chat.completions.create(
            model=RAY_MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=64,
            temperature=0.0,
        )
        assert chat_resp.choices[0].message.content.strip()

        comp_resp = client.completions.create(
            model=RAY_MODEL_ID,
            prompt="The capital of France is",
            max_tokens=64,
            temperature=0.0,
        )
        assert comp_resp.choices[0].text.strip()
    finally:
        serve.shutdown()


def test_sglang_serve_e2e_pipeline_parallel():
    """Verify SGLang multi-GPU deployment works with tp_size=2, pp_size=2.

    Requires a node with at least 4 GPUs. Confirms that:
    - Placement group bundles are correctly constructed as
      [{"GPU": 1, "CPU": 1}, {"GPU": 1}, {"GPU": 1}, {"GPU": 1}]
    - The model loads and serves inference correctly across all 4 GPUs.
    """
    llm_config = LLMConfig(
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
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 2,
            "pp_size": 2,
            "mem_fraction_static": 0.8,
        },
    )

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    try:
        wait_for_condition(_app_is_running, timeout=300)

        # tp_size=2, pp_size=2 → num_devices=4 → 4 GPU bundles
        # first bundle merges replica actor CPU with first GPU worker
        deployment_options = SGLangServer.get_deployment_options(llm_config)
        expected_bundles = [{"GPU": 1, "CPU": 1}, {"GPU": 1}, {"GPU": 1}, {"GPU": 1}]
        assert deployment_options["placement_group_bundles"] == expected_bundles, (
            f"Expected placement group bundles {expected_bundles}, "
            f"got {deployment_options['placement_group_bundles']}"
        )

        client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

        chat_resp = client.chat.completions.create(
            model=RAY_MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=64,
            temperature=0.0,
        )
        assert chat_resp.choices[0].message.content.strip()

        comp_resp = client.completions.create(
            model=RAY_MODEL_ID,
            prompt="The capital of France is",
            max_tokens=64,
            temperature=0.0,
        )
        assert comp_resp.choices[0].text.strip()
    finally:
        serve.shutdown()


def test_sglang_custom_placement_group_config():
    """Verify explicit placement_group_config is respected by get_deployment_options.

    Covers the configuration pattern used in serve_sglang_multinode_example.py
    where users provide custom bundles and strategy for multi-node TP/PP.
    Does not require GPUs — only tests configuration logic.
    """
    custom_bundles = [{"GPU": 1}] * 8
    custom_strategy = "PACK"

    llm_config = LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 1,
                "max_replicas": 2,
                "target_ongoing_requests": 4,
            }
        },
        placement_group_config={
            "placement_group_bundles": custom_bundles,
            "placement_group_strategy": custom_strategy,
        },
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 4,
            "pp_size": 2,
            "mem_fraction_static": 0.8,
        },
    )

    deployment_options = SGLangServer.get_deployment_options(llm_config)
    assert deployment_options["placement_group_bundles"] == custom_bundles, (
        f"Expected custom bundles {custom_bundles}, "
        f"got {deployment_options['placement_group_bundles']}"
    )
    assert deployment_options["placement_group_strategy"] == custom_strategy, (
        f"Expected strategy '{custom_strategy}', "
        f"got '{deployment_options['placement_group_strategy']}'"
    )


def test_sglang_custom_placement_group_default_strategy():
    """Verify that custom bundles without an explicit strategy default to PACK."""
    custom_bundles = [{"GPU": 1}] * 4

    llm_config = LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 2,
            "pp_size": 2,
        },
        placement_group_config={
            "placement_group_bundles": custom_bundles,
        },
    )

    deployment_options = SGLangServer.get_deployment_options(llm_config)
    assert deployment_options["placement_group_bundles"] == custom_bundles
    assert deployment_options["placement_group_strategy"] == "PACK"


# ---------------------------------------------------------------------------
# Protocol decoupling tests — verify modules are importable without vLLM
# and that SGLang protocol models are wired correctly.
# ---------------------------------------------------------------------------


class TestSGLangProtocolDecoupling:
    """Verify modules are importable without vLLM and SGLang models are wired."""

    def test_modules_importable_without_vllm(self):
        """openai_api_models, ingress, llm_server, and ray.serve.llm should
        all import without vLLM installed."""
        from ray.llm._internal.serve.core.configs import openai_api_models  # noqa: F401
        from ray.llm._internal.serve.core.ingress import ingress  # noqa: F401
        from ray.llm._internal.serve.core.server.llm_server import LLMServer
        import ray.serve.llm  # noqa: F401

        assert LLMServer._default_engine_cls is None

    def test_error_response_round_trip(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ErrorInfo,
            ErrorResponse,
        )

        resp = ErrorResponse(error=ErrorInfo(message="bad", code=400, type="Invalid"))
        assert resp.error.message == "bad"
        assert resp.error.code == 400
        assert resp.model_dump()["error"]["message"] == "bad"

    def test_score_request_is_sglang_scoring_request(self):
        from sglang.srt.entrypoints.openai.protocol import ScoringRequest
        from ray.llm._internal.serve.core.configs.openai_api_models import ScoreRequest

        assert issubclass(ScoreRequest, ScoringRequest)


# ---------------------------------------------------------------------------
# SGLang WideEP Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def cleanup_wideep():
    """Cleanup Ray Serve resources after WideEP tests."""
    yield
    serve.shutdown()


def test_sglang_wideep_config_validation():
    """Verify WideEP configuration constraints are validated.

    - moe_a2a_backend must be 'deepep'
    - moe_dp_size must be 1 for pure WideEP
    - nnodes must divide total_gpus evenly
    """
    from ray.llm._internal.serve.engines.sglang.sglang_models import SGLangEngineConfig
    from ray.llm._internal.serve.serving_patterns.sglang.sglang_wideep_server import (
        SGLangWideEPServer,
    )

    # Valid config: moe_a2a_backend='deepep', moe_dp_size=1
    valid_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={
            "moe_a2a_backend": "deepep",
            "tp_size": 8,
            "nnodes": 2,
        },
    )
    engine_config = SGLangEngineConfig.from_llm_config(valid_config)
    engine_config.validate_wideep_config()  # Should not raise

    # Invalid: moe_a2a_backend != 'deepep'
    invalid_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={
            "moe_a2a_backend": "none",
            "tp_size": 8,
        },
    )
    deployment_options = SGLangWideEPServer.get_deployment_options(invalid_config)
    # get_deployment_options should work, but __init__ should validate
    assert deployment_options["gang_scheduling_config"].gang_size == 1

    # Invalid: moe_dp_size != 1 without DPA
    invalid_dp_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={
            "moe_a2a_backend": "deepep",
            "tp_size": 8,
            "moe_dp_size": 2,  # Invalid for pure WideEP
        },
    )
    engine_config = SGLangEngineConfig.from_llm_config(invalid_dp_config)
    with pytest.raises(ValueError, match="moe_dp_size=1"):
        engine_config.validate_wideep_config()

    # Valid: moe_dp_size != 1 with DPA enabled
    valid_dpa_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={
            "moe_a2a_backend": "deepep",
            "tp_size": 16,
            "dp_size": 8,
            "enable_dp_attention": True,  # DPA + WideEP
            "moe_dp_size": 1,
        },
    )
    engine_config = SGLangEngineConfig.from_llm_config(valid_dpa_config)
    engine_config.validate_wideep_config()  # Should not raise


def test_sglang_wideep_deployment_options():
    """Verify SGLangWideEPServer.get_deployment_options returns correct values.

    - gang_size=1 (single Engine per gang)
    - Placement group bundles based on tp_size/pp_size/nnodes
    - SPREAD strategy for multi-node, PACK for single-node
    """
    from ray.llm._internal.serve.serving_patterns.sglang.sglang_wideep_server import (
        SGLangWideEPServer,
    )
    from ray.serve.config import GangPlacementStrategy

    # Single-node WideEP
    single_node_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={
            "moe_a2a_backend": "deepep",
            "tp_size": 8,
            "nnodes": 1,
        },
    )
    options = SGLangWideEPServer.get_deployment_options(single_node_config)
    assert options["gang_scheduling_config"].gang_size == 1
    assert len(options["placement_group_bundles"]) == 8
    assert options["placement_group_strategy"] == "PACK"
    assert (
        options["gang_scheduling_config"].gang_placement_strategy
        == GangPlacementStrategy.PACK
    )

    # Multi-node WideEP
    multi_node_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={
            "moe_a2a_backend": "deepep",
            "tp_size": 16,
            "nnodes": 2,
        },
    )
    options = SGLangWideEPServer.get_deployment_options(multi_node_config)
    assert options["gang_scheduling_config"].gang_size == 1
    assert len(options["placement_group_bundles"]) == 2
    assert options["placement_group_bundles"][0] == {"GPU": 8}
    assert options["placement_group_strategy"] == "SPREAD"
    assert (
        options["gang_scheduling_config"].gang_placement_strategy
        == GangPlacementStrategy.SPREAD
    )


@pytest.mark.skip(reason="Requires SGLang WideEP support and 2 GPUs")
def test_sglang_wideep_gang_scheduling(cleanup_wideep):
    """Verify WideEP deployment uses gang scheduling with gang_size=1.

    This test requires at least 2 GPUs and SGLang with WideEP support to run.
    Skipped by default until WideEP support is fully integrated.
    """
    from ray.llm._internal.serve.serving_patterns.sglang import (
        build_sglang_wideep_deployment,
    )

    llm_config = LLMConfig(
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
            "model_path": MODEL_ID,
            "moe_a2a_backend": "deepep",
            "tp_size": 2,
            "nnodes": 1,
            "mem_fraction_static": 0.8,
        },
    )

    app = build_sglang_wideep_deployment(llm_config)
    serve.run(app, blocking=False)

    wait_for_condition(_app_is_running, timeout=300)

    # Verify deployment has gang scheduling enabled
    status = serve.status()
    deployment_name = f"SGLangWideEPServer:{RAY_MODEL_ID}"
    assert deployment_name in status.applications[SERVE_DEFAULT_APP_NAME].deployments

    client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")
    chat_resp = client.chat.completions.create(
        model=RAY_MODEL_ID,
        messages=[{"role": "user", "content": "What is the capital of France?"}],
        max_tokens=64,
        temperature=0.0,
    )
    assert chat_resp.choices[0].message.content.strip()


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
