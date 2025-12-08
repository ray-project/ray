import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import (
    build_pd_openai_app,
    LLMConfig,
    ModelLoadingConfig,
)
from ray.serve.schema import ApplicationStatus


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def test_llm_serve_prefill_decode_with_data_parallelism():
    """Test Prefill-Decode disaggregation with Data Parallelism and Expert Parallelism.

    Cluster: 2 nodes x 4 GPUs = 8 GPUs total
    - Prefill: DP=4 (scheduled on node with "prefill" custom resource)
    - Decode: DP=4 (scheduled on node with "decode" custom resource)

    TODO(seiji): remove custom resources on worker nodes when
    https://github.com/ray-project/ray/issues/59150 is closed.
    """
    model_loading_config = ModelLoadingConfig(
        model_id="deepseek",
        model_source="deepseek-ai/DeepSeek-V2-Lite",
    )
    base_engine_kwargs = {
        "tensor_parallel_size": 1,
        "enable_expert_parallel": True,
        "load_format": "dummy",
        "max_model_len": 512,
        "max_num_batched_tokens": 256,
        "enforce_eager": True,
    }

    prefill_config = LLMConfig(
        model_loading_config=model_loading_config,
        engine_kwargs={
            **base_engine_kwargs,
            "data_parallel_size": 4,
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            },
        },
        experimental_configs={
            "dp_size_per_node": 4,
        },
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
        deployment_config={
            "ray_actor_options": {"resources": {"prefill": 1}},
        },
    )

    decode_config = LLMConfig(
        model_loading_config=model_loading_config,
        engine_kwargs={
            **base_engine_kwargs,
            "data_parallel_size": 4,
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            },
        },
        experimental_configs={
            "dp_size_per_node": 4,
        },
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
        deployment_config={
            "ray_actor_options": {"resources": {"decode": 1}},
        },
    )

    # build_pd_openai_app auto-detects DP and uses build_dp_deployment
    app = build_pd_openai_app(
        {
            "prefill_config": prefill_config,
            "decode_config": decode_config,
        }
    )
    serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=300)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
