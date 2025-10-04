import pytest

import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig
from ray.llm._internal.serve.configs.server_models import LLMServingArgs


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.mark.parametrize(
    "tp_size,pp_size",
    [
        (2, 4),  # TP×PP=8 > 4 GPUs/node, FORCES cross-node placement
        (4, 2),  # TP×PP=8 > 4 GPUs/node, FORCES cross-node placement
    ],
)
def test_llm_serve_multi_node(tp_size, pp_size):
    """Test multi-node Ray Serve LLM deployment with custom placement groups.

    Cluster: 2 nodes × 4 GPUs = 8 total GPUs
    TP×PP=8 exceeds per-node capacity, forcing cross-node deployment.
    """
    total_gpus = tp_size * pp_size
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 2}] * total_gpus,
        "strategy": "PACK",
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="opt-1.3b",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            distributed_executor_backend="ray",
            max_model_len=512,
            max_num_batched_tokens=256,
            enforce_eager=True,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    app = build_openai_app(llm_serving_args=LLMServingArgs(llm_configs=[llm_config]))
    serve.run(app, blocking=False)

    # Basic deployment validation - serve.run will raise if deployment fails
    # Cleanup handled by autouse fixture


def test_llm_serve_data_parallelism():
    """Test Data Parallelism with STRICT_PACK override.

    When data_parallel_size > 1, the system should override user-provided
    strategy with STRICT_PACK to ensure each replica is co-located on one node.
    """
    # User provides SPREAD strategy (will be overridden to STRICT_PACK for DP)
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 2}],
        "strategy": "SPREAD",  # Will be overridden to STRICT_PACK
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="opt-1.3b",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=2,  # Data parallel size = 2
                max_replicas=2,
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            distributed_executor_backend="ray",
            max_model_len=512,
            enforce_eager=True,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    app = build_openai_app(llm_serving_args=LLMServingArgs(llm_configs=[llm_config]))
    serve.run(app, blocking=False)

    # Basic deployment validation - serve.run will raise if deployment fails
    # Cleanup handled by autouse fixture


if __name__ == "__main__":
    pytest.main(["-v", __file__])
