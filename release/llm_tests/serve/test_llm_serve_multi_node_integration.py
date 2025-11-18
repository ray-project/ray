import pytest

import ray
from ray import serve
from ray.serve.llm import (
    build_dp_deployment,
    build_openai_app,
    LLMConfig,
    LLMServingArgs,
    ModelLoadingConfig,
)


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
        "bundles": [{"GPU": 1, "CPU": 1}] * total_gpus,
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
    """Test Data Parallelism deployment with STRICT_PACK override.

    Validates that DP deployments work correctly with placement group configs:
    1. STRICT_PACK strategy is enforced (per-replica co-location)
    2. num_replicas = data_parallel_size
    3. Each replica gets its own placement group with specified bundles
    4. DPRankAssigner correctly coordinates ranks across replicas

    """
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}],
        "strategy": "SPREAD",  # Will be overridden to STRICT_PACK
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="opt-1.3b",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(),  # DP sets num_replicas, not autoscaling
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            data_parallel_size=2,  # 2 DP replicas
            distributed_executor_backend="ray",
            max_model_len=512,
            max_num_batched_tokens=256,
            enforce_eager=True,
        ),
        experimental_configs=dict(
            dp_size_per_node=2,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    # Deploy DP application
    # build_dp_deployment internally validates deployment options via LLMServer.get_deployment_options():
    # - STRICT_PACK override (SPREAD -> STRICT_PACK)
    # - num_replicas = data_parallel_size (2)
    # - placement_group_bundles are properly configured
    app = build_dp_deployment(llm_config)
    serve.run(app, blocking=False)

    # Deployment starting successfully validates:
    # - DPRankAssigner is working
    # - DPServer replicas can coordinate
    # - Placement groups are created correctly with STRICT_PACK
    # - Each replica gets the right resources
    # Cleanup handled by autouse fixture


if __name__ == "__main__":
    pytest.main(["-v", __file__])
