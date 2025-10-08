from typing import Any, Dict

import pytest

from ray.llm._internal.serve.deployments.data_parallel.dp_server import DPServer
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.serve.llm import LLMConfig, ModelLoadingConfig


def get_llm_config_with_placement_group(
    tensor_parallel_size: int = 1,
    pipeline_parallel_size: int = 1,
    placement_group_config: Dict[str, Any] = None,
) -> LLMConfig:
    """Create LLMConfig with specified parallelism parameters and placement group config."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="test_model",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tensor_parallel_size,
            pipeline_parallel_size=pipeline_parallel_size,
            distributed_executor_backend="ray",
        ),
        placement_group_config=placement_group_config,
        runtime_env=None,
    )


@pytest.mark.parametrize(
    "tp_size,pp_size,placement_strategy",
    [
        (2, 4, "PACK"),  # Multi-node PP+TP with PACK
        (4, 2, "PACK"),  # Multi-node PP+TP with PACK
        (8, 1, "SPREAD"),  # Multi-node TP with SPREAD
        (1, 8, "SPREAD"),  # Multi-node PP with SPREAD
    ],
)
def test_llm_serve_custom_placement_group(tp_size, pp_size, placement_strategy):
    """Test Ray Serve LLM with custom placement group configurations."""
    total_gpus = tp_size * pp_size

    # Create custom placement group configuration
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}] * total_gpus,
        "strategy": placement_strategy,
    }

    llm_config = get_llm_config_with_placement_group(
        tensor_parallel_size=tp_size,
        pipeline_parallel_size=pp_size,
        placement_group_config=placement_group_config,
    )

    # Verify the configuration is properly set
    assert llm_config.placement_group_config == placement_group_config
    assert llm_config.engine_kwargs["tensor_parallel_size"] == tp_size
    assert llm_config.engine_kwargs["pipeline_parallel_size"] == pp_size

    # Test that serve options are generated correctly
    serve_options = LLMServer.get_deployment_options(llm_config)
    assert "placement_group_bundles" in serve_options
    assert "placement_group_strategy" in serve_options
    assert serve_options["placement_group_strategy"] == placement_strategy
    assert len(serve_options["placement_group_bundles"]) == total_gpus


@pytest.mark.parametrize(
    "tp_size,pp_size",
    [
        (2, 1),  # TP-only should use PACK by default
        (1, 2),  # PP-only should use PACK by default
        (2, 2),  # TP+PP should use PACK by default
    ],
)
def test_llm_serve_default_placement_strategy(tp_size, pp_size):
    """Test that Ray Serve LLM uses PACK strategy by default for all configurations."""
    llm_config = get_llm_config_with_placement_group(
        tensor_parallel_size=tp_size,
        pipeline_parallel_size=pp_size,
        placement_group_config=None,  # Use defaults
    )

    serve_options = LLMServer.get_deployment_options(llm_config)
    # All configurations should default to PACK strategy
    assert serve_options["placement_group_strategy"] == "PACK"
    assert len(serve_options["placement_group_bundles"]) == tp_size * pp_size


def test_llm_serve_placement_group_validation():
    """Test validation of placement group configurations."""

    # Test missing bundles
    with pytest.raises(
        ValueError, match="placement_group_config must contain 'bundles'"
    ):
        llm_config = get_llm_config_with_placement_group(
            placement_group_config={"strategy": "PACK"}
        )
        LLMServer.get_deployment_options(llm_config)

    # Test missing strategy (should default to PACK, not fail)
    llm_config = get_llm_config_with_placement_group(
        placement_group_config={"bundles": [{"GPU": 1}]}
    )
    serve_options = LLMServer.get_deployment_options(llm_config)
    assert serve_options["placement_group_strategy"] == "PACK"


def test_llm_serve_multi_gpu_per_bundle_passes_through():
    """Test multiple GPUs per bundle pass through Serve validation.

    Serve allows GPU>1 per bundle in placement_group_config. vLLM will enforce
    its own GPU<=1 restriction during engine creation (not tested here).
    This confirms Serve doesn't block it, allowing vLLM to manage its constraints.
    """
    llm_config = get_llm_config_with_placement_group(
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
        placement_group_config={
            "bundles": [{"GPU": 2, "CPU": 4}],
            "strategy": "PACK",
        },
    )

    # Serve should accept and pass through GPU=2 to placement group
    # First bundle gets CPU: 4 (from config) + 1 (replica actor) = 5
    serve_options = LLMServer.get_deployment_options(llm_config)
    assert serve_options["placement_group_bundles"][0]["GPU"] == 2
    assert serve_options["placement_group_bundles"][0]["CPU"] == 5

    # vLLM will reject this during actual engine creation with a validation error
    # (not tested here since this is a config-only CPU test)


@pytest.mark.parametrize(
    "tp_size,pp_size,expected_bundles",
    [
        (1, 1, 1),
        (2, 1, 2),
        (1, 2, 2),
        (2, 2, 4),
        (4, 2, 8),
        (2, 4, 8),
    ],
)
def test_llm_serve_bundle_count(tp_size, pp_size, expected_bundles):
    """Test that correct number of bundles are created for different TP/PP configs."""
    llm_config = get_llm_config_with_placement_group(
        tensor_parallel_size=tp_size,
        pipeline_parallel_size=pp_size,
    )

    serve_options = LLMServer.get_deployment_options(llm_config)
    assert len(serve_options["placement_group_bundles"]) == expected_bundles


def test_llm_serve_accelerator_and_resource_merging():
    """Test accelerator type injection and replica actor resource merging."""
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}] * 2,
        "strategy": "PACK",
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="test_model",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
            ray_actor_options=dict(
                num_cpus=2,
                num_gpus=1,
                memory=1000000000,  # 1GB
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=1,
            distributed_executor_backend="ray",
        ),
        accelerator_type="L4",
        placement_group_config=placement_group_config,
    )

    serve_options = LLMServer.get_deployment_options(llm_config)

    # First bundle: merged replica actor resources
    # CPU: 1 (from bundle) + 2 (from replica actor) = 3
    # GPU: Already 1 in both
    first_bundle = serve_options["placement_group_bundles"][0]
    assert first_bundle["CPU"] == 3
    assert first_bundle["GPU"] == 2  # 1 from bundle + 1 from replica actor
    assert "memory" in first_bundle
    assert "accelerator_type:L4" in first_bundle

    # Tail bundles: original config + accelerator type
    for bundle in serve_options["placement_group_bundles"][1:]:
        assert bundle["CPU"] == 1
        assert bundle["GPU"] == 1
        assert "accelerator_type:L4" in bundle
        assert bundle["accelerator_type:L4"] == 0.001


def test_llm_serve_data_parallel_placement_override():
    """Test that data parallel deployments override placement group strategy to STRICT_PACK."""
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}] * 2,
        "strategy": "SPREAD",  # This should be overridden
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="test_model",
            model_source="facebook/opt-1.3b",
        ),
        # For DP correctness, do not set autoscaling_config; DP size fixes replicas
        deployment_config=dict(),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=1,
            data_parallel_size=2,  # Enable data parallelism
            distributed_executor_backend="ray",
        ),
        placement_group_config=placement_group_config,
    )

    serve_options = DPServer.get_deployment_options(llm_config)

    # Data parallel should override to STRICT_PACK regardless of user-specified strategy
    assert serve_options["placement_group_strategy"] == "STRICT_PACK"
    # Note: num_replicas is set by build_dp_deployment, not by get_deployment_options


if __name__ == "__main__":
    pytest.main(["-v", __file__])
