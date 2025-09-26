import pytest
from typing import Dict, Any

import ray
from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    serve.shutdown()
    ray.shutdown()


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
        "bundles": [{"GPU": 1, "CPU": 2}] * total_gpus,
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
    serve_options = llm_config.get_serve_options()
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

    serve_options = llm_config.get_serve_options()
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
        llm_config.get_serve_options()

    # Test missing strategy (should default to PACK, not fail)
    llm_config = get_llm_config_with_placement_group(
        placement_group_config={"bundles": [{"GPU": 1}]}
    )
    serve_options = llm_config.get_serve_options()
    assert serve_options["placement_group_strategy"] == "PACK"

    # Test multiple GPUs per bundle (should now be allowed)
    llm_config = get_llm_config_with_placement_group(
        placement_group_config={"bundles": [{"GPU": 2, "CPU": 1}], "strategy": "PACK"}
    )
    serve_options = llm_config.get_serve_options()
    assert serve_options["placement_group_bundles"][0]["GPU"] == 2


def test_llm_serve_accelerator_type_integration():
    """Test that accelerator types are properly integrated with custom placement groups."""
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}] * 4,
        "strategy": "PACK",
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="test_model",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=2,
            distributed_executor_backend="ray",
        ),
        accelerator_type="L4",
        placement_group_config=placement_group_config,
    )

    serve_options = llm_config.get_serve_options()

    # Check that accelerator type is added to bundles
    for bundle in serve_options["placement_group_bundles"]:
        assert "accelerator_type:L4" in bundle
        assert bundle["accelerator_type:L4"] == 0.001


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
    """Test that the correct number of bundles are created for different TP/PP configurations."""
    llm_config = get_llm_config_with_placement_group(
        tensor_parallel_size=tp_size,
        pipeline_parallel_size=pp_size,
    )

    serve_options = llm_config.get_serve_options()
    assert len(serve_options["placement_group_bundles"]) == expected_bundles


def test_llm_serve_resource_merging():
    """Test that replica actor resources are properly merged with placement group bundles."""
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
        placement_group_config=placement_group_config,
    )

    serve_options = llm_config.get_serve_options()

    # First bundle should have merged resources (replica + child)
    first_bundle = serve_options["placement_group_bundles"][0]
    assert first_bundle["CPU"] >= 2  # At least the replica actor resources
    assert first_bundle["GPU"] >= 1
    assert "memory" in first_bundle

    # Other bundles should have the original configuration
    for bundle in serve_options["placement_group_bundles"][1:]:
        assert bundle["CPU"] == 1
        assert bundle["GPU"] == 1


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
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=1,
            data_parallel_size=2,  # Enable data parallelism
            distributed_executor_backend="ray",
        ),
        placement_group_config=placement_group_config,
    )

    serve_options = llm_config.get_serve_options()

    # Data parallel should override to STRICT_PACK
    assert serve_options["placement_group_strategy"] == "STRICT_PACK"
    assert serve_options["num_replicas"] == 2


if __name__ == "__main__":
    pytest.main(["-v", __file__])
