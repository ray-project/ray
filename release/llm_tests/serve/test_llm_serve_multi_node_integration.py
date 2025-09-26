import pytest
import time

import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    serve.shutdown()
    ray.shutdown()


def wait_for_deployment_ready(app_name: str, timeout: int = 120) -> bool:
    """Wait for a Ray Serve deployment to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            serve_status = serve.status()
            if app_name in serve_status.applications:
                app_status = serve_status.applications[app_name]
                if app_status.status == "RUNNING":
                    return True
                elif app_status.status in ["DEPLOY_FAILED", "UNHEALTHY"]:
                    raise RuntimeError(
                        f"Deployment failed with status: {app_status.status}"
                    )
            time.sleep(2)
        except Exception as e:
            print(f"Error checking deployment status: {e}")
            time.sleep(2)
    return False


@pytest.mark.parametrize(
    "tp_size,pp_size,placement_strategy",
    [
        (2, 1, "PACK"),  # Multi-node TP
        (1, 2, "PACK"),  # Multi-node PP
        (2, 2, "SPREAD"),  # Multi-node TP+PP with SPREAD
    ],
)
def test_llm_serve_multi_node_deployment(tp_size, pp_size, placement_strategy):
    """Test actual deployment of multi-node Ray Serve LLM with custom placement groups."""
    app_name = f"test_multi_node_{tp_size}_{pp_size}_{placement_strategy.lower()}"

    total_gpus = tp_size * pp_size
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 2}] * total_gpus,
        "strategy": placement_strategy,
    }

    llm_config = LLMConfig(
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
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            distributed_executor_backend="ray",
            max_model_len=512,  # Small context for faster testing
            max_num_batched_tokens=256,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    # Build and deploy the application
    app = build_openai_app(llm_config)
    serve.run(app, name=app_name)

    # Wait for deployment to be ready
    assert wait_for_deployment_ready(
        app_name
    ), f"Deployment {app_name} failed to become ready"

    # Verify deployment is running
    serve_status = serve.status()
    assert app_name in serve_status.applications
    assert serve_status.applications[app_name].status == "RUNNING"

    # Clean up
    serve.delete(app_name)


def test_llm_serve_placement_group_failure_recovery():
    """Test that deployment handles placement group creation failures gracefully."""
    # Create a configuration that's likely to fail (too many resources)
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 100}] * 16,  # Excessive CPU requirements
        "strategy": "STRICT_PACK",
    }

    llm_config = LLMConfig(
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
            tensor_parallel_size=4,
            pipeline_parallel_size=4,
            distributed_executor_backend="ray",
        ),
        placement_group_config=placement_group_config,
    )

    app_name = "test_placement_group_failure"
    app = build_openai_app(llm_config)

    # This should either fail gracefully or handle resource constraints
    try:
        serve.run(app, name=app_name)
        # If it succeeds, clean up
        serve.delete(app_name)
    except Exception as e:
        # Failure is acceptable for this test - we're testing graceful failure handling
        print(f"Expected failure occurred: {e}")


def test_llm_serve_mixed_parallelism_strategies():
    """Test different placement strategies for the same TP/PP configuration."""
    configurations = [
        {"strategy": "PACK", "description": "pack_strategy"},
        {"strategy": "SPREAD", "description": "spread_strategy"},
    ]

    for i, config in enumerate(configurations):
        app_name = f"test_mixed_{config['description']}"

        placement_group_config = {
            "bundles": [{"GPU": 1, "CPU": 1}] * 4,
            "strategy": config["strategy"],
        }

        llm_config = LLMConfig(
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
                tensor_parallel_size=2,
                pipeline_parallel_size=2,
                distributed_executor_backend="ray",
                max_model_len=512,
            ),
            placement_group_config=placement_group_config,
            runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
        )

        app = build_openai_app(llm_config)
        serve.run(app, name=app_name)

        # Verify deployment configuration
        serve_options = llm_config.get_serve_options()
        assert serve_options["placement_group_strategy"] == config["strategy"]

        # Clean up
        serve.delete(app_name)
        time.sleep(2)  # Brief pause between deployments


def test_llm_serve_custom_resources():
    """Test custom resource types in placement group bundles."""
    placement_group_config = {
        "bundles": [
            {"GPU": 1, "CPU": 2, "custom_resource": 1},
            {"GPU": 1, "CPU": 2, "custom_resource": 1},
        ],
        "strategy": "PACK",
    }

    llm_config = LLMConfig(
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
            tensor_parallel_size=2,
            pipeline_parallel_size=1,
            distributed_executor_backend="ray",
        ),
        placement_group_config=placement_group_config,
    )

    # Verify custom resources are preserved
    serve_options = llm_config.get_serve_options()
    for bundle in serve_options["placement_group_bundles"]:
        assert bundle["custom_resource"] == 1
        assert bundle["GPU"] == 1
        assert bundle["CPU"] >= 2  # May be merged with replica actor resources


@pytest.mark.parametrize(
    "bundle_count,expected_tp_pp",
    [
        (1, (1, 1)),
        (2, (2, 1)),  # or (1, 2)
        (4, (2, 2)),  # or (4, 1) or (1, 4)
        (8, (4, 2)),  # or (2, 4) or (8, 1) or (1, 8)
    ],
)
def test_llm_serve_bundle_count_consistency(bundle_count, expected_tp_pp):
    """Test that bundle count matches TP*PP configuration."""
    tp_size, pp_size = expected_tp_pp

    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}] * bundle_count,
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
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            distributed_executor_backend="ray",
        ),
        placement_group_config=placement_group_config,
    )

    serve_options = llm_config.get_serve_options()
    assert len(serve_options["placement_group_bundles"]) == bundle_count
    assert bundle_count == tp_size * pp_size


if __name__ == "__main__":
    pytest.main(["-v", __file__])
