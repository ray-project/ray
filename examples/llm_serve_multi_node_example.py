#!/usr/bin/env python
"""
Example: Multi-Node Ray Serve LLM Deployment with Custom Placement Groups

This example demonstrates how to deploy an LLM using Ray Serve with custom
placement group configurations for multi-node tensor and pipeline parallelism.
"""

import ray
from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app


def example_multi_node_deployment():
    """Deploy an LLM with multi-node TP and PP using custom placement groups."""

    # Initialize Ray
    ray.init()

    # Example 1: Multi-node deployment with PACK strategy
    print("Example 1: Multi-node deployment with PACK strategy")
    print("=" * 60)

    llm_config_pack = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llama-7b-pack",
            model_source="facebook/opt-1.3b",  # Using smaller model for demo
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
            max_model_len=2048,
            max_num_batched_tokens=1024,
        ),
        # Custom placement group configuration
        placement_group_config={
            "bundles": [
                {"GPU": 1, "CPU": 2},  # Bundle for TP worker 1, PP stage 1
                {"GPU": 1, "CPU": 2},  # Bundle for TP worker 2, PP stage 1
                {"GPU": 1, "CPU": 2},  # Bundle for TP worker 1, PP stage 2
                {"GPU": 1, "CPU": 2},  # Bundle for TP worker 2, PP stage 2
            ],
            "strategy": "PACK",  # Pack bundles on nodes when possible
        },
        accelerator_type="L4",  # Specify accelerator type
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    # Build and deploy the application
    app_pack = build_openai_app(llm_config_pack)
    serve.run(app_pack, name="llm_pack_example")

    print("Deployment with PACK strategy is running!")
    print("You can test it with:")
    print("curl -X POST http://localhost:8000/v1/completions \\")
    print('  -H "Content-Type: application/json" \\')
    print(
        '  -d \'{"model": "llama-7b-pack", "prompt": "The future of AI is", "max_tokens": 50}\''
    )

    # Clean up
    serve.delete("llm_pack_example")

    # Example 2: Multi-node deployment with SPREAD strategy for fault tolerance
    print("\n\nExample 2: Multi-node deployment with SPREAD strategy")
    print("=" * 60)

    llm_config_spread = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llama-7b-spread",
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
            pipeline_parallel_size=1,  # TP-only for this example
            distributed_executor_backend="ray",
            max_model_len=2048,
        ),
        # SPREAD strategy for maximum fault tolerance
        placement_group_config={
            "bundles": [
                {"GPU": 1, "CPU": 3, "memory": 8000000000},  # 8GB memory per bundle
                {"GPU": 1, "CPU": 3, "memory": 8000000000},
                {"GPU": 1, "CPU": 3, "memory": 8000000000},
                {"GPU": 1, "CPU": 3, "memory": 8000000000},
            ],
            "strategy": "SPREAD",  # Spread across different nodes for fault tolerance
        },
        accelerator_type="A100-80G",
    )

    app_spread = build_openai_app(llm_config_spread)
    serve.run(app_spread, name="llm_spread_example")

    print("Deployment with SPREAD strategy is running!")
    print(
        "This deployment prioritizes fault tolerance by spreading workers across nodes."
    )

    # Clean up
    serve.delete("llm_spread_example")

    # Example 3: Custom resource requirements
    print("\n\nExample 3: Custom resource requirements")
    print("=" * 60)

    llm_config_custom = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llama-7b-custom",
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
        ),
        # Custom placement group with special resources
        placement_group_config={
            "bundles": [
                {
                    "GPU": 1,
                    "CPU": 4,
                    "memory": 16000000000,  # 16GB memory
                    "special_resource": 1,  # Custom resource type
                },
                {
                    "GPU": 1,
                    "CPU": 4,
                    "memory": 16000000000,
                    "special_resource": 1,
                },
                {
                    "GPU": 1,
                    "CPU": 4,
                    "memory": 16000000000,
                    "special_resource": 1,
                },
                {
                    "GPU": 1,
                    "CPU": 4,
                    "memory": 16000000000,
                    "special_resource": 1,
                },
            ],
            "strategy": "PACK",
        },
    )

    app_custom = build_openai_app(llm_config_custom)
    serve.run(app_custom, name="llm_custom_example")

    print("Deployment with custom resources is running!")
    print("This example shows how to specify custom resource requirements.")

    # Clean up
    serve.delete("llm_custom_example")

    # Shutdown Ray
    ray.shutdown()
    print("\nAll examples completed successfully!")


def example_comparison_with_defaults():
    """Compare custom placement groups with default behavior."""

    ray.init()

    print("Comparison: Custom vs Default Placement Groups")
    print("=" * 60)

    # Default behavior (no custom placement group)
    default_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="default-model",
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
        # No placement_group_config specified - will use defaults
    )

    # Get the serve options to see what placement group configuration is used
    default_options = default_config.get_serve_options()
    print("Default placement group configuration:")
    print(
        f"  Strategy: {default_options['placement_group_strategy']}"
    )  # Should be PACK
    print(f"  Number of bundles: {len(default_options['placement_group_bundles'])}")
    print(f"  Bundle configuration: {default_options['placement_group_bundles'][0]}")

    # Custom configuration
    custom_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="custom-model",
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
        placement_group_config={
            "bundles": [{"GPU": 1, "CPU": 4, "memory": 12000000000}] * 4,
            "strategy": "SPREAD",
        },
    )

    custom_options = custom_config.get_serve_options()
    print("\nCustom placement group configuration:")
    print(f"  Strategy: {custom_options['placement_group_strategy']}")
    print(f"  Number of bundles: {len(custom_options['placement_group_bundles'])}")
    print(f"  Bundle configuration: {custom_options['placement_group_bundles'][0]}")

    ray.shutdown()


if __name__ == "__main__":
    print("Ray Serve Multi-Node LLM Deployment Examples")
    print("=" * 80)

    # Run the main examples
    example_multi_node_deployment()

    print("\n" + "=" * 80)

    # Run the comparison example
    example_comparison_with_defaults()

    print("\nFor more advanced usage, see:")
    print("- release/llm_tests/serve/test_llm_serve_multi_node.py")
    print("- release/llm_tests/serve/benchmark/benchmark_multi_node_serve.py")
