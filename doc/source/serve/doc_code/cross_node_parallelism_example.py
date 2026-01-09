# flake8: noqa
"""
Cross-node parallelism examples for Ray Serve LLM.

TP / PP / custom placement group strategies
for multi-node LLM deployments.
"""

# __cross_node_tp_example_start__
import vllm
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with tensor parallelism across 2 GPUs
# Tensor parallelism splits model weights across GPUs
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    accelerator_type="L4",
    engine_kwargs=dict(
        tensor_parallel_size=2,
        max_model_len=8192,
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __cross_node_tp_example_end__

# __cross_node_pp_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with pipeline parallelism across 2 GPUs
# Pipeline parallelism splits model layers across GPUs
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    accelerator_type="L4",
    engine_kwargs=dict(
        pipeline_parallel_size=2,
        max_model_len=8192,
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __cross_node_pp_example_end__

# __cross_node_tp_pp_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with both tensor and pipeline parallelism
# This example uses 4 GPUs total (2 TP * 2 PP)
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    accelerator_type="L4",
    engine_kwargs=dict(
        tensor_parallel_size=2,
        pipeline_parallel_size=2,
        max_model_len=8192,
        enable_chunked_prefill=True,
        max_num_batched_tokens=4096,
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __cross_node_tp_pp_example_end__

# __custom_placement_group_pack_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with custom placement group using PACK strategy
# PACK tries to place workers on as few nodes as possible for locality
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    accelerator_type="L4",
    engine_kwargs=dict(
        tensor_parallel_size=2,
        max_model_len=8192,
    ),
    placement_group_config=dict(
        bundles=[{"GPU": 1}] * 2,
        strategy="PACK",
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __custom_placement_group_pack_example_end__

# __custom_placement_group_spread_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with custom placement group using SPREAD strategy
# SPREAD distributes workers across nodes for fault tolerance
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    accelerator_type="L4",
    engine_kwargs=dict(
        tensor_parallel_size=4,
        max_model_len=8192,
    ),
    placement_group_config=dict(
        bundles=[{"GPU": 1}] * 4,
        strategy="SPREAD",
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __custom_placement_group_spread_example_end__

# __custom_placement_group_strict_pack_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with custom placement group using STRICT_PACK strategy
# STRICT_PACK ensures all workers are placed on the same node
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3.1-8b",
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    accelerator_type="A100",
    engine_kwargs=dict(
        tensor_parallel_size=2,
        max_model_len=8192,
    ),
    placement_group_config=dict(
        bundles=[{"GPU": 1}] * 2,
        strategy="STRICT_PACK",
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __custom_placement_group_strict_pack_example_end__
