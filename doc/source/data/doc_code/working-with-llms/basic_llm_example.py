"""
This file serves as a documentation example and CI test for basic LLM batch inference.

"""

# Dependency setup
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "ray[llm]"])
subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "--upgrade", "transformers"]
)
subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy==1.26.4"])


# __basic_llm_example_start__
import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_processor

# __basic_config_example_start__
# Basic vLLM configuration
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,  # Reduce if CUDA OOM occurs
        "max_model_len": 4096,  # Constrain to fit test GPU memory
    },
    concurrency=1,
    batch_size=64,
)
# __basic_config_example_end__

processor = build_processor(
    config,
    preprocess=lambda row: dict(
        messages=[
            {"role": "system", "content": "You are a bot that responds with haikus."},
            {"role": "user", "content": row["item"]},
        ],
        sampling_params=dict(
            temperature=0.3,
            max_tokens=250,
        ),
    ),
    postprocess=lambda row: dict(
        answer=row["generated_text"],
        **row,  # This will return all the original columns in the dataset.
    ),
)

ds = ray.data.from_items(["Start of the haiku is: Complete this for me..."])

if __name__ == "__main__":
    try:
        import torch

        if torch.cuda.is_available():
            ds = processor(ds)
            ds.show(limit=1)
        else:
            print("Skipping basic LLM run (no GPU available)")
    except Exception as e:
        print(f"Skipping basic LLM run due to environment error: {e}")

# __hf_token_config_example_start__
# Configuration with Hugging Face token
config_with_token = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    runtime_env={"env_vars": {"HF_TOKEN": "your_huggingface_token"}},
    concurrency=1,
    batch_size=64,
)
# __hf_token_config_example_end__

# __parallel_config_example_start__
# Model parallelism configuration for larger models
# tensor_parallel_size=2: Split model across 2 GPUs for tensor parallelism
# pipeline_parallel_size=2: Use 2 pipeline stages (total 4 GPUs needed)
# Total GPUs required = tensor_parallel_size * pipeline_parallel_size = 4
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "max_model_len": 16384,
        "tensor_parallel_size": 2,
        "pipeline_parallel_size": 2,
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 2048,
    },
    concurrency=1,
    batch_size=32,
    accelerator_type="L4",
)
# __parallel_config_example_end__

# __runai_config_example_start__
# RunAI streamer configuration for optimized model loading
# Note: Install vLLM with runai dependencies: pip install -U "vllm[runai]>=0.10.1"
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "load_format": "runai_streamer",
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)
# __runai_config_example_end__

# __lora_config_example_start__
# Multi-LoRA configuration
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_lora": True,
        "max_lora_rank": 32,
        "max_loras": 1,
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=32,
)
# __lora_config_example_end__

# __s3_config_example_start__
# S3 hosted model configuration
s3_config = vLLMEngineProcessorConfig(
    model_source="s3://your-bucket/your-model-path/",
    engine_kwargs={
        "load_format": "runai_streamer",
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)
# __s3_config_example_end__

# __gpu_memory_config_example_start__
# GPU memory management configuration
# If you encounter CUDA out of memory errors, try these optimizations:
config_memory_optimized = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "max_model_len": 8192,
        "max_num_batched_tokens": 2048,
        "enable_chunked_prefill": True,
        "gpu_memory_utilization": 0.85,
        "block_size": 16,
    },
    concurrency=1,
    batch_size=16,
)

# For very large models or limited GPU memory:
config_minimal_memory = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "max_model_len": 4096,
        "max_num_batched_tokens": 1024,
        "enable_chunked_prefill": True,
        "gpu_memory_utilization": 0.75,
    },
    concurrency=1,
    batch_size=8,
)
# __gpu_memory_config_example_end__

# __embedding_config_example_start__
# Embedding model configuration
embedding_config = vLLMEngineProcessorConfig(
    model_source="sentence-transformers/all-MiniLM-L6-v2",
    task_type="embed",
    engine_kwargs=dict(
        enable_prefix_caching=False,
        enable_chunked_prefill=False,
        max_model_len=256,
        enforce_eager=True,
    ),
    batch_size=32,
    concurrency=1,
    apply_chat_template=False,
    detokenize=False,
)

# Example usage for embeddings
def create_embedding_processor():
    return build_processor(
        embedding_config,
        preprocess=lambda row: dict(prompt=row["text"]),
        postprocess=lambda row: {
            "text": row["prompt"],
            "embedding": row["embeddings"],
        },
    )


# __embedding_config_example_end__

# __classification_config_example_start__
# Sequence classification model configuration
# Use task_type="classify" for classification models (e.g., sentiment, quality scoring)
# Use task_type="score" for cross-encoder scoring models
classification_config = vLLMEngineProcessorConfig(
    model_source="nvidia/nemocurator-fineweb-nemotron-4-edu-classifier",
    task_type="classify",
    engine_kwargs=dict(
        max_model_len=512,
        enforce_eager=True,
    ),
    batch_size=8,
    concurrency=1,
    apply_chat_template=False,
    detokenize=False,
)


# Example usage for classification
def create_classification_processor():
    return build_processor(
        classification_config,
        preprocess=lambda row: dict(prompt=row["text"]),
        postprocess=lambda row: {
            "text": row["prompt"],
            # Classification models return logits in the 'embeddings' field
            "score": float(row["embeddings"][0])
            if row.get("embeddings") is not None and len(row["embeddings"]) > 0
            else None,
        },
    )


# __classification_config_example_end__

# __shared_vllm_engine_config_example_start__
import ray
from ray import serve
from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
from ray.serve.llm import (
    LLMConfig,
    ModelLoadingConfig,
    build_llm_deployment,
)
from ray.serve.llm.openai_api_models import CompletionRequest

llm_config = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id="facebook/opt-1.3b",
        model_source="facebook/opt-1.3b",
    ),
    deployment_config=dict(
        name="demo_deployment_config",
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        ),
    ),
    engine_kwargs=dict(
        enable_prefix_caching=True,
        enable_chunked_prefill=True,
        max_num_batched_tokens=4096,
    ),
)

APP_NAME = "demo_app"
DEPLOYMENT_NAME = "demo_deployment"
override_serve_options = dict(name=DEPLOYMENT_NAME)

llm_app = build_llm_deployment(
    llm_config, override_serve_options=override_serve_options
)
app = serve.run(llm_app, name=APP_NAME)
config = ServeDeploymentProcessorConfig(
    deployment_name=DEPLOYMENT_NAME,
    app_name=APP_NAME,
    dtype_mapping={
        "CompletionRequest": CompletionRequest,
    },
    concurrency=1,
    batch_size=64,
)

processor1 = build_processor(
    config,
    preprocess=lambda row: dict(
        method="completions",
        dtype="CompletionRequest",
        request_kwargs=dict(
            model="facebook/opt-1.3b",
            prompt=f"This is a prompt for {row['id']}",
            stream=False,
        ),
    ),
    postprocess=lambda row: dict(
        prompt=row["choices"][0]["text"],
    ),
)

processor2 = build_processor(
    config,
    preprocess=lambda row: dict(
        method="completions",
        dtype="CompletionRequest",
        request_kwargs=dict(
            model="facebook/opt-1.3b",
            prompt=row["prompt"],
            stream=False,
        ),
    ),
    postprocess=lambda row: row,
)

ds = ray.data.range(10)
ds = processor2(processor1(ds))
print(ds.take_all())
# __shared_vllm_engine_config_example_end__

# __cross_node_parallelism_config_example_start__
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_model_len": 16384,
        "pipeline_parallel_size": 4,
        "tensor_parallel_size": 4,
        "distributed_executor_backend": "ray",
    },
    batch_size=32,
    concurrency=1,
)
# __cross_node_parallelism_config_example_end__

# __custom_placement_group_strategy_config_example_start__
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_model_len": 16384,
        "pipeline_parallel_size": 2,
        "tensor_parallel_size": 2,
        "distributed_executor_backend": "ray",
    },
    batch_size=32,
    concurrency=1,
    placement_group_config={
        "bundles": [{"GPU": 1}] * 4,
        "strategy": "STRICT_PACK",
    },
)
# __custom_placement_group_strategy_config_example_end__

# __concurrent_config_example_start__
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_model_len": 16384,
    },
    concurrency=10,
    batch_size=64,
)
# __concurrent_config_example_end__
# __basic_llm_example_end__
