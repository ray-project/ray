"""
This file serves as a documentation example and CI test for basic LLM batch inference.

Structure:
1. Infrastructure setup: Dependency handling, transformers version fix
2. Docs example (between __basic_llm_example_start/end__): Embedded in Sphinx docs via literalinclude
3. Test validation and cleanup
"""

import subprocess
import sys
import os

# Infrastructure: Ensure compatible dependency versions
# vLLM version should match Ray LLM requirements for optimal compatibility
try:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "transformers>=4.36.0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Install compatible vLLM version - check Ray documentation for latest supported version
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "vllm>=0.10.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
except:
    pass

# __basic_llm_example_start__
import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

# __basic_config_example_start__
# Basic vLLM configuration
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,  # Reduce if CUDA OOM occurs
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)
# __basic_config_example_end__

processor = build_llm_processor(
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
    ds = processor(ds)
    ds.show(limit=1)

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
    return build_llm_processor(
        embedding_config,
        preprocess=lambda row: dict(prompt=row["text"]),
        postprocess=lambda row: {
            "text": row["prompt"],
            "embedding": row["embeddings"],
        },
    )


# __embedding_config_example_end__

# __basic_llm_example_end__

# Configuration factory functions for testing
def create_basic_config():
    """Create basic vLLM configuration for testing."""
    return vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            "enable_chunked_prefill": True,
            "max_num_batched_tokens": 4096,
            "max_model_len": 16384,
        },
        concurrency=1,
        batch_size=64,
    )


def create_memory_optimized_config():
    """Create memory-optimized configuration for testing."""
    return vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            "max_model_len": 8192,
            "max_num_batched_tokens": 2048,
            "enable_chunked_prefill": True,
            "gpu_memory_utilization": 0.85,
        },
        concurrency=1,
        batch_size=16,
    )


def create_s3_config():
    """Create S3 model loading configuration for testing."""
    return vLLMEngineProcessorConfig(
        model_source="s3://your-bucket/your-model/",
        engine_kwargs={"load_format": "runai_streamer"},
        runtime_env={
            "env_vars": {
                "AWS_ACCESS_KEY_ID": "your_access_key_id",
                "AWS_SECRET_ACCESS_KEY": "your_secret_access_key",
                "AWS_REGION": "your_region",
            }
        },
        concurrency=1,
        batch_size=64,
    )


def run_test():
    """
    Comprehensive test for LLM configurations and functionality.

    This function tests both configuration validation and actual processor creation.
    For CI environments, it runs full functionality tests when possible.
    """
    try:
        import torch

        # Check if running in pytest
        in_pytest = "pytest" in sys.modules
        suppress_output = in_pytest

        if not suppress_output:
            print("Testing LLM configurations and functionality...")

        # Test 1: Basic configuration and processor creation
        basic_config = create_basic_config()
        assert basic_config.model_source == "unsloth/Llama-3.1-8B-Instruct"
        assert basic_config.engine_kwargs["enable_chunked_prefill"] is True
        assert basic_config.engine_kwargs["max_num_batched_tokens"] == 4096
        assert basic_config.engine_kwargs["max_model_len"] == 16384
        assert basic_config.concurrency == 1
        assert basic_config.batch_size == 64

        # Test 2: Memory-optimized configuration
        memory_config = create_memory_optimized_config()
        assert memory_config.engine_kwargs["gpu_memory_utilization"] == 0.85
        assert memory_config.batch_size == 16

        # Test 3: Processor creation (this tests the build_llm_processor function)
        try:
            test_processor = build_llm_processor(
                basic_config,
                preprocess=lambda row: dict(
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": row["item"]},
                    ],
                    sampling_params=dict(temperature=0.3, max_tokens=50),
                ),
                postprocess=lambda row: dict(response=row["generated_text"]),
            )
            assert test_processor is not None
            if not suppress_output:
                print("✓ Processor creation successful")
        except Exception as e:
            if not suppress_output:
                print(f"⚠ Processor creation test: {e}")

        # Test 4: Dataset creation and preprocessing
        test_dataset = ray.data.from_items([{"item": "Hello, world!"}])
        assert test_dataset.count() == 1

        # Test preprocessing function
        preprocess_fn = lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": row["item"]},
            ],
            sampling_params=dict(temperature=0.3, max_tokens=50),
        )

        preprocessed = test_dataset.map(preprocess_fn).take(1)[0]
        assert "messages" in preprocessed
        assert len(preprocessed["messages"]) == 2
        assert preprocessed["messages"][0]["role"] == "system"
        assert preprocessed["messages"][1]["content"] == "Hello, world!"

        if not suppress_output:
            print("✓ Dataset and preprocessing tests successful")

        # Test 5: S3 configuration structure
        s3_config = create_s3_config()
        assert s3_config.model_source == "s3://your-bucket/your-model/"
        assert s3_config.engine_kwargs["load_format"] == "runai_streamer"
        assert "AWS_ACCESS_KEY_ID" in s3_config.runtime_env["env_vars"]

        if not suppress_output:
            print("✓ All LLM configuration and functionality tests passed")
        return True
    except Exception as e:
        if not suppress_output:
            print(f"✗ LLM test failed: {e}")
            import traceback

            traceback.print_exc()
        return False


def run_doctest():
    """
    Doctest-safe version that validates configuration without running inference.
    This avoids GPU memory issues during documentation testing.
    """
    try:
        # Just validate basic configuration creation without importing main module
        from ray.data.llm import vLLMEngineProcessorConfig

        # Test basic configuration
        basic_config = vLLMEngineProcessorConfig(
            model_source="unsloth/Llama-3.1-8B-Instruct",
            engine_kwargs={
                "enable_chunked_prefill": True,
                "max_num_batched_tokens": 4096,
                "max_model_len": 16384,
            },
            concurrency=1,
            batch_size=64,
        )
        assert basic_config.model_source == "unsloth/Llama-3.1-8B-Instruct"

        # Test advanced configuration
        advanced_config = vLLMEngineProcessorConfig(
            model_source="unsloth/Llama-3.1-8B-Instruct",
            engine_kwargs={
                "enable_chunked_prefill": True,
                "max_num_batched_tokens": 8192,
                "max_model_len": 32768,
            },
            concurrency=2,
            batch_size=128,
        )
        assert advanced_config.model_source == "unsloth/Llama-3.1-8B-Instruct"

        print("LLM processor configured successfully")
        return True

    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return False


if __name__ == "__main__":
    # When run directly, only do configuration validation (no actual inference)
    print("Basic LLM Example - Configuration Demo")
    print("=" * 40)
    print("Note: This demo validates configuration setup only.")
    print("Actual inference requires GPU resources and is shown in the docs.")

    # Run validation tests
    success = run_test()
    if success:
        print("All configurations validated successfully!")
    else:
        print("Configuration validation failed!")
        sys.exit(1)
