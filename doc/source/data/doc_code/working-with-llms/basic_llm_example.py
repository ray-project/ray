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

# Infrastructure: Fix transformers version for RoPE compatibility
try:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "transformers>=4.36.0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
except:
    pass

# __basic_llm_example_start__
import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

# __basic_config_example_start__
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
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

if __name__ != "__main__":
    ds = processor(ds)
    ds.show(limit=1)

# __simple_config_example_start__
# Basic vLLM configuration
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)
# __simple_config_example_end__

# __hf_token_config_example_start__
# Configuration with Hugging Face token
config_with_token = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    runtime_env={"env_vars": {"HF_TOKEN": "your_huggingface_token"}},
    concurrency=1,
    batch_size=64,
)
# __hf_token_config_example_end__

# Create sample dataset

# __parallel_config_example_start__
# Model parallelism configuration
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
)
# __parallel_config_example_end__

parallel_config = vLLMEngineProcessorConfig(
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
)

# __runai_config_example_start__
# RunAI streamer configuration
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

runai_config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "load_format": "runai_streamer",
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)

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

lora_config = vLLMEngineProcessorConfig(
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
# __basic_llm_example_end__

# Additional configuration examples for comprehensive testing
def create_basic_config():
    """Create basic vLLM configuration."""
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


def create_parallel_config():
    """Create model parallelism configuration."""
    return vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            "max_model_len": 16384,
            "tensor_parallel_size": 2,
            "pipeline_parallel_size": 2,
            "enable_chunked_prefill": True,
            "max_num_batched_tokens": 2048,
        },
        concurrency=1,
        batch_size=64,
    )


def create_runai_config():
    """Create RunAI streamer configuration."""
    return vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={"load_format": "runai_streamer"},
        concurrency=1,
        batch_size=64,
    )


def create_s3_config():
    """Create S3 model loading configuration."""
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


def create_lora_config():
    """Create multi-LoRA configuration."""
    return vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            "enable_lora": True,
            "max_lora_rank": 32,
            "max_loras": 1,
        },
        concurrency=1,
        batch_size=64,
    )


def run_test():
    """
    Configuration validation test for all LLM configurations.

    This function validates configuration creation and processor setup
    without running actual inference (which requires GPU resources).
    """
    try:
        # Control output during pytest runs
        suppress_output = "pytest" in sys.modules

        if not suppress_output:
            print("Testing all LLM configurations...")

        # Test 1: Basic configuration
        basic_config = create_basic_config()
        assert basic_config.model_source == "unsloth/Llama-3.1-8B-Instruct"
        assert basic_config.engine_kwargs["enable_chunked_prefill"] is True
        assert basic_config.engine_kwargs["max_num_batched_tokens"] == 4096
        assert basic_config.engine_kwargs["max_model_len"] == 16384
        assert basic_config.concurrency == 1
        assert basic_config.batch_size == 64

        # Test 2: Model parallelism configuration
        parallel_config = create_parallel_config()
        assert parallel_config.engine_kwargs["tensor_parallel_size"] == 2
        assert parallel_config.engine_kwargs["pipeline_parallel_size"] == 2
        assert parallel_config.engine_kwargs["enable_chunked_prefill"] is True
        assert parallel_config.engine_kwargs["max_num_batched_tokens"] == 2048

        # Test 3: RunAI streamer configuration
        runai_config = create_runai_config()
        assert runai_config.engine_kwargs["load_format"] == "runai_streamer"
        assert runai_config.model_source == "unsloth/Llama-3.1-8B-Instruct"

        # Test 4: S3 configuration with environment variables
        s3_config = create_s3_config()
        assert s3_config.model_source == "s3://your-bucket/your-model/"
        assert s3_config.engine_kwargs["load_format"] == "runai_streamer"
        assert "AWS_ACCESS_KEY_ID" in s3_config.runtime_env["env_vars"]
        assert "AWS_SECRET_ACCESS_KEY" in s3_config.runtime_env["env_vars"]
        assert "AWS_REGION" in s3_config.runtime_env["env_vars"]

        # Test 5: Multi-LoRA configuration
        lora_config = create_lora_config()
        assert lora_config.engine_kwargs["enable_lora"] is True
        assert lora_config.engine_kwargs["max_lora_rank"] == 32
        assert lora_config.engine_kwargs["max_loras"] == 1

        if not suppress_output:
            print("Basic LLM example validation successful (all configs tested)")
        return True
    except Exception as e:
        if not suppress_output:
            print(f"Basic LLM example validation failed: {e}")
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
