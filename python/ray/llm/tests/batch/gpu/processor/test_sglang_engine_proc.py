"""This test suite does not need sglang to be installed."""
import sys

import pytest

import ray
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.sglang_engine_proc import (
    SGLangEngineProcessorConfig,
)


def test_sglang_engine_processor(gpu_type, model_llama_3_2_216M):
    config = SGLangEngineProcessorConfig(
        model_source=model_llama_3_2_216M,
        engine_kwargs=dict(
            context_length=8192,
            tp_size=2,
            dp_size=2,
            disable_cuda_graph=True,
            dtype="half",  # Older GPUs (e.g. T4) don't support bfloat16
        ),
        runtime_env=dict(
            env_vars=dict(
                RANDOM_ENV_VAR="12345",
            ),
        ),
        accelerator_type=gpu_type,
        concurrency=4,
        batch_size=64,
        max_concurrent_batches=4,
        max_pending_requests=111,
        apply_chat_template=True,
        tokenize=True,
        detokenize=True,
    )
    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "ChatTemplateStage",
        "TokenizeStage",
        "SGLangEngineStage",
        "DetokenizeStage",
    ]

    stage = processor.get_stage_by_name("SGLangEngineStage")
    assert stage.fn_constructor_kwargs == {
        "model": model_llama_3_2_216M,
        "engine_kwargs": {
            "context_length": 8192,
            "tp_size": 2,
            "dp_size": 2,
            "disable_cuda_graph": True,
            "dtype": "half",
        },
        "task_type": "generate",
        "max_pending_requests": 111,
    }

    runtime_env = stage.map_batches_kwargs.pop("runtime_env")
    assert "env_vars" in runtime_env
    assert runtime_env["env_vars"]["RANDOM_ENV_VAR"] == "12345"
    compute = stage.map_batches_kwargs.pop("compute")
    assert isinstance(compute, ray.data._internal.compute.ActorPoolStrategy)
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 4,  # Based on tp_size=2, dp_size=2 in engine_kwargs
        "resources": None,
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
