import sys

import pydantic
import pytest

import ray
from ray.data.llm import build_processor, vLLMEngineProcessorConfig
from ray.llm._internal.batch.constants import vLLMTaskType
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    PrepareMultimodalStageConfig,
    TokenizerStageConfig,
)


@pytest.mark.parametrize(
    "tensor_parallel_size, expected_distributed_executor_backend",
    [(1, "uni"), (2, "ray")],
)
def test_vllm_engine_processor(
    gpu_type,
    tensor_parallel_size,
    expected_distributed_executor_backend,
):
    config = vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen3.5-0.8B",
        engine_kwargs=dict(
            max_model_len=8192,
            tensor_parallel_size=tensor_parallel_size,
        ),
        runtime_env=dict(
            env_vars=dict(
                RANDOM_ENV_VAR="12345",
            ),
        ),
        accelerator_type=gpu_type,
        concurrency=4,
        batch_size=64,
        max_pending_requests=111,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        detokenize_stage=DetokenizeStageConfig(enabled=True),
        prepare_multimodal_stage=PrepareMultimodalStageConfig(enabled=True),
    )
    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "PrepareMultimodalStage",
        "ChatTemplateStage",
        "TokenizeStage",
        "vLLMEngineStage",
        "DetokenizeStage",
    ]

    stage = processor.get_stage_by_name("vLLMEngineStage")
    assert stage.fn_constructor_kwargs == {
        "model": "Qwen/Qwen3.5-0.8B",
        "engine_kwargs": {
            "max_model_len": 8192,
            "distributed_executor_backend": expected_distributed_executor_backend,
            "tensor_parallel_size": tensor_parallel_size,
            "task_type": vLLMTaskType.GENERATE,
        },
        "task_type": vLLMTaskType.GENERATE,
        "max_pending_requests": 111,
        "dynamic_lora_loading_path": None,
        "max_concurrent_batches": 8,
        "batch_size": 64,
        "should_continue_on_error": False,
        "log_engine_metrics": True,
    }

    runtime_env = stage.map_batches_kwargs.pop("runtime_env")
    assert "env_vars" in runtime_env
    assert runtime_env["env_vars"]["RANDOM_ENV_VAR"] == "12345"
    compute = stage.map_batches_kwargs.pop("compute")
    assert isinstance(compute, ray.data._internal.compute.ActorPoolStrategy)

    if expected_distributed_executor_backend == "ray":
        ray_remote_args_fn = stage.map_batches_kwargs.pop("ray_remote_args_fn")
        assert ray_remote_args_fn is not None
        assert stage.map_batches_kwargs == {
            "zero_copy_batch": True,
            "max_concurrency": 8,
            "accelerator_type": gpu_type,
            "num_gpus": 0,
        }
    else:
        assert "ray_remote_args_fn" not in stage.map_batches_kwargs
        assert stage.map_batches_kwargs == {
            "zero_copy_batch": True,
            "max_concurrency": 8,
            "accelerator_type": gpu_type,
            "num_gpus": 1,
        }


def test_vllm_engine_processor_task_override():
    config = vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen3.5-0.8B",
        engine_kwargs=dict(
            task_type=vLLMTaskType.EMBED,
        ),
        task_type=vLLMTaskType.GENERATE,
        concurrency=4,
        batch_size=64,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        detokenize_stage=DetokenizeStageConfig(enabled=True),
        prepare_multimodal_stage=PrepareMultimodalStageConfig(enabled=True),
    )
    processor = ProcessorBuilder.build(config)
    stage = processor.get_stage_by_name("vLLMEngineStage")

    assert stage.fn_constructor_kwargs["task_type"] == vLLMTaskType.GENERATE
    assert (
        stage.fn_constructor_kwargs["engine_kwargs"]["task_type"]
        == vLLMTaskType.GENERATE
    )


def test_vllm_engine_processor_invalid_task():
    with pytest.raises(
        pydantic.ValidationError, match="Invalid task type: invalid_task"
    ):
        vLLMEngineProcessorConfig(
            model_source="Qwen/Qwen3.5-0.8B",
            engine_kwargs=dict(
                task_type=vLLMTaskType.EMBED,
            ),
            task_type="invalid_task",
            concurrency=4,
            batch_size=64,
            chat_template_stage=ChatTemplateStageConfig(enabled=True),
            tokenize_stage=TokenizerStageConfig(enabled=True),
            detokenize_stage=DetokenizeStageConfig(enabled=True),
            prepare_multimodal_stage=PrepareMultimodalStageConfig(enabled=True),
        )


def test_vllm_engine_processor_placement_group(gpu_type):
    config = vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen3.5-0.8B",
        engine_kwargs=dict(
            max_model_len=8192,
        ),
        accelerator_type=gpu_type,
        concurrency=4,
        batch_size=64,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        placement_group_config=dict(bundles=[{"CPU": 1, "GPU": 1}]),
    )
    processor = ProcessorBuilder.build(config)
    stage = processor.get_stage_by_name("vLLMEngineStage")

    stage.map_batches_kwargs.pop("runtime_env")
    stage.map_batches_kwargs.pop("compute")

    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "max_concurrency": 8,
        "accelerator_type": gpu_type,
        "num_cpus": 1,
        "num_gpus": 1,
    }


@pytest.mark.parametrize(
    "engine_kwargs_extra,expected_num_bundles",
    [
        ({"tensor_parallel_size": 2}, 2),
        (
            {"tensor_parallel_size": 2, "pipeline_parallel_size": 2},
            4,
        ),
        ({}, 1),  # Default case: tp=1, pp=1 → executor_backend="uni"
    ],
)
def test_vllm_engine_processor_bundle_per_worker(
    gpu_type, engine_kwargs_extra, expected_num_bundles
):
    """Test bundle_per_worker auto-expands based on tp*pp."""
    engine_kwargs = dict(max_model_len=8192)
    engine_kwargs.update(engine_kwargs_extra)
    config = vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen3.5-0.8B",
        engine_kwargs=engine_kwargs,
        accelerator_type=gpu_type,
        concurrency=4,
        batch_size=64,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        placement_group_config={"bundle_per_worker": {"CPU": 1, "GPU": 1}},
    )
    processor = ProcessorBuilder.build(config)
    stage = processor.get_stage_by_name("vLLMEngineStage")

    stage.map_batches_kwargs.pop("runtime_env")
    stage.map_batches_kwargs.pop("compute")

    expected_kwargs = {
        "zero_copy_batch": True,
        "max_concurrency": 8,
        "accelerator_type": gpu_type,
    }

    if expected_num_bundles > 1:
        # TP/PP > 1 -> executor_backend="ray"
        ray_remote_args_fn = stage.map_batches_kwargs.pop("ray_remote_args_fn")
        assert ray_remote_args_fn.args[0] == expected_num_bundles
        assert ray_remote_args_fn.args[1] == gpu_type
        expected_bundles = [{"CPU": 1, "GPU": 1}] * expected_num_bundles
        assert ray_remote_args_fn.args[2]["bundles"] == expected_bundles
        assert ray_remote_args_fn.args[2]["strategy"] == "PACK"
        expected_kwargs["num_gpus"] = 0
    else:
        # TP=1, PP=1 -> executor_backend="uni"
        expected_kwargs["num_cpus"] = 1
        expected_kwargs["num_gpus"] = 1

    assert stage.map_batches_kwargs == expected_kwargs


def test_vllm_engine_processor_bundle_per_worker_conflict(gpu_type):
    """Test that specifying both bundle_per_worker and bundles raises error."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        vLLMEngineProcessorConfig(
            model_source="Qwen/Qwen3.5-0.8B",
            engine_kwargs=dict(max_model_len=8192),
            accelerator_type=gpu_type,
            placement_group_config={
                "bundle_per_worker": {"CPU": 1, "GPU": 1},
                "bundles": [{"CPU": 1, "GPU": 1}],
            },
        )


def test_prepare_multimodal_stage_vllm_engine_processor(gpu_type, model_smolvlm_256m):
    config = vLLMEngineProcessorConfig(
        model_source=model_smolvlm_256m,
        engine_kwargs=dict(
            max_model_len=8192,
        ),
        accelerator_type=gpu_type,
        concurrency=1,
        batch_size=16,
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
            model_config_kwargs=dict(
                allowed_local_media_path="/tmp",
            ),
        ),
    )
    processor = ProcessorBuilder.build(config)

    assert "PrepareMultimodalStage" in processor.list_stage_names()
    stage = processor.get_stage_by_name("PrepareMultimodalStage")
    fn_kwargs = stage.fn_constructor_kwargs

    assert "model_config_kwargs" in fn_kwargs
    model_config_kwargs = fn_kwargs["model_config_kwargs"]
    assert model_config_kwargs["allowed_local_media_path"] == "/tmp"
    assert model_config_kwargs["model"] == model_smolvlm_256m


class TestVLLMEngineProcessorConfig:
    def test_build_processor_autoconfig_failure(self):
        config = vLLMEngineProcessorConfig(
            model_source="nonexistent-org/nonexistent-model",
        )

        processor = build_processor(config)
        assert processor is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
