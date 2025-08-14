"""This test suite does not need sglang to be installed."""
import sys
from unittest.mock import MagicMock, patch

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


class TestSGLangEngineProcessorConfig:
    @pytest.mark.parametrize(
        "experimental_config",
        [
            {"max_tasks_in_flight_per_actor": 10},
            {},
        ],
    )
    def test_experimental_max_tasks_in_flight_per_actor_usage(
        self, experimental_config
    ):
        """Tests that max_tasks_in_flight_per_actor is set properly in the ActorPoolStrategy."""

        from ray.llm._internal.batch.processor.base import DEFAULT_MAX_TASKS_IN_FLIGHT
        from ray.llm._internal.batch.processor.sglang_engine_proc import (
            SGLangEngineProcessorConfig,
            build_sglang_engine_processor,
        )

        with patch("ray.data.ActorPoolStrategy") as mock_actor_pool:
            mock_actor_pool.return_value = MagicMock()

            config = SGLangEngineProcessorConfig(
                model_source="unsloth/Llama-3.2-1B-Instruct",
                experimental=experimental_config,
            )
            build_sglang_engine_processor(config)

            mock_actor_pool.assert_called()
            call_kwargs = mock_actor_pool.call_args[1]
            if experimental_config:
                assert (
                    call_kwargs["max_tasks_in_flight_per_actor"]
                    == experimental_config["max_tasks_in_flight_per_actor"]
                )
            else:
                assert (
                    call_kwargs["max_tasks_in_flight_per_actor"]
                    == DEFAULT_MAX_TASKS_IN_FLIGHT
                )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
