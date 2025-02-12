from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.vllm_engine_proc import (
    vLLMEngineProcessorConfig,
)


def test_vllm_engine_processor():
    config = vLLMEngineProcessorConfig(
        model="meta-llama/Llama-3.1-8B-Instruct",
        engine_kwargs=dict(
            max_model_len=8192,
        ),
        runtime_env=dict(
            env_vars=dict(
                RANDOM_ENV_VAR="12345",
            ),
        ),
        accelerator_type="L40S",
        concurrency=4,
        batch_size=64,
        max_pending_requests=111,
        apply_chat_template=True,
        tokenize=True,
        detokenize=True,
        has_image=True,
    )
    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "PrepareImageStage",
        "ChatTemplateStage",
        "TokenizeStage",
        "vLLMEngineStage",
        "DetokenizeStage",
    ]

    stage = processor.get_stage_by_name("vLLMEngineStage")
    assert stage.fn_constructor_kwargs == {
        "model": "meta-llama/Llama-3.1-8B-Instruct",
        "engine_kwargs": {
            "max_model_len": 8192,
            "distributed_executor_backend": "mp",
        },
        "task_type": "generate",
        "max_pending_requests": 111,
    }

    runtime_env = stage.map_batches_kwargs.pop("runtime_env")
    assert "env_vars" in runtime_env
    assert runtime_env["env_vars"]["RANDOM_ENV_VAR"] == "12345"
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 4,
        "max_concurrency": 4,
        "accelerator_type": "L40S",
        "num_gpus": 1,
    }
