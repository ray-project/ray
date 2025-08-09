import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.vllm_engine_proc import (
    vLLMEngineProcessorConfig,
    vLLMSharedEngineProcessorConfig,
)
from ray.serve.llm import LLMConfig, ModelLoadingConfig

CHAT_TEMPLATE = """
{% if messages[0]['role'] == 'system' %}
    {% set offset = 1 %}
{% else %}
    {% set offset = 0 %}
{% endif %}
{{ bos_token }}
{% for message in messages %}
    {% if (message['role'] == 'user') != (loop.index0 % 2 == offset) %}
        {{ raise_exception('Conversation roles must alternate user/assistant/user/assistant/...') }}
    {% endif %}
    {{ '<|im_start|>' + message['role'] + '\n' + message['content'] | trim + '<|im_end|>\n' }}
{% endfor %}
{% if add_generation_prompt %}
    {{ '<|im_start|>assistant\n' }}
{% endif %}
    """


def test_vllm_engine_processor(gpu_type, model_opt_125m):
    config = vLLMEngineProcessorConfig(
        model_source=model_opt_125m,
        engine_kwargs=dict(
            max_model_len=8192,
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
        "model": model_opt_125m,
        "engine_kwargs": {
            "max_model_len": 8192,
            "distributed_executor_backend": "mp",
        },
        "task_type": "generate",
        "max_pending_requests": 111,
        "dynamic_lora_loading_path": None,
        "max_concurrent_batches": 8,
        "batch_size": 64,
    }

    runtime_env = stage.map_batches_kwargs.pop("runtime_env")
    assert "env_vars" in runtime_env
    assert runtime_env["env_vars"]["RANDOM_ENV_VAR"] == "12345"
    compute = stage.map_batches_kwargs.pop("compute")
    assert isinstance(compute, ray.data._internal.compute.ActorPoolStrategy)
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "max_concurrency": 8,
        "accelerator_type": gpu_type,
        "num_gpus": 1,
    }


def test_generation_model(gpu_type, model_opt_125m):
    # OPT models don't have chat template, so we use ChatML template
    # here to demonstrate the usage of custom chat template.
    processor_config = vLLMEngineProcessorConfig(
        model_source=model_opt_125m,
        engine_kwargs=dict(
            enable_prefix_caching=False,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
            max_model_len=2048,
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        apply_chat_template=True,
        chat_template=CHAT_TEMPLATE,
        tokenize=True,
        detokenize=True,
    )

    processor = ProcessorBuilder.build(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                max_tokens=50,
                detokenize=False,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_embedding_model(gpu_type, model_smolvlm_256m):
    processor_config = vLLMEngineProcessorConfig(
        model_source=model_smolvlm_256m,
        task_type="embed",
        engine_kwargs=dict(
            enable_prefix_caching=False,
            enable_chunked_prefill=False,
            max_model_len=2048,
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        apply_chat_template=True,
        chat_template=None,
        tokenize=True,
        detokenize=False,
    )

    processor = ProcessorBuilder.build(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
        ),
        postprocess=lambda row: {
            "resp": row["embeddings"],
            "prompt": row["prompt"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)
    assert all("prompt" in out for out in outs)


def test_vision_model(gpu_type, model_smolvlm_256m):
    processor_config = vLLMEngineProcessorConfig(
        model_source=model_smolvlm_256m,
        task_type="generate",
        engine_kwargs=dict(
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
            # CI uses T4 GPU which does not support bfloat16.
            dtype="half",
        ),
        # CI uses T4 GPU which is not supported by vLLM v1 FlashAttn.
        runtime_env=dict(
            env_vars=dict(
                VLLM_USE_V1="0",
            ),
        ),
        apply_chat_template=True,
        has_image=True,
        tokenize=False,
        detokenize=False,
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are an assistant"},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Say {row['id']} words about this image.",
                        },
                        {
                            "type": "image",
                            "image": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg",
                        },
                    ],
                },
            ],
            sampling_params=dict(
                temperature=0.3,
                max_tokens=50,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


class TestSharedvLLMEngine:
    def test_multi_turn_shared_engine(self, gpu_type, model_opt_125m):
        """Test multi-turn conversation using a shared vLLM engine."""
        shared_processor_config = vLLMSharedEngineProcessorConfig(
            llm_config=LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id=model_opt_125m,
                ),
                engine_kwargs=dict(
                    enable_prefix_caching=False,
                    enable_chunked_prefill=True,
                    max_num_batched_tokens=2048,
                    max_model_len=2048,
                    # Skip CUDA graph capturing to reduce startup time.
                    enforce_eager=True,
                ),
            ),
            model_source=model_opt_125m,
            batch_size=16,
            accelerator_type=gpu_type,
            concurrency=1,
            apply_chat_template=True,
            chat_template=CHAT_TEMPLATE,
            tokenize=True,
            detokenize=True,
        )

        processor1 = ProcessorBuilder.build(
            config=shared_processor_config,
            preprocess=lambda row: dict(
                messages=[
                    {"role": "system", "content": "You are a calculator"},
                    {"role": "user", "content": f"{row['id']} ** 3 = ?"},
                ],
                sampling_params=dict(
                    temperature=0.3,
                    max_tokens=50,
                    detokenize=False,
                ),
            ),
            postprocess=lambda row: {
                "resp1": row["generated_text"],
            },
        )

        processor2 = ProcessorBuilder.build(
            config=shared_processor_config,
            preprocess=lambda row: dict(
                **row,
                messages=[
                    {"role": "system", "content": "Based on the previous conversation"},
                    {"role": "user", "content": "What is this number minus 2?"},
                ],
                sampling_params=dict(
                    temperature=0.3,
                    max_tokens=50,
                    detokenize=False,
                ),
            ),
            postprocess=lambda row: {
                **row,
                "resp2": row["generated_text"],
            },
        )

        ds = ray.data.range(10)
        ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})

        ds_turn1 = processor1(ds)
        ds_turn2 = processor2(ds_turn1)

        outs = ds_turn2.take_all()

        assert len(outs) == 10
        assert all("resp1" in out for out in outs)
        assert all("resp2" in out for out in outs)

    def test_shared_engine_instantiation(self, gpu_type, model_opt_125m):
        """Test that shared engine deployments are properly instantiated."""
        with patch(
            "ray.llm._internal.batch.processor.vllm_engine_proc.build_llm_deployment"
        ) as mock_build_deployment, patch(
            "ray.llm._internal.batch.processor.vllm_engine_proc.run"
        ) as mock_run:
            mock_build_deployment.return_value = MagicMock()
            mock_run.return_value = MagicMock()

            shared_processor_config = vLLMSharedEngineProcessorConfig(
                llm_config=LLMConfig(
                    model_loading_config=ModelLoadingConfig(
                        model_id=model_opt_125m,
                    ),
                    engine_kwargs=dict(
                        enable_prefix_caching=False,
                        enable_chunked_prefill=True,
                        max_num_batched_tokens=2048,
                        max_model_len=2048,
                        # Skip CUDA graph capturing to reduce startup time.
                        enforce_eager=True,
                    ),
                ),
                model_source=model_opt_125m,
                batch_size=16,
                accelerator_type=gpu_type,
                concurrency=1,
                apply_chat_template=True,
                chat_template=CHAT_TEMPLATE,
                tokenize=True,
                detokenize=True,
            )

            processor1 = ProcessorBuilder.build(config=shared_processor_config)
            processor2 = ProcessorBuilder.build(config=shared_processor_config)

            assert mock_build_deployment.call_count == 1

            # Using a different instance of vLLMSharedEngineProcessorConfig results
            # in a new deployment despite the same configurations.
            shared_processor_config2 = vLLMSharedEngineProcessorConfig(
                llm_config=LLMConfig(
                    model_loading_config=ModelLoadingConfig(
                        model_id=model_opt_125m,
                    ),
                    engine_kwargs=dict(
                        enable_prefix_caching=False,
                        enable_chunked_prefill=True,
                        max_num_batched_tokens=2048,
                        max_model_len=2048,
                        # Skip CUDA graph capturing to reduce startup time.
                        enforce_eager=True,
                    ),
                ),
                model_source=model_opt_125m,
                batch_size=16,
                accelerator_type=gpu_type,
                concurrency=1,
                apply_chat_template=True,
                chat_template=CHAT_TEMPLATE,
                tokenize=True,
                detokenize=True,
            )

            processor3 = ProcessorBuilder.build(config=shared_processor_config2)

            assert mock_build_deployment.call_count == 2


class TestVLLMEngineProcessorConfig:
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
        from ray.llm._internal.batch.processor.vllm_engine_proc import (
            build_vllm_engine_processor,
            vLLMEngineProcessorConfig,
        )

        with patch("ray.data.ActorPoolStrategy") as mock_actor_pool:
            mock_actor_pool.return_value = MagicMock()

            config = vLLMEngineProcessorConfig(
                model_source="unsloth/Llama-3.2-1B-Instruct",
                experimental=experimental_config,
            )
            build_vllm_engine_processor(config)

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
