import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data.llm import (
    MultimodalProcessorConfig,
    build_llm_processor,
    vLLMEngineProcessorConfig,
)
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    PrepareImageStageConfig,
    PrepareMultimodalStageConfig,
    TokenizerStageConfig,
)


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
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        detokenize_stage=DetokenizeStageConfig(enabled=True),
        prepare_image_stage=PrepareImageStageConfig(enabled=True),
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


def test_vllm_engine_processor_placement_group(gpu_type, model_opt_125m):
    config = vLLMEngineProcessorConfig(
        model_source=model_opt_125m,
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


@pytest.mark.parametrize("backend", ["mp", "ray"])
def test_generation_model(gpu_type, model_opt_125m, backend):
    # OPT models don't have chat template, so we use ChatML template
    # here to demonstrate the usage of custom chat template.
    chat_template = """
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

    processor_config = vLLMEngineProcessorConfig(
        model_source=model_opt_125m,
        engine_kwargs=dict(
            enable_prefix_caching=False,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
            max_model_len=2048,
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
            distributed_executor_backend=backend,
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        chat_template_stage=ChatTemplateStageConfig(
            enabled=True, chat_template=chat_template
        ),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        detokenize_stage=DetokenizeStageConfig(enabled=True),
    )

    processor = build_llm_processor(
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
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=True),
        detokenize_stage=DetokenizeStageConfig(enabled=False),
    )

    processor = build_llm_processor(
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


@pytest.mark.parametrize("use_nested_config", [True, False])
def test_legacy_vision_model(gpu_type, model_smolvlm_256m, use_nested_config):
    processor_config = dict(
        model_source=model_smolvlm_256m,
        task_type="generate",
        engine_kwargs=dict(
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
            # CI uses T4 GPU which does not support bfloat16.
            dtype="half",
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
    )

    if use_nested_config:
        processor_config.update(
            prepare_image_stage=PrepareImageStageConfig(enabled=True),
            chat_template_stage=ChatTemplateStageConfig(enabled=True),
            tokenize_stage=TokenizerStageConfig(enabled=False),
            detokenize_stage=DetokenizeStageConfig(enabled=False),
        )
    else:
        processor_config.update(
            apply_chat_template=True,
            has_image=True,
            tokenize=False,
            detokenize=False,
        )

    processor = build_llm_processor(
        vLLMEngineProcessorConfig(**processor_config),
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

    ds = ray.data.range(1)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    print("LLM output: ", outs)
    assert len(outs) == 1
    assert all("resp" in out for out in outs)


@pytest.mark.parametrize("input_raw_image_data", [True, False])
@pytest.mark.parametrize("decouple_tokenizer", [True, False])
def test_vision_model(
    gpu_type, model_smolvlm_256m, image_asset, input_raw_image_data, decouple_tokenizer
):
    image_url, image_pil = image_asset
    multimodal_processor_config = MultimodalProcessorConfig(
        model_source=model_smolvlm_256m,
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
            chat_template_content_format="openai",
        ),
        concurrency=1,
    )
    multimodal_processor = build_llm_processor(
        multimodal_processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are an assistant"},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Say {row['val']} words about this image.",
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": image_url},
                            "uuid": "image-1-id",  # UUID will not be included in the output as it's only used for internal caching
                        }
                        if input_raw_image_data
                        else {
                            "type": "image_pil",
                            "image_pil": image_pil,
                            "uuid": "image-2-id",
                        },
                    ],
                },
            ],
        ),
    )

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_smolvlm_256m,
        task_type="generate",
        engine_kwargs=dict(
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
            # CI uses T4 GPU which does not support bfloat16.
            dtype="half",
            limit_mm_per_prompt={"image": 1},
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=decouple_tokenizer),
        detokenize_stage=DetokenizeStageConfig(enabled=decouple_tokenizer),
    )

    llm_processor = build_llm_processor(
        llm_processor_config,
        preprocess=lambda row: dict(
            sampling_params=dict(
                temperature=0.3,
                max_tokens=50,
                detokenize=not decouple_tokenizer,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = llm_processor(multimodal_processor(ds))
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_video_model(gpu_type, model_qwen_2_5_vl_3b_instruct):
    multimodal_processor_config = MultimodalProcessorConfig(
        model_source=model_qwen_2_5_vl_3b_instruct,
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
            chat_template_content_format="openai",
        ),
        concurrency=1,
    )
    multimodal_processor = build_llm_processor(
        multimodal_processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are an assistant"},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Describe this video in {row['val']} words.",
                        },
                        {
                            "type": "video_url",
                            "video_url": {
                                "url": "https://content.pexels.com/videos/free-videos.mp4"
                            },
                        },
                    ],
                },
            ],
        ),
    )

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_qwen_2_5_vl_3b_instruct,
        task_type="generate",
        engine_kwargs=dict(
            enforce_eager=True,
            # Limit the number of videos that can be provided per prompt to prevent memory issues
            limit_mm_per_prompt={"video": 1},
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=False),
        detokenize_stage=DetokenizeStageConfig(enabled=False),
    )

    llm_processor = build_llm_processor(
        llm_processor_config,
        preprocess=lambda row: dict(
            sampling_params=dict(
                temperature=0.3,
                max_tokens=50,
            ),
            mm_processor_kwargs=dict(
                min_pixels=28 * 28,
                max_pixels=1280 * 28 * 28,
                fps=1,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(5)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = llm_processor(multimodal_processor(ds))
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 5
    assert all("resp" in out for out in outs)


@pytest.mark.parametrize("input_raw_audio_data", [True, False])
def test_audio_model(
    gpu_type, model_qwen_2_5_omni_3b, audio_asset, input_raw_audio_data
):
    audio_url, audio_data = audio_asset
    multimodal_processor_config = MultimodalProcessorConfig(
        model_source=model_qwen_2_5_omni_3b,
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
            chat_template_content_format="openai",
        ),
        concurrency=1,
    )
    multimodal_processor = build_llm_processor(
        multimodal_processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are an assistant"},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Describe this audio in {row['val']} words.",
                        },
                        {
                            "type": "input_audio",
                            "input_audio": {"data": audio_data, "format": "wav"},
                        }
                        if input_raw_audio_data
                        else {
                            "type": "audio_url",
                            "audio_url": {"url": audio_url},
                        },
                    ],
                },
            ],
        ),
    )

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_qwen_2_5_omni_3b,
        task_type="generate",
        engine_kwargs=dict(
            enforce_eager=True,
            limit_mm_per_prompt={"audio": 1},
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=False),
        detokenize_stage=DetokenizeStageConfig(enabled=False),
    )

    llm_processor = build_llm_processor(
        llm_processor_config,
        preprocess=lambda row: dict(
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
    ds = llm_processor(multimodal_processor(ds))
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


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
