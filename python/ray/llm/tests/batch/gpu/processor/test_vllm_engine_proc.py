import sys

import pytest

import ray
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.vllm_engine_proc import (
    vLLMEngineProcessorConfig,
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
    }

    runtime_env = stage.map_batches_kwargs.pop("runtime_env")
    assert "env_vars" in runtime_env
    assert runtime_env["env_vars"]["RANDOM_ENV_VAR"] == "12345"
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 4,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 1,
    }


def test_generation_model(gpu_type, model_opt_125m):
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
        ),
        batch_size=16,
        accelerator_type=gpu_type,
        concurrency=1,
        apply_chat_template=True,
        chat_template=chat_template,
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


def test_embedding_model(gpu_type, model_opt_125m):
    processor_config = vLLMEngineProcessorConfig(
        model_source=model_opt_125m,
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
        chat_template="",
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


def test_vision_model(gpu_type, model_llava_354m):
    processor_config = vLLMEngineProcessorConfig(
        model_source=model_llava_354m,
        task_type="generate",
        engine_kwargs=dict(
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
        ),
        # CI uses T4 GPU which is not supported by vLLM v1 FlashAttn.
        # runtime_env=dict(
        #     env_vars=dict(
        #         VLLM_USE_V1="1",
        #     ),
        # ),
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
