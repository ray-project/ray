import os
import tempfile
import requests
import pytest

import ray
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.vllm_engine_proc import (
    vLLMEngineProcessorConfig,
)


@pytest.fixture(scope="module")
def download_model_ckpt():
    """
    Download the model checkpoint and tokenizer from S3 for testing
    The reason to download the model from S3 is to avoid downloading the model
    from HuggingFace hub during testing, which is flaky because of the rate
    limit and HF hub downtime.
    """

    REMOTE_URL = "https://air-example-data.s3.amazonaws.com/facebook-opt-125m/"
    FILE_LIST = [
        "config.json",
        "flax_model.msgpack",
        "generation_config.json",
        "merges.txt",
        "pytorch_model.bin",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "vocab.json",
    ]

    # Create a temporary directory and save the model and tokenizer.
    with tempfile.TemporaryDirectory() as checkpoint_dir:
        # Download the model checkpoint and tokenizer from S3.
        for file_name in FILE_LIST:
            response = requests.get(REMOTE_URL + file_name)
            with open(os.path.join(checkpoint_dir, file_name), "wb") as fp:
                fp.write(response.content)

        yield os.path.abspath(checkpoint_dir)


def test_vllm_engine_processor(download_model_ckpt):
    config = vLLMEngineProcessorConfig(
        model=download_model_ckpt,
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
        "model": download_model_ckpt,
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


def test_generation_model(download_model_ckpt):
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
        model=download_model_ckpt,
        engine_kwargs=dict(
            enable_prefix_caching=False,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
            max_model_len=2048,
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
        ),
        batch_size=16,
        accelerator_type="L40S",
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


def test_embedding_model(download_model_ckpt):
    processor_config = vLLMEngineProcessorConfig(
        model=download_model_ckpt,
        task_type="embed",
        engine_kwargs=dict(
            enable_prefix_caching=False,
            enable_chunked_prefill=False,
            max_model_len=2048,
            # Skip CUDA graph capturing to reduce startup time.
            enforce_eager=True,
        ),
        batch_size=16,
        accelerator_type="L40S",
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
