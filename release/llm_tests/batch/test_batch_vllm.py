import sys
import logging
import time

import pytest

import ray
from ray.data.llm import (
    build_llm_processor,
    MultimodalProcessorConfig,
    vLLMEngineProcessorConfig,
)
from ray.llm._internal.batch.stages.configs import PrepareMultimodalStageConfig

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def disable_vllm_compile_cache(monkeypatch):
    """Automatically disable vLLM compile cache for all tests.

    Avoids AssertionError due to torch compile cache corruption caused by
    running multiple engines on the same node.
    See: https://github.com/vllm-project/vllm/issues/18851, fix expected with
    PyTorch 2.8.0
    """
    monkeypatch.setenv("VLLM_DISABLE_COMPILE_CACHE", "1")


@pytest.fixture(autouse=True)
def add_buffer_time_between_tests():
    """Add buffer time after each test to avoid resource conflicts, which cause
    flakiness.
    """
    import gc

    gc.collect()
    time.sleep(15)


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    ray.shutdown()


@pytest.mark.asyncio
async def test_vllm_multimodal_utils():
    """Test vLLM's multimodal utilities.

    This test is adapted from https://github.com/vllm-project/vllm/blob/main/tests/entrypoints/test_chat_utils.py.
    `parse_chat_messages_futures` is thoroughly tested in vLLM. This test serves as an
    integration test to verify that the function isn't moved to an unexpected location and its signature isn't changed.
    """
    from vllm.config import ModelConfig
    from vllm.entrypoints.chat_utils import parse_chat_messages_futures

    image_url = "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
    image_uuid = str(hash(image_url))

    conversation, mm_future, mm_uuids = parse_chat_messages_futures(
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": image_url},
                        "uuid": image_uuid,
                    },
                    {"type": "text", "text": "What's in the image?"},
                ],
            }
        ],
        ModelConfig(
            "microsoft/Phi-3.5-vision-instruct",
            runner="generate",
            trust_remote_code=True,
            limit_mm_per_prompt={"image": 2},
        ),
        None,  # Tokenizer is not used in vLLM's parse_chat_messages_futures
        content_format="string",
    )

    assert conversation == [
        {"role": "user", "content": "<|image_1|>\nWhat's in the image?"}
    ]

    mm_data = await mm_future
    assert mm_data is not None
    assert set(mm_data.keys()) == {"image"}

    image_data = mm_data.get("image")
    assert image_data is not None

    assert isinstance(image_data, list) and len(image_data) == 1

    assert mm_uuids is not None
    assert "image" in mm_uuids

    image_uuids = mm_uuids.get("image")
    assert image_uuids is not None
    assert isinstance(image_uuids, list) and len(image_uuids) == 1
    assert image_uuids[0] == image_uuid


def test_chat_template_with_vllm():
    """Test vLLM with explicit chat template."""

    processor_config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            max_model_len=16384,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
        ),
        tokenize=True,
        detokenize=True,
        batch_size=16,
        concurrency=1,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
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


@pytest.mark.parametrize(
    "tp_size,pp_size,concurrency",
    [
        (2, 1, 2),  # TP=2, concurrency=2
        (1, 2, 2),  # PP=2, concurrency=2
    ],
)
def test_vllm_llama_parallel(tp_size, pp_size, concurrency):
    """Test vLLM with Llama model using different parallelism configurations."""

    # vLLM v1 does not support decoupled tokenizer,
    # but since the tokenizer is in a separate process,
    # the overhead should be moderated.
    tokenize = False
    detokenize = False

    processor_config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=16384,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
        ),
        tokenize=tokenize,
        detokenize=detokenize,
        batch_size=16,
        accelerator_type=None,
        concurrency=concurrency,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
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

    ds = ray.data.range(120)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()

    # Verify results
    outs = ds.take_all()
    assert len(outs) == 120
    assert all("resp" in out for out in outs)


def test_vllm_llama_lora():
    """Test vLLM with Llama model and LoRA adapter support."""

    model_source = "s3://air-example-data/llama-3.2-216M-dummy/"
    lora_path = "s3://air-example-data/"
    lora_name = "llama-3.2-216M-lora-dummy"
    max_lora_rank = 32

    processor_config = vLLMEngineProcessorConfig(
        model_source=model_source,
        dynamic_lora_loading_path=lora_path,
        engine_kwargs=dict(
            max_model_len=4096,
            enable_chunked_prefill=True,
            enable_lora=True,
            max_lora_rank=max_lora_rank,
        ),
        tokenize=True,
        detokenize=True,
        batch_size=16,
        concurrency=1,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    processor = build_llm_processor(
        processor_config,
        preprocess=lambda row: dict(
            # For even ids, use the base model, for odd ids, use the LoRA adapter
            model=model_source if row["id"] % 2 == 0 else lora_name,
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


@pytest.mark.parametrize(
    "model_source,tp_size,pp_size,concurrency,sample_size,chat_template_content_format",
    [
        # LLaVA model with TP=1, PP=1, concurrency=1
        ("llava-hf/llava-1.5-7b-hf", 1, 1, 1, 60, "openai"),
        # Pixtral model with TP=2, PP=1, concurrency=2
        ("mistral-community/pixtral-12b", 2, 1, 2, 60, "openai"),
    ],
)
def test_vllm_vision_language_models(
    model_source,
    tp_size,
    pp_size,
    concurrency,
    sample_size,
    chat_template_content_format,
):
    """Test vLLM with vision language models using different configurations."""

    # vLLM v1 does not support decoupled tokenizer,
    # but since the tokenizer is in a separate process,
    # the overhead should be moderated.
    tokenize = False
    detokenize = False

    multimodal_processor_config = MultimodalProcessorConfig(
        model_source=model_source,
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
            chat_template_content_format=chat_template_content_format,
        ),
        concurrency=concurrency,
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
                            "text": f"Say {row['id']} words about this image.",
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
                            },
                        },
                    ],
                },
            ],
        ),
    )

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_source,
        task_type="generate",
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=4096,
            enable_chunked_prefill=True,
        ),
        apply_chat_template=True,
        tokenize=tokenize,
        detokenize=detokenize,
        batch_size=16,
        concurrency=concurrency,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
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

    ds = ray.data.range(sample_size)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = llm_processor(multimodal_processor(ds))
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == sample_size
    assert all("resp" in out for out in outs)


@pytest.mark.parametrize(
    "multimodal_content",
    [
        {
            "type": "image_url",
            "image_url": {
                "url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
            },
        },
        {
            "type": "video_url",
            "video_url": {"url": "https://content.pexels.com/videos/free-videos.mp4"},
        },
    ],
)
def test_vllm_qwen_vl_multimodal(multimodal_content):
    model_source = "Qwen/Qwen2.5-VL-3B-Instruct"
    tokenize = False
    detokenize = False

    multimodal_processor_config = MultimodalProcessorConfig(
        model_source=model_source,
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
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
                            "text": f"Describe this asset in {row['id']} sentences.",
                        },
                    ],
                },
            ],
        ),
    )

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_source,
        task_type="generate",
        engine_kwargs=dict(
            max_model_len=4096,
            enable_chunked_prefill=True,
        ),
        apply_chat_template=True,
        tokenize=tokenize,
        detokenize=detokenize,
        batch_size=16,
        concurrency=1,
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


@pytest.mark.parametrize("concurrency", [1, 4])
def test_async_udf_queue_capped(concurrency):
    """
    Test that the large object in input/output rows
    are stored in object store and does not OOM.
    """

    processor_config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            max_model_len=16384,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
        ),
        tokenize=False,
        detokenize=False,
        batch_size=4,
        accelerator_type=None,
        concurrency=concurrency,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    processor = build_llm_processor(
        processor_config,
        preprocess=lambda row: dict(
            # 1M emoji (4 bytes), should not leak to memory heap.
            large_memory_to_carry_over="🤗" * 1_000_000,
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                # we don't care about the actual output
                max_tokens=1,
                detokenize=False,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
            "large_memory_still_there": "large_memory_to_carry_over" in row,
        },
    )

    ds = ray.data.range(12000)

    def map_id_to_val_in_test_no_memory_leak(x):
        return {"id": x["id"], "val": x["id"] + 5}

    ds = ds.map(map_id_to_val_in_test_no_memory_leak)
    ds = processor(ds)
    ds = ds.materialize()

    outs = ds.take_all()
    assert all(out["large_memory_still_there"] for out in outs)


@pytest.mark.parametrize(
    "backend, placement_group_config",
    [
        # Custom placement group with STRICT_PACK strategy
        (
            "ray",
            dict(bundles=[{"CPU": 1, "GPU": 1}] * 4, strategy="STRICT_PACK"),
        ),
        # Custom placement group leaving GPU and strategy unspecified
        (
            "ray",
            dict(bundles=[{"CPU": 1}] * 4),
        ),
        # Empty placement group
        (
            "ray",
            None,
        ),
        # Custom placement group with MP backend
        (
            "mp",
            dict(bundles=[{"GPU": 1}] * 4),
        ),
        # Empty placement group with MP backend
        (
            "mp",
            None,
        ),
    ],
)
def test_vllm_placement_group(backend, placement_group_config):
    """Test vLLM with different placement group configurations."""

    config = vLLMEngineProcessorConfig(
        model_source="facebook/opt-1.3b",
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
            pipeline_parallel_size=2,
            tensor_parallel_size=2,
            distributed_executor_backend=backend,
        ),
        tokenize=False,
        detokenize=False,
        concurrency=1,
        batch_size=16,
        apply_chat_template=False,
        placement_group_config=placement_group_config,
    )

    processor = build_llm_processor(
        config,
        preprocess=lambda row: dict(
            prompt=f"You are a calculator. {row['id']} ** 3 = ?",
            sampling_params=dict(
                temperature=0.3,
                max_tokens=20,
                detokenize=True,
            ),
        ),
        postprocess=lambda row: dict(
            resp=row["generated_text"],
        ),
    )

    ds = ray.data.range(60)
    ds = processor(ds)
    ds = ds.materialize()

    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
