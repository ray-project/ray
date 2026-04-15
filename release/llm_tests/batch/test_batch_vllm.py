import sys
import logging
import time

import pytest

import ray
from ray.data.llm import (
    build_processor,
    vLLMEngineProcessorConfig,
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    PrepareMultimodalStageConfig,
    TokenizerStageConfig,
)

logger = logging.getLogger(__name__)

S3_ARTIFACT_ASSETS_URL = (
    "https://air-example-data.s3.amazonaws.com/rayllm-ossci/assets/"
)


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
    `parse_chat_messages_async` is thoroughly tested in vLLM. This test serves as an
    integration test to verify that the function isn't moved to an unexpected location and its signature isn't changed.
    """
    from vllm.config import ModelConfig
    from vllm.entrypoints.chat_utils import parse_chat_messages_async

    image_url = "https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/assets/cherry_blossom.jpg"
    image_uuid = str(hash(image_url))

    conversation, mm_data, mm_uuids = await parse_chat_messages_async(
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
        content_format="string",
    )

    assert conversation == [
        {"role": "user", "content": "<|image_1|>\nWhat's in the image?"}
    ]

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

    processor = build_processor(
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

    processor = build_processor(
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
    """Test vLLM with Llama model and LoRA adapter support.

    Validates that the LoRA adapter is actually applied during generation by
    using greedy sampling (temperature=0) and short, diverse, open-ended prompts,
    then asserting that a significant number of base-model vs LoRA paired outputs differ.

    If the LoRA adapter is not being applied, we expect very few pairs to differ
    due to non-determinism from vLLM's dynamic batching and CUDA. If it is being applied,
    we expect a significant fraction of pairs to differ due to the adapter's effect on the output.

    The two regimes are separated by the `min_diff_fraction` threshold defined below.
    """
    # Minimum fraction of base/LoRA pairs that must differ to pass.
    # Determined empirically.
    min_diff_fraction = 0.5
    model_source = "s3://air-example-data/llama-3.2-216M-dummy/"
    lora_path = "s3://air-example-data/"
    lora_name = "llama-3.2-216M-lora-dummy"
    max_lora_rank = 32

    # Short, diverse prompts — shorter prompts let the LoRA perturbation
    # manifest earlier in generation before the base model's momentum
    # dominates the output. Also prefer open-ended prompts to encourage LoRA-induced diversity.
    prompts = [
        "Hello world",
        "The capital of France is",
        "Once upon a time",
        "1 + 1 =",
        "def fibonacci(n):",
        "The quick brown fox",
        "In the beginning",
        "To be or not to be",
        "import numpy as np",
        "The weather today is",
        "My favorite color is",
        "How to cook rice:",
        "The meaning of life is",
        "SELECT * FROM",
        "Dear Sir or Madam,",
        "Breaking news:",
        "A long time ago in a galaxy",
        "The first law of thermodynamics",
        "class MyClass:",
        "Roses are red,",
        "According to recent studies,",
        "Step 1: Preheat the oven",
        "The president announced",
        "In mathematics, a prime number",
        "function hello() {",
        "The cat sat on the",
        "Water boils at",
        "Happy birthday to",
        "ERROR: NullPointerException",
        "The mitochondria is the",
    ]
    num_pairs = len(prompts)

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
        # minimize non-determinism from vLLM's dynamic batching by sending (1 base + 1 LoRA) per batch
        batch_size=2,
        concurrency=1,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    processor = build_processor(
        processor_config,
        preprocess=lambda row: dict(
            model=model_source if row["id"] % 2 == 0 else lora_name,
            messages=[{"role": "user", "content": row["prompt"]}],
            sampling_params=dict(
                temperature=0,
                max_tokens=50,
                detokenize=False,
            ),
        ),
        postprocess=lambda row: {
            "id": row["id"],
            "resp": row["generated_text"],
        },
    )

    # Build paired rows: id 2k = base, id 2k+1 = LoRA, same prompt.
    rows = []
    for i, prompt in enumerate(prompts):
        rows.append({"id": 2 * i, "prompt": prompt})
        rows.append({"id": 2 * i + 1, "prompt": prompt})

    ds = ray.data.from_items(rows)
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()

    assert len(outs) == 2 * num_pairs
    assert all("resp" in out for out in outs)

    # LoRA exercise check: a significant fraction of pairs must differ.
    by_id = {out["id"]: out["resp"] for out in outs}
    diffs = sum(
        1 for k in range(num_pairs) if by_id[2 * k] != by_id[2 * k + 1]
    )
    min_diffs = int(num_pairs * min_diff_fraction)
    assert diffs >= min_diffs, (
        f"Only {diffs}/{num_pairs} base/LoRA pairs differ (need >= {min_diffs}) — "
        "the LoRA adapter does not appear to be applied."
    )


@pytest.mark.parametrize(
    "model_source,tp_size,pp_size,concurrency,sample_size,chat_template_content_format,apply_sys_msg_formatting",
    [
        # LLaVA model with TP=1, PP=1, concurrency=1
        ("llava-hf/llava-1.5-7b-hf", 1, 1, 1, 60, "openai", False),
        # Pixtral model with TP=2, PP=1, concurrency=2
        ("mistral-community/pixtral-12b", 2, 1, 2, 60, "openai", True),
    ],
)
def test_vllm_vision_language_models(
    model_source,
    tp_size,
    pp_size,
    concurrency,
    sample_size,
    chat_template_content_format,
    apply_sys_msg_formatting,
):
    """Test vLLM with vision language models using different configurations."""

    # vLLM v1 does not support decoupled tokenizer,
    # but since the tokenizer is in a separate process,
    # the overhead should be moderated.
    tokenize = False
    detokenize = False

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_source,
        task_type="generate",
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=4096,
            enable_chunked_prefill=True,
        ),
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
            chat_template_content_format=chat_template_content_format,
            apply_sys_msg_formatting=apply_sys_msg_formatting,
        ),
        apply_chat_template=True,
        tokenize=tokenize,
        detokenize=detokenize,
        batch_size=16,
        concurrency=concurrency,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )
    llm_processor = build_processor(
        llm_processor_config,
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
                            "image_url": {
                                "url": S3_ARTIFACT_ASSETS_URL + "cherry_blossom.jpg"
                            },
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

    ds = ray.data.range(sample_size)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = llm_processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == sample_size
    assert all("resp" in out for out in outs)


@pytest.mark.parametrize(
    "multimodal_content",
    [
        {
            "type": "image_url",
            "image_url": {"url": S3_ARTIFACT_ASSETS_URL + "cherry_blossom.jpg"},
        },
        {
            "type": "video_url",
            "video_url": {"url": S3_ARTIFACT_ASSETS_URL + "free-videos.mp4"},
        },
    ],
)
def test_vllm_qwen_vl_multimodal(multimodal_content):
    model_source = "Qwen/Qwen2.5-VL-3B-Instruct"

    llm_processor_config = vLLMEngineProcessorConfig(
        model_source=model_source,
        task_type="generate",
        engine_kwargs=dict(
            enable_chunked_prefill=True,
            distributed_executor_backend="ray",
            # A single GPU won't be able to accomodate Qwen/Qwen2.5-VL-3B-Instruct's memory requirements
            # due to vllm0.12.0 resource/profiling issues.
            # Issue: https://github.com/vllm-project/vllm/issues/30521.
            tensor_parallel_size=2,
            pipeline_parallel_size=1,
        ),
        prepare_multimodal_stage=PrepareMultimodalStageConfig(
            enabled=True,
        ),
        chat_template_stage=ChatTemplateStageConfig(enabled=True),
        tokenize_stage=TokenizerStageConfig(enabled=False),
        detokenize_stage=DetokenizeStageConfig(enabled=False),
        batch_size=16,
        concurrency=1,
    )

    llm_processor = build_processor(
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
            messages=[
                {"role": "system", "content": "You are an assistant"},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Describe this asset in {row['id']} sentences.",
                        },
                        multimodal_content,
                    ],
                },
            ],
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = llm_processor(ds)
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

    processor = build_processor(
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
        # Strategy omitted (PACK default). Omitted GPU now validates as 0.0; explicit GPU: 1 for this GPU job.
        (
            "ray",
            dict(bundles=[{"CPU": 1, "GPU": 1}] * 4),
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

    processor = build_processor(
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


def test_vllm_autoscaling_no_starvation():
    """Test that chained vLLMEngineProcessor instances with autoscaling
    concurrency can run without starving each other.
    """
    processor_config_1 = vLLMEngineProcessorConfig(
        model_source="facebook/opt-1.3b",
        chat_template_stage=False,
        tokenize_stage=False,
        detokenize_stage=False,
        batch_size=16,
        concurrency=(1, 4),
    )

    processor_config_2 = vLLMEngineProcessorConfig(
        model_source="facebook/opt-1.3b",
        chat_template_stage=False,
        tokenize_stage=False,
        detokenize_stage=False,
        batch_size=16,
        concurrency=(3, 4),
    )

    processor_1 = build_processor(
        processor_config_1,
        preprocess=lambda row: dict(
            prompt=f"Calculate {row['id']} ** 2 = ",
            sampling_params=dict(
                temperature=0.3,
                max_tokens=30,
                detokenize=True,
            ),
        ),
        postprocess=lambda row: {
            "resp_1": row["generated_text"],
            "id": row.get("id", None),
        },
    )

    processor_2 = build_processor(
        processor_config_2,
        preprocess=lambda row: dict(
            prompt=f"Previous result: {row.get('resp_1', 'N/A')}. Now calculate its cube: ",
            sampling_params=dict(
                temperature=0.3,
                max_tokens=30,
                detokenize=True,
            ),
        ),
        postprocess=lambda row: {
            "resp_2": row["generated_text"],
            "resp_1": row.get("resp_1", None),
            "id": row.get("id", None),
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 1})

    processed_ds = processor_2(processor_1(ds))
    processed_ds = processed_ds.materialize()
    results = processed_ds.take_all()

    assert len(results) == 60
    assert all("resp_1" in out for out in results)
    assert all("resp_2" in out for out in results)
    assert all("id" in out for out in results)
    assert all(out.get("resp_1") for out in results)
    assert all(out.get("resp_2") for out in results)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
