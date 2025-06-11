import shutil
import sys
import os
import logging

import pytest

import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

logger = logging.getLogger(__name__)


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


@ray.remote(num_gpus=1)
def delete_torch_compile_cache_on_worker():
    """Delete torch compile cache on worker.
    Avoids AssertionError due to torch compile cache corruption
    TODO(seiji): check if this is still needed after https://github.com/vllm-project/vllm/issues/18851 is fixed
    """
    torch_compile_cache_path = os.path.expanduser("~/.cache/vllm/torch_compile_cache")
    if os.path.exists(torch_compile_cache_path):
        shutil.rmtree(torch_compile_cache_path)
        logger.warning(f"Deleted torch compile cache at {torch_compile_cache_path}")


@pytest.mark.parametrize(
    "model_source,tp_size,pp_size,concurrency,sample_size",
    [
        # LLaVA model with TP=1, PP=1, concurrency=1
        ("llava-hf/llava-1.5-7b-hf", 1, 1, 1, 60),
        # Pixtral model with TP=2, PP=1, concurrency=2
        ("mistral-community/pixtral-12b", 2, 1, 2, 60),
    ],
)
def test_vllm_vision_language_models(
    model_source, tp_size, pp_size, concurrency, sample_size
):
    """Test vLLM with vision language models using different configurations."""

    ray.get(delete_torch_compile_cache_on_worker.remote())

    # vLLM v1 does not support decoupled tokenizer,
    # but since the tokenizer is in a separate process,
    # the overhead should be moderated.
    tokenize = False
    detokenize = False

    processor_config = vLLMEngineProcessorConfig(
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
        has_image=True,
    )

    processor = build_llm_processor(
        processor_config,
        preprocess=lambda row: dict(
            model=model_source,
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

    ds = ray.data.range(sample_size)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == sample_size
    assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
