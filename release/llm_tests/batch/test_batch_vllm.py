import sys

import pytest

import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig


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
    "tp_size,pp_size,concurrency,vllm_use_v1",
    [
        (2, 1, 2, True),  # TP=2, concurrency=2, vLLM v1
        (1, 2, 2, False),  # PP=2, concurrency=2, vLLM v0
    ],
)
def test_vllm_llama_parallel(tp_size, pp_size, concurrency, vllm_use_v1):
    """Test vLLM with Llama model using different parallelism configurations."""
    if vllm_use_v1:
        runtime_env = dict(
            env_vars=dict(
                VLLM_USE_V1="1",
            ),
        )
        # vLLM v1 does not support decoupled tokenizer,
        # but since the tokenizer is in a separate process,
        # the overhead should be moderated.
        tokenize = False
        detokenize = False
    else:
        runtime_env = {}
        tokenize = True
        detokenize = True

    processor_config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            max_model_len=16384,
            enable_chunked_prefill=True,
            max_num_batched_tokens=2048,
        ),
        runtime_env=runtime_env,
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
        runtime_env={
            "env_vars": {
                "VLLM_USE_V1": "0",
            }
        },
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
    "model_source,tp_size,pp_size,concurrency,sample_size",
    [
        # LLaVA model with TP=1, PP=1, concurrency=1
        ("llava-hf/llava-1.5-7b-hf", 1, 1, 1, 60),
        # Qwen2.5 VL model with TP=2, PP=1, concurrency=2
        ("Qwen/Qwen2.5-VL-3B-Instruct", 2, 1, 2, 60),
    ],
)
@pytest.mark.parametrize("vllm_use_v1", [True, False])
def test_vllm_vision_language_models(
    model_source, tp_size, pp_size, concurrency, sample_size, vllm_use_v1
):
    """Test vLLM with vision language models using different configurations."""

    if vllm_use_v1:
        runtime_env = dict(
            env_vars=dict(
                VLLM_USE_V1="1",
            ),
        )
        # vLLM v1 does not support decoupled tokenizer,
        # but since the tokenizer is in a separate process,
        # the overhead should be moderated.
        tokenize = False
        detokenize = False
    else:
        runtime_env = {}
        tokenize = True
        detokenize = True

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
        runtime_env=runtime_env,
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
