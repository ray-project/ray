import asyncio
import json

import pytest
from pydantic import BaseModel

from ray.llm._internal.batch.constants import vLLMTaskType
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMEngineWrapper


@pytest.mark.asyncio
async def test_vllm_wrapper_generate():
    # TODO: Test v1 engine. The issue is once vLLM is imported with v0,
    # we cannot configure it to use v1, so we need a separate test for v1.

    wrapper = vLLMEngineWrapper(
        model="Qwen/Qwen3.5-0.8B",
        model_source="Qwen/Qwen3.5-0.8B",
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=2048,
        task_type=vLLMTaskType.GENERATE,
        # Older GPUs (e.g. T4) don't support bfloat16.
        dtype="half",
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": "Hello",
            "sampling_params": {
                "max_tokens": 10,
                "temperature": 0.7,
                "ignore_eos": True,
            },
        },
        {
            "__idx_in_batch": 1,
            "prompt": "World",
            "sampling_params": {
                "max_tokens": 5,
                "temperature": 0.7,
                "ignore_eos": True,
            },
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        request, output, time_taken_llm = await resp
        params = request.params
        max_tokens = params.max_tokens
        assert max_tokens == output["num_generated_tokens"]
        assert time_taken_llm > 0

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.asyncio
async def test_vllm_wrapper_embed(model_opt_125m):
    wrapper = vLLMEngineWrapper(
        model=model_opt_125m,
        model_source=model_opt_125m,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=2048,
        task_type=vLLMTaskType.EMBED,
        # Older GPUs (e.g. T4) don't support bfloat16.
        dtype="half",
    )

    batch = [
        {"__idx_in_batch": 0, "prompt": "Hello World"},
        {"__idx_in_batch": 1, "prompt": "How are you?"},
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        _, output, time_taken_llm = await resp
        assert output["embeddings"].shape == (768,)
        assert time_taken_llm > 0

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.parametrize(
    "pooling_params,tokenization_kwargs,expect_same_output",
    [
        ({}, None, True),
        # Truncation via tokenization_kwargs.
        (None, {"truncation": True, "max_length": 3}, False),
    ],
)
@pytest.mark.asyncio
async def test_vllm_wrapper_embed_pooling_params(
    model_opt_125m, pooling_params, tokenization_kwargs, expect_same_output
):
    prompt = "Hello! How's the weather?"
    wrapper = vLLMEngineWrapper(
        model=model_opt_125m,
        model_source=model_opt_125m,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=2048,
        task_type=vLLMTaskType.EMBED,
    )

    row_with_params = {
        "__idx_in_batch": 0,
        "prompt": prompt,
    }
    if pooling_params is not None:
        row_with_params["pooling_params"] = pooling_params
    if tokenization_kwargs is not None:
        row_with_params["tokenization_kwargs"] = tokenization_kwargs

    batch = [
        row_with_params,
        {
            "__idx_in_batch": 1,
            "prompt": prompt,
            # By default, no pooling params are applied.
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    outputs = {}
    for resp in asyncio.as_completed(tasks):
        request, output, time_taken_llm = await resp
        idx = request.idx_in_batch
        outputs[idx] = output

        assert output["embeddings"].shape == (768,)
        assert time_taken_llm > 0

    assert (
        outputs[0]["embeddings"] == outputs[1]["embeddings"]
    ).all() == expect_same_output

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.asyncio
async def test_vllm_wrapper_embed_long_prompt(model_opt_125m):
    # Preferred path: tokenization_kwargs truncation.
    pooling_params = None
    tokenization_kwargs = {"truncation": True, "max_length": 2048}
    # Sufficiently long prompt to trigger truncation to max_model_len
    max_model_len = 2048
    prompt = "Hello! How's the weather?" * 10_000
    wrapper = vLLMEngineWrapper(
        model=model_opt_125m,
        model_source=model_opt_125m,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=max_model_len,
        task_type=vLLMTaskType.EMBED,
    )

    row = {"__idx_in_batch": 0, "prompt": prompt}
    if pooling_params is not None:
        row["pooling_params"] = pooling_params
    if tokenization_kwargs is not None:
        row["tokenization_kwargs"] = tokenization_kwargs

    tasks = [asyncio.create_task(wrapper.generate_async(row))]

    for resp in asyncio.as_completed(tasks):
        _, output, time_taken_llm = await resp
        assert output["embeddings"].shape == (768,)
        assert output["num_input_tokens"] == max_model_len
        assert time_taken_llm > 0

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.parametrize("truncation_side", ["left", "right"])
@pytest.mark.asyncio
async def test_vllm_wrapper_generate_long_prompt(truncation_side):
    """A generate row's tokenization_kwargs must reach the engine.

    Without it the over-length prompt is rejected ("maximum context length")
    and the row fails. `max_length` leaves room for `max_tokens`, which is the
    caller's job: vLLM reserves no output budget when truncating to max_length.
    """
    max_model_len = 2048
    max_tokens = 16
    max_length = max_model_len - max_tokens
    wrapper = vLLMEngineWrapper(
        model="Qwen/Qwen3.5-0.8B",
        model_source="Qwen/Qwen3.5-0.8B",
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=max_model_len,
        task_type=vLLMTaskType.GENERATE,
    )

    # Distinct token IDs, so the surviving slice shows which end was cut.
    prompt_token_ids = list(range(100, 100 + 2 * max_model_len))
    row = {
        "__idx_in_batch": 0,
        "tokenized_prompt": prompt_token_ids,
        "sampling_params": {"max_tokens": max_tokens, "ignore_eos": True},
        "tokenization_kwargs": {
            "truncation": True,
            "max_length": max_length,
            "truncation_side": truncation_side,
        },
    }

    _, output, _ = await wrapper.generate_async(row)
    expected = (
        prompt_token_ids[-max_length:]
        if truncation_side == "left"
        else prompt_token_ids[:max_length]
    )
    assert output["prompt_token_ids"] == expected
    assert output["num_input_tokens"] == max_length
    assert output["num_generated_tokens"] == max_tokens

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.asyncio
async def test_vllm_wrapper_lora(model_llama_3_2_216M, model_llama_3_2_216M_lora):
    wrapper = vLLMEngineWrapper(
        model=model_llama_3_2_216M,
        model_source=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        task_type=vLLMTaskType.GENERATE,
        max_model_len=2048,
        enable_lora=True,
        max_lora_rank=16,
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": "Hello",
            "sampling_params": {
                "max_tokens": 10,
                "temperature": 0.7,
                "ignore_eos": True,
            },
            "model": model_llama_3_2_216M_lora,
        },
        {
            "__idx_in_batch": 1,
            "prompt": "World",
            "sampling_params": {
                "max_tokens": 5,
                "temperature": 0.7,
                "ignore_eos": True,
            },
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        request, output, time_taken_llm = await resp
        params = request.params
        max_tokens = params.max_tokens
        assert max_tokens == output["num_generated_tokens"]
        assert time_taken_llm > 0

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.asyncio
async def test_vllm_wrapper_json():
    """Test JSON output with the structured_outputs sampling param."""

    class AnswerModel(BaseModel):
        answer: int
        explain: str

    json_schema = AnswerModel.model_json_schema()

    wrapper = vLLMEngineWrapper(
        model="Qwen/Qwen3.5-0.8B",
        model_source="Qwen/Qwen3.5-0.8B",
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        task_type=vLLMTaskType.GENERATE,
        max_model_len=2048,
        structured_outputs_config={"backend": "xgrammar"},
        seed=42,
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": "Answer 2 ** 3 + 5. Return the answer in JSON. Expected fields: 'answer', 'explain'.",
            "sampling_params": {
                "max_tokens": 100,
                "temperature": 0.7,
                "structured_outputs": {"json": json_schema},
            },
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        _, output, time_taken_llm = await resp
        json_obj = json.loads(output["generated_text"])
        assert "answer" in json_obj
        assert isinstance(json_obj["answer"], int)
        assert "explain" in json_obj
        assert isinstance(json_obj["explain"], str)
        assert time_taken_llm > 0

    # Clean up GPU memory
    wrapper.shutdown()
