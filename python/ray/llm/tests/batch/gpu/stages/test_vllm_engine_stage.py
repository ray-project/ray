import asyncio
import json
import pytest
import math
import sys
from unittest.mock import MagicMock, AsyncMock, patch

from pydantic import BaseModel

from ray.llm._internal.batch.stages.vllm_engine_stage import (
    vLLMEngineStage,
    vLLMEngineStageUDF,
    vLLMEngineWrapper,
)
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMTaskType
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.fixture
def mock_vllm_wrapper():
    with patch(
        "ray.llm._internal.batch.stages.vllm_engine_stage.vLLMEngineWrapper"
    ) as mock_wrapper:
        # Create a mock instance that will be returned by the wrapper class
        mock_instance = MagicMock()
        mock_instance.generate_async = AsyncMock()
        mock_instance.shutdown = MagicMock()

        # Configure the mock instance's behavior
        async def mock_generate(row):
            return (
                MagicMock(
                    request_id=0,
                    prompt=row["prompt"],
                    prompt_token_ids=None,
                    images=[],
                    params=row["sampling_params"],
                    idx_in_batch=row["__idx_in_batch"],
                ),
                {
                    "prompt": row["prompt"],
                    "prompt_token_ids": [1, 2, 3],
                    "num_input_tokens": 3,
                    "generated_tokens": [4, 5, 6],
                    "generated_text": f"Response to: {row['prompt']}",
                    "num_generated_tokens": 3,
                    "time_per_token": 0.1,
                },
            )

        mock_instance.generate_async.side_effect = mock_generate

        # Make the wrapper class return our mock instance
        mock_wrapper.return_value = mock_instance
        yield mock_wrapper


def test_vllm_engine_stage_post_init(gpu_type, model_llama_3_2_216M):
    stage = vLLMEngineStage(
        fn_constructor_kwargs=dict(
            model=model_llama_3_2_216M,
            engine_kwargs=dict(
                tensor_parallel_size=4,
                pipeline_parallel_size=2,
                distributed_executor_backend="ray",
            ),
            task_type=vLLMTaskType.GENERATE,
            max_pending_requests=10,
        ),
        map_batches_kwargs=dict(
            zero_copy_batch=True,
            concurrency=1,
            max_concurrency=4,
            accelerator_type=gpu_type,
        ),
    )

    assert stage.fn_constructor_kwargs == {
        "model": model_llama_3_2_216M,
        "task_type": vLLMTaskType.GENERATE,
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tensor_parallel_size": 4,
            "pipeline_parallel_size": 2,
            "distributed_executor_backend": "ray",
        },
    }
    ray_remote_args_fn = stage.map_batches_kwargs.pop("ray_remote_args_fn")
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 1,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 0,
    }
    scheduling_strategy = ray_remote_args_fn()["scheduling_strategy"]
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)

    bundle_specs = scheduling_strategy.placement_group.bundle_specs
    assert len(bundle_specs) == 8
    for bundle_spec in bundle_specs:
        assert bundle_spec[f"accelerator_type:{gpu_type}"] == 0.001
        assert bundle_spec["CPU"] == 1.0
        assert bundle_spec["GPU"] == 1.0


@pytest.mark.asyncio
async def test_vllm_engine_udf_basic(mock_vllm_wrapper, model_llama_3_2_216M):
    # Create UDF instance - it will use the mocked wrapper
    udf = vLLMEngineStageUDF(
        data_column="__data",
        model=model_llama_3_2_216M,
        task_type=vLLMTaskType.GENERATE,
        engine_kwargs={
            # Test that this should be overridden by the stage.
            "model": "random-model",
            # Test that this should be overridden by the stage.
            "task": vLLMTaskType.EMBED,
            "max_num_seqs": 100,
        },
    )

    assert udf.model == model_llama_3_2_216M
    assert udf.task_type == vLLMTaskType.GENERATE
    assert udf.engine_kwargs["task"] == vLLMTaskType.GENERATE
    assert udf.engine_kwargs["max_num_seqs"] == 100
    assert udf.max_pending_requests == math.ceil(100 * 1.1)

    # Test batch processing
    batch = {
        "__data": [
            {"prompt": "Hello", "sampling_params": {"temperature": 0.7}},
            {"prompt": "World", "sampling_params": {"temperature": 0.7}},
        ]
    }

    responses = []
    async for response in udf(batch):
        responses.append(response["__data"][0])

    assert len(responses) == 2
    assert all("batch_uuid" in r for r in responses)
    assert all("time_taken_llm" in r for r in responses)
    # The output order is not guaranteed.
    assert responses[0]["prompt"] in ["Hello", "World"]
    assert responses[1]["prompt"] in ["Hello", "World"]
    assert responses[0]["prompt"] != responses[1]["prompt"]

    # Verify the wrapper was constructed with correct arguments
    mock_vllm_wrapper.assert_called_once_with(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=False,
        max_pending_requests=111,
        task=vLLMTaskType.GENERATE,
        max_num_seqs=100,
    )


@pytest.mark.asyncio
async def test_vllm_wrapper_semaphore(model_llama_3_2_216M):
    from vllm.outputs import RequestOutput, CompletionOutput

    max_pending_requests = 2

    with (
        patch("vllm.AsyncLLMEngine") as mock_engine,
        patch(
            "ray.llm._internal.batch.stages.vllm_engine_stage.vLLMEngineWrapper.generate_async_v0"
        ) as mock_generate_async_v0,
    ):
        mock_engine.from_engine_args.return_value = AsyncMock()
        num_running_requests = 0
        request_lock = asyncio.Lock()

        # Configure mock engine's generate behavior to simulate delay
        async def mock_generate(request):
            nonlocal num_running_requests
            async with request_lock:
                num_running_requests += 1

            assert num_running_requests <= max_pending_requests
            await asyncio.sleep(0.3)

            async with request_lock:
                num_running_requests -= 1

            return RequestOutput(
                request_id="test",
                prompt="test",
                prompt_token_ids=[1, 2, 3],
                prompt_logprobs=None,
                metrics=None,
                outputs=[
                    CompletionOutput(
                        index=0,
                        text="test response",
                        token_ids=[4, 5, 6],
                        cumulative_logprob=None,
                        logprobs=None,
                    )
                ],
                finished=True,
            )

        mock_generate_async_v0.side_effect = mock_generate

        # Create wrapper with max 2 pending requests
        wrapper = vLLMEngineWrapper(
            model=model_llama_3_2_216M,
            idx_in_batch_column="__idx_in_batch",
            disable_log_stats=True,
            max_pending_requests=max_pending_requests,
        )

        # Create 10 requests
        batch = [
            {"__idx_in_batch": i, "prompt": f"Test {i}", "sampling_params": {}}
            for i in range(10)
        ]

        tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]
        await asyncio.gather(*tasks)

        # Verify all requests were processed
        assert mock_generate_async_v0.call_count == 10


@pytest.mark.asyncio
async def test_vllm_wrapper_generate(model_llama_3_2_216M):
    # TODO: Test v1 engine. The issue is once vLLM is imported with v0,
    # we cannot configure it to use v1, so we need a separate test for v1.

    wrapper = vLLMEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=2048,
        task=vLLMTaskType.GENERATE,
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
        request, output = await resp
        params = request.params
        max_tokens = params.max_tokens
        assert max_tokens == output["num_generated_tokens"]


@pytest.mark.asyncio
async def test_vllm_wrapper_embed(model_opt_125m):
    wrapper = vLLMEngineWrapper(
        model=model_opt_125m,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        max_model_len=2048,
        task=vLLMTaskType.EMBED,
        # Older GPUs (e.g. T4) don't support bfloat16.
        dtype="half",
    )

    batch = [
        {"__idx_in_batch": 0, "prompt": "Hello World"},
        {"__idx_in_batch": 1, "prompt": "How are you?"},
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        _, output = await resp
        assert output["embeddings"].shape == (768,)


@pytest.mark.asyncio
async def test_vllm_wrapper_lora(model_llama_3_2_216M, model_llama_3_2_216M_lora):
    wrapper = vLLMEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        task=vLLMTaskType.GENERATE,
        max_model_len=2048,
        # Older GPUs (e.g. T4) don't support bfloat16.
        dtype="half",
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
        request, output = await resp
        params = request.params
        max_tokens = params.max_tokens
        assert max_tokens == output["num_generated_tokens"]


@pytest.mark.asyncio
async def test_vllm_wrapper_json(model_llama_3_2_1B_instruct):
    """Test the JSON output with xgrammar backend. We have to use
    a real checkpoint as we need to verify the outputs.
    """

    class AnswerModel(BaseModel):
        answer: int
        explain: str

    json_schema = AnswerModel.model_json_schema()

    wrapper = vLLMEngineWrapper(
        model=model_llama_3_2_1B_instruct,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.8,
        task=vLLMTaskType.GENERATE,
        max_model_len=2048,
        guided_decoding_backend="xgrammar",
        # Older GPUs (e.g. T4) don't support bfloat16.
        dtype="half",
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": "Answer 2 ** 3 + 5 with a detailed explanation in JSON.",
            "sampling_params": {
                "max_tokens": 100,
                "temperature": 0.7,
                "guided_decoding": {"json": json_schema},
            },
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        _, output = await resp
        json_obj = json.loads(output["generated_text"])
        assert "answer" in json_obj
        assert isinstance(json_obj["answer"], int)
        assert "explain" in json_obj
        assert isinstance(json_obj["explain"], str)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
