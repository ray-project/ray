import asyncio
import pytest
import sys
from unittest.mock import MagicMock, AsyncMock, patch


from ray.llm._internal.batch.stages.sglang_engine_stage import (
    SGLangEngineStage,
    SGLangEngineStageUDF,
    SGLangEngineWrapper,
)
from ray.llm._internal.batch.stages.sglang_engine_stage import SGLangTaskType


@pytest.fixture
def mock_sglang_wrapper():
    with patch(
        "ray.llm._internal.batch.stages.sglang_engine_stage.SGLangEngineWrapper"
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
                    params=row["params"],
                    idx_in_batch=row["__idx_in_batch"],
                ),
                {
                    "prompt": row["prompt"],
                    "prompt_token_ids": None,
                    "num_input_tokens": 3,
                    "generated_tokens": None,
                    "generated_text": f"Response to: {row['prompt']}",
                    "num_generated_tokens": 3,
                },
            )

        mock_instance.generate_async.side_effect = mock_generate

        # Make the wrapper class return our mock instance
        mock_wrapper.return_value = mock_instance
        yield mock_wrapper


def test_sglang_engine_stage_post_init(gpu_type, model_llama_3_2_216M):
    stage = SGLangEngineStage(
        fn_constructor_kwargs=dict(
            model=model_llama_3_2_216M,
            engine_kwargs=dict(
                tp=2,
                dp=2,
            ),
            task_type=SGLangTaskType.GENERATE,
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
        "task_type": SGLangTaskType.GENERATE,
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tp": 2,
            "dp": 2,
        },
    }
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 1,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 4,
    }


@pytest.mark.asyncio
async def test_sglang_engine_udf_basic(mock_sglang_wrapper, model_llama_3_2_216M):
    # Create UDF instance - it will use the mocked wrapper
    udf = SGLangEngineStageUDF(
        data_column="__data",
        model=model_llama_3_2_216M,
        task_type=SGLangTaskType.GENERATE,
        engine_kwargs={
            # Test that this should be overridden by the stage.
            "model": "random-model",
        },
    )

    assert udf.model is not None
    assert udf.task_type == SGLangTaskType.GENERATE
    assert udf.engine_kwargs["task"] == SGLangTaskType.GENERATE
    assert udf.max_pending_requests == -1  # Default value for SGLang

    # Test batch processing
    batch = {
        "__data": [
            {"prompt": "Hello", "params": {"temperature": 0.7}},
            {"prompt": "World", "params": {"temperature": 0.7}},
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
    mock_sglang_wrapper.assert_called_once_with(
        model=udf.model,
        idx_in_batch_column="__idx_in_batch",
        max_pending_requests=-1,
        task=SGLangTaskType.GENERATE,
    )


@pytest.mark.asyncio
async def test_sglang_wrapper_semaphore(model_llama_3_2_216M):
    max_pending_requests = 2

    with (
        patch("sglang.Engine") as mock_engine,
        patch(
            "ray.llm._internal.batch.stages.sglang_engine_stage.SGLangEngineWrapper._generate_async"
        ) as mock_generate_async,
    ):
        mock_engine.return_value = AsyncMock()
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

            # Create a mock SGLang output
            return {
                "prompt": request.prompt,
                "prompt_token_ids": None,
                "text": f"Response to: {request.prompt}",
                "meta_info": {
                    "prompt_tokens": 3,
                    "completion_tokens": 3,
                },
                "output_ids": [4, 5, 6],
            }

        mock_generate_async.side_effect = mock_generate

        # Create wrapper with max 2 pending requests
        wrapper = SGLangEngineWrapper(
            model=model_llama_3_2_216M,
            idx_in_batch_column="__idx_in_batch",
            max_pending_requests=max_pending_requests,
        )

        # Create 10 requests
        batch = [
            {"__idx_in_batch": i, "prompt": f"Test {i}", "params": {}}
            for i in range(10)
        ]

        tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]
        await asyncio.gather(*tasks)

        # Verify all requests were processed
        assert mock_generate_async.call_count == 10


@pytest.mark.asyncio
async def test_sglang_wrapper_generate(model_llama_3_2_216M):
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        disable_cuda_graph=True,
        context_length=2048,
        task=SGLangTaskType.GENERATE,
        # Older GPUs (e.g. T4) don't support bfloat16.
        dtype="half",
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": "Hello",
            "params": {
                "max_new_tokens": 10,
                "temperature": 0.7,
            },
        },
        {
            "__idx_in_batch": 1,
            "prompt": "World",
            "params": {
                "max_new_tokens": 5,
                "temperature": 0.7,
            },
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        request, output = await resp
        max_new_tokens = request.params["max_new_tokens"]
        assert max_new_tokens == output["num_generated_tokens"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
