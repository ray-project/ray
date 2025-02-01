import pytest
import math
from unittest.mock import MagicMock, AsyncMock, patch

from ray.llm._internal.batch.stages.vllm_engine_stage import (
    vLLMEngineStage,
    vLLMEngineStageUDF,
)
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


def test_vllm_engine_stage_post_init():
    stage = vLLMEngineStage(
        fn_constructor_kwargs=dict(
            model="facebook/opt-125m",
            engine_kwargs=dict(
                tensor_parallel_size=4,
                pipeline_parallel_size=2,
                distributed_executor_backend="ray",
            ),
            task_type="generate",
            max_pending_requests=10,
        ),
        map_batches_kwargs=dict(
            zero_copy_batch=True,
            concurrency=1,
            max_concurrency=4,
            accelerator_type="L4",
        ),
    )

    assert stage.fn_constructor_kwargs == {
        "model": "facebook/opt-125m",
        "task_type": "generate",
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tensor_parallel_size": 4,
            "pipeline_parallel_size": 2,
            "distributed_executor_backend": "ray",
        },
    }
    scheduling_strategy = stage.map_batches_kwargs.pop("scheduling_strategy")
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 1,
        "max_concurrency": 4,
        "accelerator_type": "L4",
        "num_gpus": 0,
        "runtime_env": {},
    }
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)

    bundle_specs = scheduling_strategy.placement_group.bundle_specs
    assert len(bundle_specs) == 8
    for bundle_spec in bundle_specs:
        assert bundle_spec["accelerator_type:L4"] == 0.001
        assert bundle_spec["CPU"] == 1.0
        assert bundle_spec["GPU"] == 1.0


@pytest.mark.asyncio
async def test_vllm_engine_udf_basic(mock_vllm_wrapper):
    # Create UDF instance - it will use the mocked wrapper
    udf = vLLMEngineStageUDF(
        data_column="text",
        model="facebook/opt-125m",
        task_type="generate",
        engine_kwargs={
            # Test that this should be overridden by the stage.
            "model": "random-model",
            # Test that this should be overridden by the stage.
            "task": "random-task",
            "max_num_seqs": 100,
        },
    )

    assert udf.model == "facebook/opt-125m"
    assert udf.task_type == "generate"
    assert udf.engine_kwargs["task"] == "generate"
    assert udf.engine_kwargs["max_num_seqs"] == 100
    assert udf.max_pending_requests == math.ceil(100 * 1.1)

    # Test batch processing
    batch = [
        {"prompt": "Hello", "sampling_params": {"temperature": 0.7}},
        {"prompt": "World", "sampling_params": {"temperature": 0.7}},
    ]

    responses = []
    async for response in udf.udf(batch):
        responses.append(response)

    assert len(responses) == 2
    assert all("batch_uuid" in r for r in responses)
    assert all("time_taken_llm" in r for r in responses)
    assert responses[0]["prompt"] == "Hello"
    assert responses[1]["prompt"] == "World"
    assert responses[0]["num_input_tokens"] == 3
    assert responses[0]["num_generated_tokens"] == 3

    # Verify the wrapper was constructed with correct arguments
    mock_vllm_wrapper.assert_called_once_with(
        model="facebook/opt-125m",
        disable_log_stats=False,
        max_pending_requests=110,
        runtime_env={},
        task="generate",
        gpu_memory_utilization=0.95,
        use_v2_block_manager=True,
        enable_prefix_caching=False,
        enforce_eager=False,
        pipeline_parallel_size=1,
        max_num_seqs=100,
        tensor_parallel_size=1,
        max_logprobs=0,
    )


@pytest.mark.asyncio
async def test_vllm_engine_udf_expected_keys():
    udf = vLLMEngineStageUDF(
        data_column="text",
        model="facebook/opt-125m",
        engine_kwargs={},
        task_type="generate",
    )

    assert set(udf.expected_input_keys) == {"prompt", "sampling_params"}

    # Test embed task type
    udf_embed = vLLMEngineStageUDF(
        data_column="text",
        model="facebook/opt-125m",
        engine_kwargs={},
        task_type="embed",
    )

    assert set(udf_embed.expected_input_keys) == {"prompt"}


@pytest.mark.asyncio
async def test_vllm_engine_udf_shutdown(mock_vllm_wrapper):
    udf = vLLMEngineStageUDF(
        data_column="text",
        model="facebook/opt-125m",
        engine_kwargs={},
        task_type="generate",
    )

    del udf
    # Verify shutdown was called on the mock instance
    mock_vllm_wrapper.return_value.shutdown.assert_called_once()
