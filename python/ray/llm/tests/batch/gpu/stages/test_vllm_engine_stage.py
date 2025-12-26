import asyncio
import json
import math
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from ray.data import ActorPoolStrategy
from ray.llm._internal.batch.stages.vllm_engine_stage import (
    vLLMEngineStage,
    vLLMEngineStageUDF,
    vLLMEngineWrapper,
    vLLMOutputData,
    vLLMTaskType,
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
                0.1,  # time_taken_llm
            )

        mock_instance.generate_async.side_effect = mock_generate

        # Configure the scheduler config mock
        mock_scheduler_config = MagicMock()
        mock_scheduler_config.max_num_seqs = 128  # Current vLLM default
        mock_instance.get_scheduler_config.return_value = mock_scheduler_config

        # Configure the engine mock
        mock_engine = MagicMock()
        mock_engine.do_log_stats = AsyncMock()
        mock_instance.engine = mock_engine

        # Make the wrapper class return our mock instance
        mock_wrapper.return_value = mock_instance
        yield mock_wrapper


def test_vllm_engine_stage_post_init(gpu_type, model_llama_3_2_216M):
    stage = vLLMEngineStage(
        fn_constructor_kwargs=dict(
            model=model_llama_3_2_216M,
            engine_kwargs=dict(
                tensor_parallel_size=2,
                pipeline_parallel_size=2,
                distributed_executor_backend="ray",
            ),
            task_type=vLLMTaskType.GENERATE,
            max_pending_requests=10,
        ),
        map_batches_kwargs=dict(
            zero_copy_batch=True,
            compute=ActorPoolStrategy(size=1),
            max_concurrency=4,
            accelerator_type=gpu_type,
        ),
    )

    assert stage.fn_constructor_kwargs == {
        "model": model_llama_3_2_216M,
        "task_type": vLLMTaskType.GENERATE,
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tensor_parallel_size": 2,
            "pipeline_parallel_size": 2,
            "distributed_executor_backend": "ray",
        },
    }
    ray_remote_args_fn = stage.map_batches_kwargs.pop("ray_remote_args_fn")
    compute = stage.map_batches_kwargs.pop("compute")
    assert isinstance(compute, ActorPoolStrategy)
    assert compute.min_size == 1
    assert compute.max_size == 1

    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 0,
    }
    scheduling_strategy = ray_remote_args_fn()["scheduling_strategy"]
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)

    bundle_specs = scheduling_strategy.placement_group.bundle_specs
    assert len(bundle_specs) == 4
    for bundle_spec in bundle_specs:
        assert bundle_spec[f"accelerator_type:{gpu_type}"] == 0.001
        assert bundle_spec["CPU"] == 1.0
        assert bundle_spec["GPU"] == 1.0


@pytest.mark.asyncio
async def test_vllm_engine_udf_basic(mock_vllm_wrapper, model_llama_3_2_216M):
    # Create UDF instance - it will use the mocked wrapper
    udf = vLLMEngineStageUDF(
        data_column="__data",
        expected_input_keys=["prompt", "sampling_params"],
        model=model_llama_3_2_216M,
        task_type=vLLMTaskType.GENERATE,
        batch_size=32,
        max_concurrent_batches=4,
        engine_kwargs={
            # Test that this should be overridden by the stage.
            "model": "random-model",
            # Test that this should be overridden by the stage.
            "task": vLLMTaskType.EMBED,
            "max_num_seqs": 100,
            "disable_log_stats": False,
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
        responses.extend(response["__data"])

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
        model_source=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=False,
        max_pending_requests=111,
        task=vLLMTaskType.GENERATE,
        max_num_seqs=100,
        dynamic_lora_loading_path=None,
        enable_log_requests=False,
    )


@pytest.mark.asyncio
async def test_vllm_wrapper_semaphore(model_llama_3_2_216M):
    from vllm.outputs import CompletionOutput, RequestOutput

    max_pending_requests = 2

    with (
        patch("vllm.AsyncLLMEngine") as mock_engine,
        patch(
            "ray.llm._internal.batch.stages.vllm_engine_stage.vLLMEngineWrapper._generate_async"
        ) as mock_generate_async,
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

        mock_generate_async.side_effect = mock_generate

        # Create wrapper with max 2 pending requests
        wrapper = vLLMEngineWrapper(
            model=model_llama_3_2_216M,
            model_source=model_llama_3_2_216M,
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
        assert mock_generate_async.call_count == 10

        # Clean up GPU memory
        wrapper.shutdown()


@pytest.mark.asyncio
async def test_vllm_wrapper_generate(model_llama_3_2_216M):
    # TODO: Test v1 engine. The issue is once vLLM is imported with v0,
    # we cannot configure it to use v1, so we need a separate test for v1.

    wrapper = vLLMEngineWrapper(
        model=model_llama_3_2_216M,
        model_source=model_llama_3_2_216M,
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
        _, output, time_taken_llm = await resp
        assert output["embeddings"].shape == (768,)
        assert time_taken_llm > 0

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pooling_params,expect_same_output",
    [
        ({}, True),
        ({"truncate_prompt_tokens": 3}, False),
        ({"normalize": True}, False),
    ],
)
async def test_vllm_wrapper_embed_pooling_params(
    model_opt_125m, pooling_params, expect_same_output
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
        task=vLLMTaskType.EMBED,
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": prompt,
            "pooling_params": pooling_params,
        },
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

        # Validate pooling params for idx=0
        if idx == 0 and pooling_params:
            for key, expected_value in pooling_params.items():
                assert hasattr(request.params, key)
                actual_value = getattr(request.params, key)
                assert actual_value == expected_value

        assert output["embeddings"].shape == (768,)
        assert time_taken_llm > 0

    assert (
        outputs[0]["embeddings"] == outputs[1]["embeddings"]
    ).all() == expect_same_output

    # Clean up GPU memory
    wrapper.shutdown()


@pytest.mark.asyncio
async def test_vllm_wrapper_embed_long_prompt(model_opt_125m):
    # Sufficiently long prompt to trigger truncation to max_model_len
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
        max_model_len=2048,
        task=vLLMTaskType.EMBED,
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": prompt,
            # Long prompts shouldn't induce vLLM errors as prommpt length is truncated
            # to max_model_len when truncate_prompt_tokens is set to -1
            "pooling_params": {"truncate_prompt_tokens": -1},
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
        task=vLLMTaskType.GENERATE,
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
        model_source=model_llama_3_2_1B_instruct,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        task=vLLMTaskType.GENERATE,
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
                "guided_decoding": {"json": json_schema},
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


def test_vllm_output_data_logprobs():
    """Test that logprobs and prompt_logprobs are correctly extracted."""
    from vllm.logprobs import Logprob
    from vllm.outputs import CompletionOutput, RequestOutput

    logprobs = [
        {
            123: Logprob(logprob=-0.5, rank=1, decoded_token="hello"),
            456: Logprob(logprob=-1.2, rank=2, decoded_token="hi"),
        },
        {
            789: Logprob(logprob=-0.3, rank=1, decoded_token="world"),
            999: Logprob(logprob=-1.5, rank=2, decoded_token="earth"),
        },
    ]

    prompt_logprobs = [
        None,
        {
            111: Logprob(logprob=-0.1, rank=1, decoded_token="test"),
            222: Logprob(logprob=-0.8, rank=2, decoded_token="demo"),
        },
    ]

    request_output = RequestOutput(
        request_id="test",
        prompt="test prompt",
        prompt_token_ids=[1, 2],
        prompt_logprobs=prompt_logprobs,
        outputs=[
            CompletionOutput(
                index=0,
                text="hello world",
                token_ids=[123, 789],
                cumulative_logprob=-0.8,
                logprobs=logprobs,
            )
        ],
        finished=True,
    )

    output_data = vLLMOutputData.from_vllm_engine_output(request_output)

    expected_logprobs = [
        {
            123: {"logprob": -0.5, "rank": 1, "decoded_token": "hello"},
            456: {"logprob": -1.2, "rank": 2, "decoded_token": "hi"},
        },
        {
            789: {"logprob": -0.3, "rank": 1, "decoded_token": "world"},
            999: {"logprob": -1.5, "rank": 2, "decoded_token": "earth"},
        },
    ]
    assert output_data.logprobs == expected_logprobs

    expected_prompt_logprobs = [
        None,
        {
            111: {"logprob": -0.1, "rank": 1, "decoded_token": "test"},
            222: {"logprob": -0.8, "rank": 2, "decoded_token": "demo"},
        },
    ]
    assert output_data.prompt_logprobs == expected_prompt_logprobs

    dumped = output_data.model_dump()
    assert dumped["logprobs"] == expected_logprobs
    assert dumped["prompt_logprobs"] == expected_prompt_logprobs


def test_vllm_output_data_no_logprobs():
    """Test that None logprobs are handled correctly when not requested."""
    from vllm.outputs import CompletionOutput, RequestOutput

    request_output = RequestOutput(
        request_id="test",
        prompt="test prompt",
        prompt_token_ids=[1, 2],
        prompt_logprobs=None,
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

    output_data = vLLMOutputData.from_vllm_engine_output(request_output)

    assert output_data.logprobs is None
    assert output_data.prompt_logprobs is None

    dumped = output_data.model_dump()
    assert dumped["logprobs"] is None
    assert dumped["prompt_logprobs"] is None


@pytest.mark.asyncio
async def test_vllm_udf_default_raises_on_error(mock_vllm_wrapper):
    """Default behavior (should_continue_on_error=False) raises on inference error."""
    mock_vllm_wrapper.return_value.generate_async.side_effect = ValueError(
        "prompt too long"
    )

    udf = vLLMEngineStageUDF(
        data_column="__data",
        expected_input_keys=["prompt", "sampling_params"],
        model="/tmp/fake-model",
        task_type=vLLMTaskType.GENERATE,
        batch_size=32,
        max_concurrent_batches=4,
        engine_kwargs={},
        should_continue_on_error=False,
    )

    batch = {"__data": [{"prompt": "test", "sampling_params": {"temperature": 0.7}}]}

    with pytest.raises(ValueError, match="prompt too long"):
        async for _ in udf(batch):
            pass


@pytest.mark.asyncio
async def test_vllm_udf_should_continue_on_error_yields_error_row(mock_vllm_wrapper):
    """With should_continue_on_error=True, errors yield rows with __inference_error__."""
    mock_vllm_wrapper.return_value.generate_async.side_effect = ValueError(
        "prompt too long"
    )

    udf = vLLMEngineStageUDF(
        data_column="__data",
        expected_input_keys=["prompt", "sampling_params"],
        model="/tmp/fake-model",
        task_type=vLLMTaskType.GENERATE,
        batch_size=32,
        max_concurrent_batches=4,
        engine_kwargs={},
        should_continue_on_error=True,
    )

    batch = {
        "__data": [{"prompt": "test prompt", "sampling_params": {"temperature": 0.7}}]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 1
    assert "__inference_error__" in results[0]
    assert "ValueError" in results[0]["__inference_error__"]
    assert "prompt too long" in results[0]["__inference_error__"]
    # Error rows include the original prompt for debuggability
    assert results[0]["prompt"] == "test prompt"


@pytest.mark.asyncio
async def test_vllm_udf_mixed_success_and_error(mock_vllm_wrapper):
    """Mixed batch: some rows succeed, some fail."""
    call_count = 0

    async def mock_generate(row):
        nonlocal call_count
        call_count += 1
        idx = row["__idx_in_batch"]
        if idx == 1:
            raise ValueError("prompt too long")
        return (
            MagicMock(
                request_id=idx,
                prompt=row["prompt"],
                params=row["sampling_params"],
                idx_in_batch=idx,
            ),
            {
                "prompt": row["prompt"],
                "generated_text": f"Response to: {row['prompt']}",
            },
            0.1,
        )

    mock_vllm_wrapper.return_value.generate_async.side_effect = mock_generate

    udf = vLLMEngineStageUDF(
        data_column="__data",
        expected_input_keys=["prompt", "sampling_params"],
        model="/tmp/fake-model",
        task_type=vLLMTaskType.GENERATE,
        batch_size=32,
        max_concurrent_batches=4,
        engine_kwargs={},
        should_continue_on_error=True,
    )

    batch = {
        "__data": [
            {"prompt": "first", "sampling_params": {"temperature": 0.7}},
            {"prompt": "second", "sampling_params": {"temperature": 0.7}},
            {"prompt": "third", "sampling_params": {"temperature": 0.7}},
        ]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 3

    errors = [r for r in results if r.get("__inference_error__") is not None]
    successes = [r for r in results if r.get("__inference_error__") is None]

    assert len(errors) == 1
    assert len(successes) == 2
    assert "ValueError" in errors[0]["__inference_error__"]


@pytest.mark.asyncio
async def test_vllm_udf_fatal_error_always_raises(mock_vllm_wrapper):
    """Fatal errors (EngineDeadError) always propagate, even with should_continue_on_error=True."""
    from vllm.v1.engine.exceptions import EngineDeadError

    mock_vllm_wrapper.return_value.generate_async.side_effect = EngineDeadError()

    udf = vLLMEngineStageUDF(
        data_column="__data",
        expected_input_keys=["prompt", "sampling_params"],
        model="/tmp/fake-model",
        task_type=vLLMTaskType.GENERATE,
        batch_size=32,
        max_concurrent_batches=4,
        engine_kwargs={},
        should_continue_on_error=True,  # Even with this True, fatal errors should raise
    )

    batch = {"__data": [{"prompt": "test", "sampling_params": {"temperature": 0.7}}]}

    with pytest.raises(EngineDeadError):
        async for _ in udf(batch):
            pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
