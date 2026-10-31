"""This test suite does not need sglang to be installed."""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ray.data import ActorPoolStrategy
from ray.llm._internal.batch.stages.sglang_engine_stage import (
    SGLangEngineStage,
    SGLangEngineStageUDF,
    SGLangEngineWrapper,
    SGLangOutputData,
    SGLangTaskType,
)


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
                    params=row["sampling_params"],
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
                0.1,  # time_taken_llm
            )

        mock_instance.generate_async.side_effect = mock_generate

        # Make the wrapper class return our mock instance
        mock_wrapper.return_value = mock_instance
        yield mock_wrapper


@pytest.fixture
def mock_sgl_engine():
    """Mock the SGLang engine and its _generate_async method."""
    with (
        patch(
            "ray.llm._internal.batch.stages.sglang_engine_stage.SGLangEngineWrapper._generate_async"
        ) as mock_generate_async,
    ):
        try:
            import sglang  # noqa: F401
        except ImportError:
            # Mock sglang module if it's not installed in test env.
            mock_sgl = MagicMock()
            mock_sgl.Engine = AsyncMock()
            sys.modules["sglang"] = mock_sgl
        num_running_requests = 0
        request_lock = asyncio.Lock()

        # Configure mock engine's generate behavior to simulate delay
        async def mock_generate(request):
            nonlocal num_running_requests
            async with request_lock:
                num_running_requests += 1

            # This will be checked in tests that use max_pending_requests
            max_pending_requests = getattr(mock_generate, "max_pending_requests", -1)
            if max_pending_requests > 0:
                assert num_running_requests <= max_pending_requests

            await asyncio.sleep(0.1)  # Reduced sleep time for faster tests

            async with request_lock:
                num_running_requests -= 1

            # Create a mock SGLang output
            return {
                "prompt": request.prompt,
                "prompt_token_ids": None,
                "text": f"Response to: {request.prompt}",
                "meta_info": {
                    "prompt_tokens": 3,
                    "completion_tokens": request.params.get("max_new_tokens", 3),
                    "finish_reason": "stop",
                },
                "output_ids": [4, 5, 6],
            }

        mock_generate_async.side_effect = mock_generate
        yield mock_generate_async


def test_sglang_engine_stage_post_init(gpu_type, model_llama_3_2_216M):
    stage = SGLangEngineStage(
        fn_constructor_kwargs=dict(
            model=model_llama_3_2_216M,
            engine_kwargs=dict(
                tp_size=2,
                dp_size=2,
            ),
            task_type=SGLangTaskType.GENERATE,
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
        "task_type": SGLangTaskType.GENERATE,
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tp_size": 2,
            "dp_size": 2,
        },
    }

    compute = stage.map_batches_kwargs.pop("compute")
    assert isinstance(compute, ActorPoolStrategy)
    assert compute.min_size == 1
    assert compute.max_size == 1

    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 4,
    }


@pytest.mark.asyncio
async def test_sglang_engine_udf_basic(mock_sglang_wrapper, model_llama_3_2_216M):
    # Create UDF instance - it will use the mocked wrapper
    udf = SGLangEngineStageUDF(
        data_column="__data",
        expected_input_keys=["prompt", "sampling_params"],
        model=model_llama_3_2_216M,
        task_type=SGLangTaskType.GENERATE,
        engine_kwargs={
            # Test that this should be overridden by the stage.
            "model": "random-model",
            # When reaching SGLangEngineStageUDF, this kwargs has been inserted by the processor
            # even though it is unset in the processor config.
            "task": SGLangTaskType.GENERATE,
        },
    )

    assert udf.model is not None
    assert udf.task_type == SGLangTaskType.GENERATE
    assert udf.engine_kwargs["task"] == SGLangTaskType.GENERATE
    assert udf.max_pending_requests == -1  # Default value for SGLang

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
    mock_sglang_wrapper.assert_called_once_with(
        model=udf.model,
        idx_in_batch_column="__idx_in_batch",
        max_pending_requests=-1,
        task=SGLangTaskType.GENERATE,
    )


@pytest.mark.parametrize("max_pending_requests,batch_size", [(2, 10), (-1, 5)])
@pytest.mark.asyncio
async def test_sglang_wrapper(
    mock_sgl_engine, model_llama_3_2_216M, max_pending_requests, batch_size
):
    """Test the SGLang wrapper with different configurations."""
    mock_generate_async = mock_sgl_engine

    # Set the max_pending_requests for assertion in the mock
    mock_generate_async.side_effect.max_pending_requests = max_pending_requests

    # Create wrapper with configured max_pending_requests
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        max_pending_requests=max_pending_requests,
        skip_tokenizer_init=False,
    )

    # Create batch requests with different sampling parameters
    batch = [
        {
            "__idx_in_batch": i,
            "prompt": f"Test {i}",
            "sampling_params": {
                "max_new_tokens": i + 5,
                "temperature": 0.7,
            },
        }
        for i in range(batch_size)
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]
    results = await asyncio.gather(*tasks)

    # Verify all requests were processed
    assert mock_generate_async.call_count == batch_size

    # Verify the outputs match expected values
    for i, (request, output, time_taken_llm) in enumerate(results):
        assert output["prompt"] == f"Test {i}"
        assert output["num_generated_tokens"] == i + 5  # max_new_tokens we set
        assert time_taken_llm > 0


@pytest.mark.asyncio
async def test_sglang_error_handling(model_llama_3_2_216M):
    """Test error handling when SGLang is not available."""
    with patch.dict(sys.modules, {"sglang": None}):
        with pytest.raises(ImportError, match="SGLang is not installed"):
            SGLangEngineWrapper(
                model=model_llama_3_2_216M,
                idx_in_batch_column="__idx_in_batch",
            )


@pytest.mark.asyncio
async def test_sglang_invalid_task_type(model_llama_3_2_216M, mock_sgl_engine):
    """Test handling of invalid task types."""
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    # Create a task type that doesn't exist in the prepare_llm_request method
    invalid_task_type = "invalid_task"
    wrapper.task_type = invalid_task_type

    with pytest.raises(ValueError, match=f"Unsupported task type: {invalid_task_type}"):
        await wrapper._prepare_llm_request(
            {
                "prompt": "Hello",
                "sampling_params": {"temperature": 0.7},
                "__idx_in_batch": 0,
            }
        )


@pytest.mark.asyncio
async def test_sglang_wrapper_uses_explicit_multimodal_data(
    model_llama_3_2_216M, mock_sgl_engine
):
    """Explicit multimodal data is propagated to the engine request and
    popped from the row.
    """
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    sentinel_image = object()
    sentinel_audio = object()
    sentinel_video = object()

    row = {
        "__idx_in_batch": 0,
        "prompt": "hi",
        "multimodal_data": {
            "image": [sentinel_image],
            "audio": [sentinel_audio],
            "video": [sentinel_video],
        },
        "multimodal_uuids": {"image": ["uuid-1"]},
        "mm_processor_kwargs": {"crop_size": 224},
        "sampling_params": {"max_tokens": 1, "temperature": 0.0},
    }

    request = await wrapper._prepare_llm_request(row)

    assert request.image_data == [sentinel_image]
    assert request.audio_data == [sentinel_audio]
    assert request.video_data == [sentinel_video]

    # Verify that media columns are popped from the row to avoid memory leaks
    assert "multimodal_data" not in row
    assert "multimodal_uuids" not in row
    assert "mm_processor_kwargs" not in row


@pytest.mark.asyncio
async def test_sglang_wrapper_direct_media_columns(
    model_llama_3_2_216M, mock_sgl_engine
):
    """Direct media columns (image_data, audio_data, video_data) are accepted
    and popped, while generic columns (e.g. image, audio, video) are not stolen.
    """
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    sentinel_image = object()
    sentinel_audio = object()
    sentinel_video = object()
    sentinel_user_image = object()

    row = {
        "__idx_in_batch": 0,
        "prompt": "hi",
        "image_data": [sentinel_image],
        "audio_data": sentinel_audio,
        "video_data": sentinel_video,
        "image": sentinel_user_image,  # Generic column that should not be stolen
        "sampling_params": {"max_tokens": 1, "temperature": 0.0},
    }

    request = await wrapper._prepare_llm_request(row)

    assert request.image_data == [sentinel_image]
    assert request.audio_data == sentinel_audio
    assert request.video_data == sentinel_video

    assert "image_data" not in row
    assert "audio_data" not in row
    assert "video_data" not in row
    assert row["image"] is sentinel_user_image


@pytest.mark.asyncio
async def test_sglang_wrapper_unsupported_modality_fails_closed(
    model_llama_3_2_216M, mock_sgl_engine
):
    """Unsupported modalities raise an explicit ValueError rather than
    silently degrading.
    """
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    row = {
        "__idx_in_batch": 0,
        "prompt": "hi",
        "multimodal_data": {"pointcloud": [1, 2, 3]},
        "sampling_params": {"temperature": 0.0},
    }

    with pytest.raises(ValueError, match="Unsupported multimodal modality"):
        await wrapper._prepare_llm_request(row)


@pytest.mark.asyncio
async def test_sglang_wrapper_conflicting_media_columns(
    model_llama_3_2_216M, mock_sgl_engine
):
    """Providing both multimodal_data and direct columns for the same modality
    raises an error.
    """
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    row = {
        "__idx_in_batch": 0,
        "prompt": "hi",
        "multimodal_data": {"image": ["img1"]},
        "image_data": ["img2"],
        "sampling_params": {"temperature": 0.0},
    }

    with pytest.raises(ValueError, match="Conflicting image data provided"):
        await wrapper._prepare_llm_request(row)


@pytest.mark.asyncio
async def test_sglang_wrapper_generate_forwards_media_kwargs(model_llama_3_2_216M):
    """Verify that _generate_async forwards media kwargs to engine.async_generate."""
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    mock_engine = AsyncMock()

    async def mock_async_generate(**kwargs):
        assert kwargs["image_data"] == ["test_image"]
        assert kwargs["audio_data"] == ["test_audio"]
        assert kwargs["video_data"] == ["test_video"]

        async def _stream():
            yield {
                "meta_info": {
                    "finish_reason": "stop",
                    "prompt_tokens": 5,
                    "completion_tokens": 10,
                },
                "output_ids": [1, 2, 3],
                "text": "test output",
            }

        return _stream()

    mock_engine.async_generate = mock_async_generate
    wrapper.engine = mock_engine

    row = {
        "__idx_in_batch": 0,
        "prompt": "Describe image, audio, and video",
        "multimodal_data": {
            "image": ["test_image"],
            "audio": ["test_audio"],
            "video": ["test_video"],
        },
        "sampling_params": {"temperature": 0.7},
    }

    request, output, time_taken = await wrapper.generate_async(row)
    assert request is not None
    assert output["prompt"] == "Describe image, audio, and video"
    assert output["generated_text"] == "test output"
    assert output["num_input_tokens"] == 5
    assert output["num_generated_tokens"] == 10
    # Output schema alignment fields
    assert "embeddings" in output
    assert "logprobs" in output
    assert "prompt_logprobs" in output
    assert time_taken > 0


@pytest.mark.asyncio
async def test_sglang_wrapper_engine_type_error_handling(model_llama_3_2_216M):
    """Verify that TypeError from engine on media kwargs is re-raised with a
    clear message.
    """
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
    )

    mock_engine = AsyncMock()
    mock_engine.async_generate.side_effect = TypeError(
        "async_generate() got an unexpected keyword argument 'audio_data'"
    )
    wrapper.engine = mock_engine

    row = {
        "__idx_in_batch": 0,
        "prompt": "Listen to this",
        "multimodal_data": {"audio": ["test_audio"]},
        "sampling_params": {"temperature": 0.7},
    }

    with pytest.raises(
        ValueError, match="The underlying SGLang engine rejected the multimodal input"
    ):
        await wrapper.generate_async(row)


def test_sglang_output_data_schema():
    """Verify SGLangOutputData schema contains all standard fields."""
    output = {
        "prompt": "hello",
        "prompt_token_ids": [1, 2],
        "meta_info": {
            "prompt_tokens": 2,
            "completion_tokens": 3,
            "finish_reason": "stop",
            "output_token_logprobs": [0.1, 0.2],
        },
        "output_ids": [3, 4, 5],
        "text": "world",
    }
    data = SGLangOutputData.from_sglang_engine_output(output)
    dump = data.model_dump()
    expected_fields = {
        "prompt",
        "prompt_token_ids",
        "num_input_tokens",
        "generated_tokens",
        "generated_text",
        "num_generated_tokens",
        "embeddings",
        "metrics",
        "logprobs",
        "prompt_logprobs",
    }
    assert expected_fields.issubset(set(dump.keys()))
    assert dump["logprobs"] == [0.1, 0.2]
    assert dump["embeddings"] is None


@pytest.mark.asyncio
async def test_sglang_wrapper_tokenized_prompt_only(
    model_llama_3_2_216M, mock_sgl_engine
):
    """Token-only requests without 'prompt' are accepted when skip_tokenizer_init=True."""
    wrapper = SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.GENERATE,
        skip_tokenizer_init=True,
    )

    row = {
        "__idx_in_batch": 0,
        "tokenized_prompt": [101, 2054, 2003, 102],
        "sampling_params": {"max_tokens": 1, "temperature": 0.0},
    }

    request = await wrapper._prepare_llm_request(row)

    assert request.prompt is None
    assert request.prompt_token_ids == [101, 2054, 2003, 102]
    assert "tokenized_prompt" not in row


def test_sglang_engine_stage_input_keys(gpu_type, model_llama_3_2_216M):
    """Verify get_required_input_keys does not require 'prompt'."""
    stage = SGLangEngineStage(
        fn_constructor_kwargs=dict(
            model=model_llama_3_2_216M,
            task_type=SGLangTaskType.GENERATE,
        ),
        map_batches_kwargs=dict(
            compute=ActorPoolStrategy(size=1),
            accelerator_type=gpu_type,
        ),
    )
    required_keys = stage.get_required_input_keys()
    assert "prompt" not in required_keys
    assert "sampling_params" in required_keys

    optional_keys = stage.get_optional_input_keys()
    assert "prompt" in optional_keys
    assert "tokenized_prompt" in optional_keys
    assert "multimodal_data" in optional_keys
    assert "image_data" in optional_keys
    assert "audio_data" in optional_keys
    assert "video_data" in optional_keys


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
