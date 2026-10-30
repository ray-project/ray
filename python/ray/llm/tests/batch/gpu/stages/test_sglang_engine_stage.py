"""This test suite does not need sglang to be installed."""

import asyncio
import sys
from contextlib import nullcontext
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from ray.data import ActorPoolStrategy
from ray.llm._internal.batch.stages.sglang_engine_stage import (
    SGLangEngineRequest,
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


# ---------------------------------------------------------------------------
# Embed
# ---------------------------------------------------------------------------


async def _async_encode_with_input_ids(
    self, prompt=None, dimensions=None, input_ids=None
):
    pass


async def _async_encode_text_only(self, prompt, dimensions=None):
    pass


def _make_embed_wrapper(skip_tokenizer_init=True):
    """A wrapper with a mocked engine, bypassing engine construction."""
    wrapper = SGLangEngineWrapper.__new__(SGLangEngineWrapper)
    wrapper.request_id = 0
    wrapper.idx_in_batch_column = "__idx_in_batch"
    wrapper.task_type = SGLangTaskType.EMBED
    wrapper.skip_tokenizer_init = skip_tokenizer_init
    wrapper.semaphore = nullcontext()
    wrapper.engine = MagicMock()
    wrapper.engine.async_encode = AsyncMock(
        return_value={"embedding": [0.5, -0.5, 1.0], "meta_info": {"prompt_tokens": 3}}
    )
    return wrapper


def _embed_row(**overrides):
    row = {
        "prompt": "hello",
        "tokenized_prompt": np.array([1, 2, 3]),
        "__idx_in_batch": 0,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize("requested", [None, False, True])
def test_embed_wrapper_enables_embedding_mode(mock_sgl_engine, requested):
    kwargs = dict(
        model="model", idx_in_batch_column="__idx_in_batch", task=SGLangTaskType.EMBED
    )
    if requested is not None:
        kwargs["is_embedding"] = requested
    with patch("sglang.Engine") as mock_engine:
        mock_engine.async_encode = _async_encode_with_input_ids
        SGLangEngineWrapper(**kwargs)
    assert mock_engine.call_args.kwargs["is_embedding"] is True


def test_embed_wrapper_rejects_sglang_without_token_id_encode(mock_sgl_engine):
    """Token-in embedding needs `async_encode(input_ids=...)`; fail before the
    engine is built rather than on the first row."""
    with patch("sglang.Engine") as mock_engine:
        mock_engine.async_encode = _async_encode_text_only
        with pytest.raises(ValueError, match="does not accept token IDs"):
            SGLangEngineWrapper(
                model="model",
                idx_in_batch_column="__idx_in_batch",
                task=SGLangTaskType.EMBED,
            )
        mock_engine.assert_not_called()


def test_embed_wrapper_text_mode_works_with_older_sglang(mock_sgl_engine):
    with patch("sglang.Engine") as mock_engine:
        mock_engine.async_encode = _async_encode_text_only
        SGLangEngineWrapper(
            model="model",
            idx_in_batch_column="__idx_in_batch",
            task=SGLangTaskType.EMBED,
            skip_tokenizer_init=False,
        )
    mock_engine.assert_called_once()


@pytest.mark.asyncio
async def test_embed_sends_token_ids_and_returns_float32_array():
    wrapper = _make_embed_wrapper()
    request, output, _ = await wrapper.generate_async(
        _embed_row(embed_params={"dimensions": np.int64(2)})
    )

    wrapper.engine.async_encode.assert_awaited_once_with(
        input_ids=[1, 2, 3], dimensions=2
    )
    assert request.prompt_token_ids == [1, 2, 3]
    assert output["embeddings"].dtype == np.float32
    np.testing.assert_array_equal(output["embeddings"], [0.5, -0.5, 1.0])
    assert output["num_input_tokens"] == 3
    assert output["num_generated_tokens"] == 0
    assert output["prompt"] == "hello"


@pytest.mark.asyncio
async def test_embed_text_mode_sends_the_prompt():
    wrapper = _make_embed_wrapper(skip_tokenizer_init=False)
    row = _embed_row()
    del row["tokenized_prompt"]
    await wrapper.generate_async(row)
    wrapper.engine.async_encode.assert_awaited_once_with(prompt="hello")


@pytest.mark.asyncio
async def test_embed_converts_tensor_embeddings():
    torch = pytest.importorskip("torch")
    wrapper = _make_embed_wrapper()
    wrapper.engine.async_encode.return_value = {
        "embedding": torch.tensor([1.0, 2.0], dtype=torch.bfloat16),
        "meta_info": {"prompt_tokens": 3},
    }
    _, output, _ = await wrapper.generate_async(_embed_row())
    assert output["embeddings"].dtype == np.float32
    np.testing.assert_array_equal(output["embeddings"], [1.0, 2.0])


@pytest.mark.asyncio
async def test_embed_ignores_unset_embed_params():
    """Arrow struct columns fill keys other rows set with None."""
    wrapper = _make_embed_wrapper()
    await wrapper.generate_async(_embed_row(embed_params={"dimensions": None}))
    wrapper.engine.async_encode.assert_awaited_once_with(input_ids=[1, 2, 3])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "embed_params,match",
    [
        ({"normalize": True}, "Unsupported embed_params"),
        ({"dimensions": 0}, "positive integer"),
        ({"dimensions": True}, "positive integer"),
        ({"dimensions": 2.5}, "positive integer"),
    ],
)
async def test_embed_rejects_invalid_embed_params(embed_params, match):
    wrapper = _make_embed_wrapper()
    with pytest.raises(ValueError, match=match):
        await wrapper._prepare_llm_request(_embed_row(embed_params=embed_params))


def test_embed_output_without_embedding_is_rejected():
    request = SGLangEngineRequest(
        request_id=0, idx_in_batch=0, prompt="p", prompt_token_ids=None, params={}
    )
    with pytest.raises(ValueError, match="no embedding"):
        SGLangOutputData.from_embedding_output({"meta_info": {}}, request)


def test_sglang_stage_embed_input_keys():
    stage = SGLangEngineStage(
        fn_constructor_kwargs=dict(
            model="model", task_type=SGLangTaskType.EMBED, engine_kwargs={}
        ),
        map_batches_kwargs=dict(),
    )
    assert set(stage.get_required_input_keys()) == {"prompt"}
    assert set(stage.get_optional_input_keys()) == {"embed_params"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
