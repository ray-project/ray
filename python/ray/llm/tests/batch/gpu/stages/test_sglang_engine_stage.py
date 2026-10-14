"""This test suite does not need sglang to be installed."""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from ray.data import ActorPoolStrategy
from ray.llm._internal.batch.stages.sglang_engine_stage import (
    SGLangEngineStage,
    SGLangEngineStageUDF,
    SGLangEngineWrapper,
    SGLangTaskType,
    _check_sglang_supports_score,
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
        score_options=None,
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


class _FakeScoreResult:
    def __init__(self, scores, prompt_tokens=7):
        self.scores = scores
        self.prompt_tokens = prompt_tokens


@pytest.fixture
def score_engine():
    """Patch `sglang.Engine` with a mock whose `async_score` returns one score per item."""
    engine = MagicMock()

    async def async_score(query, items, **kwargs):
        return _FakeScoreResult([[float(len(item))] for item in items])

    engine.async_score = AsyncMock(side_effect=async_score)
    engine_cls = MagicMock(return_value=engine)
    with patch.dict(sys.modules, {"sglang": MagicMock(Engine=engine_cls)}):
        engine.engine_cls = engine_cls
        yield engine


def _score_wrapper(model, **score_options):
    return SGLangEngineWrapper(
        model=model,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.SCORE,
        score_options=score_options,
    )


def _score_row(**overrides):
    row = {
        "__idx_in_batch": 0,
        "query_token_ids": [1, 2, 3],
        "item_token_ids": [[4], [5, 6]],
    }
    row.update(overrides)
    return row


def test_score_stage_input_keys(model_llama_3_2_216M):
    stage = SGLangEngineStage(
        fn_constructor_kwargs=dict(
            model=model_llama_3_2_216M,
            engine_kwargs={},
            task_type=SGLangTaskType.SCORE,
        ),
        map_batches_kwargs=dict(zero_copy_batch=True),
    )
    assert set(stage.get_required_input_keys()) == {
        "query_token_ids",
        "item_token_ids",
    }
    assert set(stage.get_optional_input_keys()) == {
        "query_embed_overrides",
        "item_embed_overrides",
    }


def test_score_wrapper_engine_kwargs(score_engine, model_llama_3_2_216M):
    SGLangEngineWrapper(
        model=model_llama_3_2_216M,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.SCORE,
        score_options={"label_token_ids": [1]},
        enable_mis=True,
        attention_backend="flashinfer",
        skip_tokenizer_init=False,
    )
    kwargs = score_engine.engine_cls.call_args.kwargs
    assert kwargs["enable_mis"] is True
    assert kwargs["attention_backend"] == "flashinfer"
    assert kwargs["skip_tokenizer_init"] is True
    assert "label_token_ids" not in kwargs
    assert "score_options" not in kwargs


@pytest.mark.parametrize(
    "query,items",
    [
        ([1, 2, 3], [[4], [5, 6]]),
        (np.array([1, 2, 3]), np.array([[4, 7], [5, 6]])),
        (
            np.array([1, 2, 3]),
            np.array([np.array([4]), np.array([5, 6])], dtype=object),
        ),
    ],
    ids=["lists", "uniform_ndarray", "ragged_ndarray"],
)
@pytest.mark.asyncio
async def test_score_wrapper_calls_async_score(
    score_engine, model_llama_3_2_216M, query, items
):
    wrapper = _score_wrapper(
        model_llama_3_2_216M, label_token_ids=[10, 20], apply_softmax=True
    )
    row = _score_row(query_token_ids=query, item_token_ids=items)

    request, output, _ = await wrapper.generate_async(row)

    kwargs = score_engine.async_score.call_args.kwargs
    assert kwargs["query"] == [1, 2, 3]
    assert all(type(t) is int for t in kwargs["query"])
    assert kwargs["items"] == [list(item) for item in items]
    assert kwargs["label_token_ids"] == [10, 20]
    assert kwargs["apply_softmax"] is True
    assert kwargs["query_embed_overrides"] is None
    assert kwargs["item_embed_overrides"] is None
    assert output == {
        "scores": [[float(len(item))] for item in items],
        "num_input_tokens": 7,
    }
    assert request.idx_in_batch == 0
    # The token ID columns stay on the row.
    assert "query_token_ids" in row and "item_token_ids" in row


@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"query_token_ids": "hello"}, "query_token_ids"),
        ({"query_token_ids": [1.5, 2]}, "query_token_ids"),
        ({"query_token_ids": [True, 2]}, "query_token_ids"),
        ({"item_token_ids": []}, "item_token_ids"),
        ({"item_token_ids": ["a", "b"]}, "item_token_ids"),
        ({"item_token_ids": [[1], [2.5]]}, "item_token_ids"),
    ],
)
@pytest.mark.asyncio
async def test_score_wrapper_rejects_invalid_token_ids(
    score_engine, model_llama_3_2_216M, overrides, match
):
    wrapper = _score_wrapper(model_llama_3_2_216M)
    with pytest.raises(ValueError, match=match):
        await wrapper.generate_async(_score_row(**overrides))
    score_engine.async_score.assert_not_called()


@pytest.mark.asyncio
async def test_score_wrapper_embed_overrides(score_engine, model_llama_3_2_216M):
    wrapper = _score_wrapper(model_llama_3_2_216M, embed_override_token_id=42)
    row = _score_row(
        query_embed_overrides=np.ones((2, 4), dtype=np.float32),
        item_embed_overrides=np.array(
            [None, np.zeros((1, 4), dtype=np.float32)], dtype=object
        ),
    )

    await wrapper.generate_async(row)

    kwargs = score_engine.async_score.call_args.kwargs
    assert kwargs["embed_override_token_id"] == 42
    assert [tuple(t.shape) for t in kwargs["query_embed_overrides"]] == [(1, 4)] * 2
    assert kwargs["item_embed_overrides"][0] is None
    assert tuple(kwargs["item_embed_overrides"][1][0].shape) == (1, 4)
    assert "query_embed_overrides" not in row
    assert "item_embed_overrides" not in row


@pytest.mark.asyncio
async def test_score_wrapper_all_none_item_overrides(
    score_engine, model_llama_3_2_216M
):
    wrapper = _score_wrapper(model_llama_3_2_216M)
    await wrapper.generate_async(_score_row(item_embed_overrides=[None, None]))
    assert score_engine.async_score.call_args.kwargs["item_embed_overrides"] is None


@pytest.mark.parametrize(
    "score_options,row_overrides,match",
    [
        ({}, {"query_embed_overrides": [[0.1, 0.2]]}, "embed_override_token"),
        (
            {"embed_override_token_id": 42, "item_first": True},
            {"query_embed_overrides": [[0.1, 0.2]]},
            "item_first",
        ),
        (
            {"embed_override_token_id": 42},
            {"item_embed_overrides": [[[0.1, 0.2]]]},
            "1 entries for 2 items",
        ),
    ],
)
@pytest.mark.asyncio
async def test_score_wrapper_embed_override_errors(
    score_engine, model_llama_3_2_216M, score_options, row_overrides, match
):
    wrapper = _score_wrapper(model_llama_3_2_216M, **score_options)
    with pytest.raises(ValueError, match=match):
        await wrapper.generate_async(_score_row(**row_overrides))


@pytest.mark.asyncio
async def test_score_wrapper_score_count_mismatch(score_engine, model_llama_3_2_216M):
    score_engine.async_score.side_effect = None
    score_engine.async_score.return_value = _FakeScoreResult([[0.5]])
    wrapper = _score_wrapper(model_llama_3_2_216M)
    with pytest.raises(RuntimeError, match="Expected 2 score lists, got 1"):
        await wrapper.generate_async(_score_row())


@pytest.mark.asyncio
async def test_score_udf(score_engine, model_llama_3_2_216M):
    udf = SGLangEngineStageUDF(
        data_column="__data",
        expected_input_keys=["query_token_ids", "item_token_ids"],
        model=model_llama_3_2_216M,
        task_type=SGLangTaskType.SCORE,
        engine_kwargs={"task": SGLangTaskType.SCORE},
        score_options={"label_token_ids": [1]},
    )
    batch = {
        "__data": [
            {"query_token_ids": [1], "item_token_ids": [[2]] * n, "n": n}
            for n in (1, 2, 3)
        ]
    }

    responses = []
    async for response in udf(batch):
        responses.extend(response["__data"])

    assert sorted(len(r["scores"]) for r in responses) == [1, 2, 3]
    assert all(len(r["scores"]) == r["n"] for r in responses)
    assert all("params" not in r for r in responses)


def _async_score_full(
    self,
    query,
    items,
    label_token_ids=None,
    apply_softmax=False,
    item_first=False,
    embed_override_token_id=None,
    query_embed_overrides=None,
    item_embed_overrides=None,
):
    pass


def _async_score_old(self, query, items, label_token_ids=None, apply_softmax=False):
    pass


def _async_score_var_kwargs(self, query, items, **kwargs):
    pass


@pytest.mark.parametrize(
    "async_score,error",
    [
        (_async_score_full, None),
        (_async_score_var_kwargs, None),
        (_async_score_old, "embed_override_token_id"),
        (None, "async_score"),
    ],
)
def test_check_sglang_supports_score(async_score, error):
    engine_cls = type(
        "Engine", (), {} if async_score is None else {"async_score": async_score}
    )
    if error is None:
        _check_sglang_supports_score(engine_cls)
    else:
        with pytest.raises(ImportError, match=error):
            _check_sglang_supports_score(engine_cls)


def test_score_wrapper_checks_sglang_before_engine_init(model_llama_3_2_216M):
    engine_cls = MagicMock(spec=[])
    with patch.dict(sys.modules, {"sglang": MagicMock(Engine=engine_cls)}):
        with pytest.raises(ImportError, match="async_score"):
            _score_wrapper(model_llama_3_2_216M)
    engine_cls.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
