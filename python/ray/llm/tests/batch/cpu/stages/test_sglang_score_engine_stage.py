"""Score task tests for the SGLang engine stage. SGLang is mocked."""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from ray.llm._internal.batch.stages.sglang_engine_stage import (
    SGLangEngineStage,
    SGLangEngineStageUDF,
    SGLangEngineWrapper,
    SGLangTaskType,
)
from ray.llm._internal.batch.stages.sglang_score_task_handler import (
    _check_sglang_supports_score,
    _convert_to_embed_override_tensors,
)

torch = pytest.importorskip("torch")

_MODEL = "test-model"


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


def _score_wrapper(**score_options):
    return SGLangEngineWrapper(
        model=_MODEL,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.SCORE,
        task_options=score_options,
    )


def _score_row(**overrides):
    row = {
        "__idx_in_batch": 0,
        "query_token_ids": [1, 2, 3],
        "item_token_ids": [[4], [5, 6]],
    }
    row.update(overrides)
    return row


def test_score_stage_input_keys():
    stage = SGLangEngineStage(
        fn_constructor_kwargs=dict(
            model=_MODEL,
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


def test_score_wrapper_engine_kwargs(score_engine):
    SGLangEngineWrapper(
        model=_MODEL,
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.SCORE,
        task_options={"label_token_ids": [1]},
        enable_mis=True,
        attention_backend="flashinfer",
        skip_tokenizer_init=False,
    )
    kwargs = score_engine.engine_cls.call_args.kwargs
    assert kwargs["enable_mis"] is True
    assert kwargs["attention_backend"] == "flashinfer"
    assert kwargs["skip_tokenizer_init"] is True
    assert "label_token_ids" not in kwargs
    assert "task_options" not in kwargs


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
async def test_score_wrapper_calls_async_score(score_engine, query, items):
    wrapper = _score_wrapper(label_token_ids=[10, 20], apply_softmax=True)
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
    assert "score_extraction_token_id" not in kwargs
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
async def test_score_wrapper_rejects_invalid_token_ids(score_engine, overrides, match):
    wrapper = _score_wrapper()
    with pytest.raises(ValueError, match=match):
        await wrapper.generate_async(_score_row(**overrides))
    score_engine.async_score.assert_not_called()


@pytest.mark.asyncio
async def test_score_wrapper_embed_overrides(score_engine):
    wrapper = _score_wrapper(embed_override_token_id=42)
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
async def test_score_wrapper_all_none_item_overrides(score_engine):
    wrapper = _score_wrapper()
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
    score_engine, score_options, row_overrides, match
):
    wrapper = _score_wrapper(**score_options)
    with pytest.raises(ValueError, match=match):
        await wrapper.generate_async(_score_row(**row_overrides))


@pytest.mark.asyncio
async def test_score_wrapper_score_count_mismatch(score_engine):
    score_engine.async_score.side_effect = None
    score_engine.async_score.return_value = _FakeScoreResult([[0.5]])
    wrapper = _score_wrapper()
    with pytest.raises(RuntimeError, match="Expected 2 score lists, got 1"):
        await wrapper.generate_async(_score_row())


@pytest.mark.parametrize(
    "returned_rows,error",
    [((2, 1), None), ((1, 1), "Expected 2 score lists for item 0")],
)
@pytest.mark.asyncio
async def test_score_wrapper_score_extraction(score_engine, returned_rows, error):
    async def async_score(query, items, **kwargs):
        return _FakeScoreResult(
            [[[float(i), 0.5]] * n for i, n in enumerate(returned_rows)]
        )

    score_engine.async_score.side_effect = async_score
    wrapper = _score_wrapper(score_extraction_token_id=7)
    row = _score_row(item_token_ids=[[4, 7, 5, 7], [6, 7]])

    if error:
        with pytest.raises(RuntimeError, match=error):
            await wrapper.generate_async(row)
        return
    _, output, _ = await wrapper.generate_async(row)

    assert score_engine.async_score.call_args.kwargs["score_extraction_token_id"] == 7
    assert output["scores"] == [[[0.0, 0.5], [0.0, 0.5]], [[1.0, 0.5]]]


@pytest.mark.asyncio
async def test_score_udf(score_engine):
    udf = SGLangEngineStageUDF(
        data_column="__data",
        expected_input_keys=["query_token_ids", "item_token_ids"],
        model=_MODEL,
        task_type=SGLangTaskType.SCORE,
        engine_kwargs={"task": SGLangTaskType.SCORE},
        task_options={"label_token_ids": [1]},
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


def test_check_sglang_supports_score_extraction():
    engine_cls = type("Engine", (), {"async_score": _async_score_full})
    with pytest.raises(ImportError, match="score_extraction_token_id"):
        _check_sglang_supports_score(engine_cls, ["score_extraction_token_id"])


def test_score_wrapper_checks_sglang_before_engine_init():
    engine_cls = MagicMock(spec=[])
    with patch.dict(sys.modules, {"sglang": MagicMock(Engine=engine_cls)}):
        with pytest.raises(ImportError, match="async_score"):
            _score_wrapper()
    engine_cls.assert_not_called()


def _nested_object_array(entries):
    array = np.empty(len(entries), dtype=object)
    for i, entry in enumerate(entries):
        array[i] = entry
    return array


@pytest.mark.parametrize(
    "embeds,expected_count,dtype",
    [
        (np.ones((2, 3), dtype=np.float32), 2, torch.float32),
        (np.ones(3, dtype=np.float32), 1, torch.float32),
        ([[0.1, 0.2, 0.3]], 1, torch.float64),
        (_nested_object_array([np.ones(3), np.zeros(3)]), 2, torch.float64),
        (torch.ones(2, 3, dtype=torch.bfloat16), 2, torch.bfloat16),
        ([torch.ones(3, dtype=torch.bfloat16)] * 3, 3, torch.bfloat16),
        (
            _nested_object_array([torch.ones(3, dtype=torch.bfloat16)] * 2),
            2,
            torch.bfloat16,
        ),
    ],
    ids=[
        "ndarray_2d",
        "ndarray_1d",
        "list",
        "object_ndarray",
        "bf16_tensor",
        "bf16_tensor_list",
        "bf16_object_ndarray",
    ],
)
def test_convert_to_embed_override_tensors(embeds, expected_count, dtype):
    result = _convert_to_embed_override_tensors(embeds)
    assert len(result) == expected_count
    assert all(t.shape == (1, 3) and t.dtype == dtype for t in result)


@pytest.mark.parametrize("embeds", [None, [], np.zeros((0, 3))])
def test_convert_to_embed_override_tensors_empty(embeds):
    assert _convert_to_embed_override_tensors(embeds) is None


@pytest.mark.parametrize(
    "embeds,match",
    [
        (np.zeros((2, 2, 2)), "shaped"),
        (_nested_object_array([np.ones(2), np.ones(3)]), "same shape"),
        ([torch.ones(2), torch.ones(3)], "same shape"),
    ],
)
def test_convert_to_embed_override_tensors_errors(embeds, match):
    with pytest.raises(ValueError, match=match):
        _convert_to_embed_override_tensors(embeds)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
