import sys
from unittest.mock import patch

import numpy as np
import pytest

from ray.llm._internal.batch.stages.sglang_score_tokenize_stage import (
    SGLangScoreTokenizeStage,
    SGLangScoreTokenizeUDF,
)

BOS = 900
EOS = 901


class FakeTokenizer:
    """Maps each character to its code point, wrapped in BOS/EOS if asked."""

    def __init__(self):
        self.calls = []

    def __call__(self, texts, add_special_tokens=True):
        self.calls.append((list(texts), add_special_tokens))
        ids = [[ord(c) for c in text] for text in texts]
        if add_special_tokens:
            ids = [[BOS] + row + [EOS] for row in ids]
        return {"input_ids": ids}


def _ids(text, special=False):
    ids = [ord(c) for c in text]
    return [BOS] + ids + [EOS] if special else ids


def _make_udf(**kwargs):
    tokenizer = FakeTokenizer()
    module = "ray.llm._internal.batch.stages.tokenize_stage"
    with patch(f"{module}.download_model_files", return_value="path"), patch(
        "transformers.AutoTokenizer.from_pretrained", return_value=tokenizer
    ), patch(f"{module}.get_cached_tokenizer", side_effect=lambda t: t):
        udf = SGLangScoreTokenizeUDF(
            data_column="__data",
            expected_input_keys=["query", "items"],
            model="test-model",
            **kwargs,
        )
    return udf, tokenizer


async def _run(udf, rows):
    batch = {"__data": [dict(row) for row in rows]}
    outputs = []
    async for output in udf(batch):
        outputs.extend(output["__data"])
    return outputs


@pytest.mark.asyncio
async def test_score_tokenize_concatenates_query_and_item():
    # Without MIS, SGLang tokenizes each `query + item` text as one sequence.
    udf, tokenizer = _make_udf()
    rows = [
        {"query": "q", "items": ["ab", "c"]},
        {"query": "rs", "items": np.array(["d"])},
    ]

    outputs = await _run(udf, rows)

    assert tokenizer.calls == [(["qab", "qc", "rsd"], True)]
    assert [o["query_token_ids"] for o in outputs] == [[], []]
    assert [o["item_token_ids"] for o in outputs] == [
        [_ids("qab", True), _ids("qc", True)],
        [_ids("rsd", True)],
    ]
    assert outputs[0]["query"] == "q"


@pytest.mark.asyncio
async def test_score_tokenize_item_first():
    udf, _ = _make_udf(item_first=True)
    [output] = await _run(udf, [{"query": "q", "items": ["a"]}])
    assert output["item_token_ids"] == [_ids("aq", True)]


@pytest.mark.asyncio
async def test_score_tokenize_mis_adds_special_tokens_to_query_only():
    udf, tokenizer = _make_udf(enable_mis=True)
    outputs = await _run(
        udf,
        [{"query": "q", "items": ["ab", "c"]}, {"query": "r", "items": ["d"]}],
    )

    assert tokenizer.calls == [(["q", "r"], True), (["ab", "c", "d"], False)]
    assert [o["query_token_ids"] for o in outputs] == [_ids("q", True), _ids("r", True)]
    assert [o["item_token_ids"] for o in outputs] == [
        [_ids("ab"), _ids("c")],
        [_ids("d")],
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        {"query_embed_overrides": np.ones((1, 4))},
        {"item_embed_overrides": [None, np.ones((1, 4))]},
    ],
)
@pytest.mark.asyncio
async def test_score_tokenize_rows_with_overrides_tokenize_separately(overrides):
    # SGLang tokenizes the query and items separately when a row has
    # embedding overrides, so marker positions are found in each sequence.
    udf, _ = _make_udf()
    rows = [
        {"query": "q", "items": ["a", "b"], **overrides},
        # Ray Data stores missing overrides as NaN next to array rows.
        {
            "query": "r",
            "items": ["c", "d"],
            "query_embed_overrides": np.nan,
            "item_embed_overrides": [None, np.nan],
        },
    ]
    with_overrides, without = await _run(udf, rows)

    assert with_overrides["query_token_ids"] == _ids("q", True)
    assert with_overrides["item_token_ids"] == [_ids("a"), _ids("b")]
    assert without["query_token_ids"] == []
    assert without["item_token_ids"] == [_ids("rc", True), _ids("rd", True)]


@pytest.mark.asyncio
async def test_score_tokenize_extraction_anchor_in_query():
    # The engine sees the concatenated sequence as one item, so SGLang's check
    # for an extraction token in the query has to run here.
    udf, _ = _make_udf(score_extraction_token_id=ord("x"))
    [output] = await _run(udf, [{"query": "q", "items": ["axb"]}])
    assert output["item_token_ids"] == [_ids("qaxb", True)]

    with pytest.raises(ValueError, match="query contains the score extraction"):
        await _run(udf, [{"query": "qx", "items": ["a"]}])


@pytest.mark.asyncio
async def test_score_tokenize_without_special_tokens():
    udf, _ = _make_udf(add_special_tokens=False, enable_mis=True)
    [output] = await _run(udf, [{"query": "q", "items": ["a"]}])
    assert output["query_token_ids"] == _ids("q")
    assert output["item_token_ids"] == [_ids("a")]


@pytest.mark.parametrize(
    "row,match",
    [
        ({"query": [1, 2], "items": ["a"]}, "'query' must be text"),
        ({"query": "q", "items": "a"}, "'items' must be a list"),
        ({"query": "q", "items": [[1, 2]]}, "'items' must be text"),
    ],
)
@pytest.mark.asyncio
async def test_score_tokenize_rejects_non_text(row, match):
    udf, _ = _make_udf()
    with pytest.raises(ValueError, match=match):
        await _run(udf, [row])


def test_score_tokenize_stage_input_keys():
    stage = SGLangScoreTokenizeStage(
        fn_constructor_kwargs=dict(model="test-model"),
        map_batches_kwargs=dict(concurrency=1),
    )
    assert set(stage.get_required_input_keys()) == {"query", "items"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
