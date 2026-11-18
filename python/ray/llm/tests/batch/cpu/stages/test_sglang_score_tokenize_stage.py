import sys

import numpy as np
import pytest

from ray.llm._internal.batch.stages.sglang_score_tokenize_stage import (
    SGLangScoreTokenizeUDF,
)

# Token IDs from the Qwen3 tokenizer.
QUERY = "Capital of France? "
QUERY_IDS = [63593, 315, 9625, 30, 220]
PARIS_IDS = [59604]
LYON_IDS = [43, 24990]
# Concatenated, the query's trailing space merges into the item's first token.
QUERY_PARIS_IDS = [63593, 315, 9625, 30, 12095]
QUERY_LYON_IDS = [63593, 315, 9625, 30, 55201]
ANCHOR = "<|box_start|>"
ANCHOR_ID = 151648


def _make_udf(tokenizer_path, **kwargs):
    return SGLangScoreTokenizeUDF(
        data_column="__data",
        expected_input_keys=["query", "items"],
        model=tokenizer_path,
        **kwargs,
    )


async def _run(udf, rows):
    batch = {"__data": [dict(row) for row in rows]}
    outputs = []
    async for output in udf(batch):
        outputs.extend(output["__data"])
    return outputs


@pytest.mark.asyncio
async def test_score_tokenize_concatenates_query_and_item(tokenizer_qwen3_0_6b):
    # Without MIS, SGLang tokenizes each `query + item` text as one sequence.
    udf = _make_udf(tokenizer_qwen3_0_6b)
    rows = [
        {"query": QUERY, "items": ["Paris", "Lyon"]},
        {"query": QUERY, "items": np.array(["Paris"])},
    ]

    outputs = await _run(udf, rows)

    assert [o["query_token_ids"] for o in outputs] == [[], []]
    assert [o["item_token_ids"] for o in outputs] == [
        [QUERY_PARIS_IDS, QUERY_LYON_IDS],
        [QUERY_PARIS_IDS],
    ]
    assert outputs[0]["query"] == QUERY


@pytest.mark.asyncio
async def test_score_tokenize_item_first(tokenizer_qwen3_0_6b):
    udf = _make_udf(tokenizer_qwen3_0_6b, item_first=True)
    [output] = await _run(udf, [{"query": QUERY, "items": ["Paris "]}])
    assert output["item_token_ids"] == [[59604, 18374, 315, 9625, 30, 220]]


@pytest.mark.asyncio
async def test_score_tokenize_mis_tokenizes_separately(
    tokenizer_qwen3_0_6b,
):
    udf = _make_udf(tokenizer_qwen3_0_6b, enable_mis=True)
    [output] = await _run(udf, [{"query": QUERY, "items": ["Paris", "Lyon"]}])
    assert output["query_token_ids"] == QUERY_IDS
    assert output["item_token_ids"] == [PARIS_IDS, LYON_IDS]


@pytest.mark.parametrize(
    "overrides",
    [
        {"query_embed_overrides": np.ones((1, 4))},
        {"item_embed_overrides": [None, np.ones((1, 4))]},
    ],
)
@pytest.mark.asyncio
async def test_score_tokenize_rows_with_overrides_tokenize_separately(
    tokenizer_qwen3_0_6b, overrides
):
    # SGLang tokenizes the query and items separately when a row has
    # embedding overrides, so marker positions are found in each sequence.
    udf = _make_udf(tokenizer_qwen3_0_6b)
    rows = [
        {"query": QUERY, "items": ["Paris", "Lyon"], **overrides},
        # Ray Data stores missing overrides as NaN next to array rows.
        {
            "query": QUERY,
            "items": ["Paris", "Lyon"],
            "query_embed_overrides": np.nan,
            "item_embed_overrides": [None, np.nan],
        },
    ]
    with_overrides, without = await _run(udf, rows)

    assert with_overrides["query_token_ids"] == QUERY_IDS
    assert with_overrides["item_token_ids"] == [PARIS_IDS, LYON_IDS]
    assert without["query_token_ids"] == []
    assert without["item_token_ids"] == [QUERY_PARIS_IDS, QUERY_LYON_IDS]


@pytest.mark.asyncio
async def test_score_tokenize_extraction_anchor_in_query(tokenizer_qwen3_0_6b):
    # The engine sees the concatenated sequence as one item, so SGLang's check
    # for an extraction token in the query has to run here.
    udf = _make_udf(tokenizer_qwen3_0_6b, score_extraction_token_id=ANCHOR_ID)
    [output] = await _run(udf, [{"query": QUERY, "items": [f"Paris{ANCHOR}"]}])
    assert output["item_token_ids"] == [QUERY_PARIS_IDS + [ANCHOR_ID]]

    with pytest.raises(ValueError, match="query contains the score extraction"):
        await _run(udf, [{"query": f"{QUERY}{ANCHOR}", "items": ["Paris"]}])


@pytest.mark.parametrize(
    "row,match",
    [
        ({"query": [1, 2], "items": ["a"]}, "'query' must be text"),
        ({"query": "q", "items": "a"}, "'items' must be a list"),
        ({"query": "q", "items": [[1, 2]]}, "'items' must be text"),
    ],
)
@pytest.mark.asyncio
async def test_score_tokenize_rejects_non_text(tokenizer_qwen3_0_6b, row, match):
    udf = _make_udf(tokenizer_qwen3_0_6b)
    with pytest.raises(ValueError, match=match):
        await _run(udf, [row])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
