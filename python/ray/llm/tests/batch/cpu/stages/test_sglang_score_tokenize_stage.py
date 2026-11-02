import sys
from unittest.mock import patch

import numpy as np
import pytest

from ray.llm._internal.batch.stages.sglang_score_tokenize_stage import (
    SGLangScoreTokenizeStage,
    SGLangScoreTokenizeUDF,
    _sequence_special_tokens,
    resolve_marker_token,
)

BOS = 900
SEP = 901
MARKER = "<emb>"
MARKER_ID = 77


class FakeTokenizer:
    """Maps each character to its code point and `<emb>` to `MARKER_ID`.

    `metaspace` mimics SentencePiece by shifting a sequence's first token, and
    `split_marker=False` tokenizes the marker character by character.
    """

    unk_token_id = 0

    def __init__(self, prefix=(), suffix=(), metaspace=False, split_marker=True):
        self.prefix = list(prefix)
        self.suffix = list(suffix)
        self.metaspace = metaspace
        self.split_marker = split_marker
        self.batch_calls = []

    def _encode(self, text):
        parts = text.split(MARKER) if self.split_marker else [text]
        ids = []
        for i, part in enumerate(parts):
            if i:
                ids.append(MARKER_ID)
            ids.extend(ord(c) for c in part)
        if self.metaspace and text and not text.startswith(MARKER):
            ids[0] += 1000
        return ids

    def __call__(self, texts, add_special_tokens=True):
        single = isinstance(texts, str)
        if not single:
            self.batch_calls.append(list(texts))
        batch = [texts] if single else texts
        ids = [
            (self.prefix + self._encode(t) + self.suffix)
            if add_special_tokens
            else self._encode(t)
            for t in batch
        ]
        return {"input_ids": ids[0] if single else ids}


def _make_udf(tokenizer, **kwargs):
    with patch(
        "ray.llm._internal.batch.stages.sglang_score_tokenize_stage.load_tokenizer",
        return_value=tokenizer,
    ), patch(
        "ray.llm._internal.batch.stages.sglang_score_tokenize_stage.get_cached_tokenizer",
        side_effect=lambda t: t,
    ):
        return SGLangScoreTokenizeUDF(
            data_column="__data",
            expected_input_keys=["query", "items"],
            model="test-model",
            **kwargs,
        )


async def _run(udf, rows):
    batch = {"__data": [dict(row) for row in rows]}
    outputs = []
    async for output in udf(batch):
        outputs.extend(output["__data"])
    return outputs


@pytest.mark.parametrize(
    "prefix,suffix",
    [([], []), ([BOS], []), ([BOS], [SEP]), ([], [SEP])],
)
def test_sequence_special_tokens(prefix, suffix):
    tokenizer = FakeTokenizer(prefix=prefix, suffix=suffix)
    assert _sequence_special_tokens(tokenizer) == (prefix, suffix)


@pytest.mark.asyncio
async def test_score_tokenize_udf():
    tokenizer = FakeTokenizer(prefix=[BOS], suffix=[SEP])
    udf = _make_udf(tokenizer)
    rows = [
        {"query": "q", "items": ["ab", "c"]},
        {"query": "rs", "items": np.array(["d"])},
    ]

    outputs = await _run(udf, rows)

    assert tokenizer.batch_calls == [["q", "rs", "ab", "c", "d"]]
    assert [o["query_token_ids"] for o in outputs] == [
        [BOS, ord("q")],
        [BOS, ord("r"), ord("s")],
    ]
    assert [o["item_token_ids"] for o in outputs] == [
        [[ord("a"), ord("b"), SEP], [ord("c"), SEP]],
        [[ord("d"), SEP]],
    ]
    assert outputs[0]["query"] == "q"


@pytest.mark.asyncio
async def test_score_tokenize_udf_without_special_tokens():
    udf = _make_udf(FakeTokenizer(prefix=[BOS], suffix=[SEP]), add_special_tokens=False)
    [output] = await _run(udf, [{"query": "q", "items": ["a"]}])
    assert output["query_token_ids"] == [ord("q")]
    assert output["item_token_ids"] == [[ord("a")]]


@pytest.mark.parametrize(
    "row,match",
    [
        ({"query": [1, 2], "items": ["a"]}, "'query' must be text"),
        ({"query": "q", "items": "a"}, "'items' must be a list"),
        ({"query": "q", "items": [[1, 2]]}, "'items' must be text"),
    ],
)
@pytest.mark.asyncio
async def test_score_tokenize_udf_rejects_non_text(row, match):
    udf = _make_udf(FakeTokenizer())
    with pytest.raises(ValueError, match=match):
        await _run(udf, [row])


@pytest.mark.asyncio
async def test_score_tokenize_udf_checks_markers():
    udf = _make_udf(
        FakeTokenizer(),
        embed_override_token=MARKER,
        embed_override_token_id=MARKER_ID,
    )
    [output] = await _run(udf, [{"query": f"a{MARKER}", "items": [f"{MARKER}b"]}])
    assert output["query_token_ids"] == [ord("a"), MARKER_ID]

    udf.tokenizer.split_marker = False
    with pytest.raises(ValueError, match="occurs 1 times in 'query' text but 0"):
        await _run(udf, [{"query": f"a{MARKER}", "items": ["b"]}])


@pytest.mark.asyncio
async def test_score_tokenize_udf_checks_extraction_tokens():
    udf = _make_udf(
        FakeTokenizer(),
        score_extraction_token=MARKER,
        score_extraction_token_id=MARKER_ID,
    )
    [output] = await _run(udf, [{"query": "q", "items": [f"a{MARKER}b{MARKER}"]}])
    assert output["item_token_ids"] == [[ord("a"), MARKER_ID, ord("b"), MARKER_ID]]

    udf.tokenizer.split_marker = False
    with pytest.raises(ValueError, match="occurs 1 times in 'items' text but 0"):
        await _run(udf, [{"query": "q", "items": [f"a{MARKER}"]}])


@pytest.mark.parametrize(
    "prefix,suffix", [([MARKER_ID], []), ([], [MARKER_ID]), ([BOS], [MARKER_ID])]
)
@pytest.mark.parametrize("name", ["embed_override_token", "score_extraction_token"])
def test_score_tokenize_udf_rejects_marker_that_wraps_sequences(prefix, suffix, name):
    tokenizer = FakeTokenizer(prefix=prefix, suffix=suffix)
    with pytest.raises(ValueError, match="special token the tokenizer adds"):
        _make_udf(tokenizer, **{name: MARKER, f"{name}_id": MARKER_ID})


def test_score_tokenize_udf_allows_marker_without_special_tokens():
    tokenizer = FakeTokenizer(prefix=[BOS], suffix=[MARKER_ID])
    udf = _make_udf(
        tokenizer,
        add_special_tokens=False,
        score_extraction_token=MARKER,
        score_extraction_token_id=MARKER_ID,
    )
    assert udf.markers == [(MARKER, MARKER_ID)]


def test_score_tokenize_udf_requires_marker_id():
    with pytest.raises(ValueError, match="or neither"):
        _make_udf(FakeTokenizer(), embed_override_token=MARKER)


@pytest.mark.parametrize("metaspace", [False, True])
def test_resolve_marker_token(metaspace):
    tokenizer = FakeTokenizer(metaspace=metaspace)
    assert resolve_marker_token(tokenizer, MARKER, "embed_override_token") == MARKER_ID


@pytest.mark.parametrize(
    "tokenizer,token,match",
    [
        (FakeTokenizer(split_marker=False), MARKER, "tokenizes to 5 tokens"),
        (FakeTokenizer(), "\x00", "isn't in the tokenizer vocabulary"),
    ],
)
def test_resolve_marker_token_errors(tokenizer, token, match):
    with pytest.raises(ValueError, match=match):
        resolve_marker_token(tokenizer, token, "embed_override_token")


def test_resolve_marker_token_rejects_merging_marker():
    class MergingTokenizer(FakeTokenizer):
        def _encode(self, text):
            if text == MARKER:
                return [MARKER_ID]
            return [ord(c) for c in text]

    with pytest.raises(ValueError, match="merges with surrounding text"):
        resolve_marker_token(MergingTokenizer(), MARKER, "embed_override_token")


def test_score_tokenize_stage_input_keys():
    stage = SGLangScoreTokenizeStage(
        fn_constructor_kwargs=dict(model="test-model"),
        map_batches_kwargs=dict(concurrency=1),
    )
    assert set(stage.get_required_input_keys()) == {"query", "items"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
