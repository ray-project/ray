import sys
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.bucket_batching import bucket_batches, validate_bucket_batching
from ray.data.block import BlockAccessor
from ray.data.iterator import DataIterator


@pytest.mark.parametrize("block_type", [pa.table, pd.DataFrame])
@pytest.mark.parametrize("batch_format", [None, "numpy", "pandas", "pyarrow"])
def test_issue_example(block_type, batch_format):
    texts = ["1", "11", "1", "1111", "111", "1", "11", "11", "111"]
    block = block_type({"text": texts, "id": list(range(len(texts)))})
    length_fn = Mock(side_effect=lambda row: len(row["text"]))
    batches = list(bucket_batches(block, 5, length_fn, batch_format))
    rows = [
        list(
            BlockAccessor.for_block(BlockAccessor.batch_to_block(batch)).iter_rows(
                public_row_format=True
            )
        )
        for batch in batches
    ]
    assert [[row["text"] for row in batch] for batch in rows] == [
        ["1", "1", "1", "11"],
        ["11", "11"],
        ["111"],
        ["111"],
        ["1111"],
    ]
    assert sorted(row["id"] for batch in rows for row in batch) == list(range(9))
    assert length_fn.call_count == 9


@pytest.mark.parametrize("value", [0, -1, 1.5, True, None])
@pytest.mark.parametrize("argument", ["max_tokens", "buffer_size"])
def test_invalid_options(value, argument):
    options = dict(max_tokens=5, buffer_size=10, length_fn=len)
    options[argument] = value
    with pytest.raises(ValueError, match=argument):
        validate_bucket_batching(**options)


def test_invalid_callable():
    with pytest.raises(TypeError, match="length_fn"):
        validate_bucket_batching(5, 10, None)


@pytest.mark.parametrize("length", [-1, 1.5, True, None, float("nan")])
def test_invalid_lengths(length):
    with pytest.raises(ValueError, match="non-negative integer"):
        list(bucket_batches(pa.table({"id": [0]}), 5, lambda row: length, None))


def test_oversized_row():
    with pytest.raises(ValueError, match="exceeds max_tokens=5"):
        list(bucket_batches(pa.table({"text": ["123456"]}), 5, lambda r: 6, None))


def test_empty_and_zero_lengths():
    assert list(bucket_batches(pa.table({"id": []}), 5, lambda r: 0, None)) == []
    block = pa.table({"id": [0, 1, 2]})
    batches = list(bucket_batches(block, 5, lambda r: np.int64(0), "pyarrow"))
    assert len(batches) == 1
    assert batches[0].equals(block)


def test_preserves_arrow_schema():
    block = pa.table({"length": pa.array([2, 1, 3], type=pa.int16())})
    batches = list(bucket_batches(block, 3, lambda r: r["length"], "pyarrow"))
    assert [b.num_rows for b in batches] == [2, 1]
    assert all(b.schema == block.schema for b in batches)


@pytest.mark.parametrize("fail", [False, True])
def test_closes_input_iterator(fail):
    closed = []

    def windows(**kwargs):
        try:
            yield pa.table({"length": [1, 2, 3]})
        finally:
            closed.append(True)

    iterator = Mock()
    iterator.iter_batches = windows
    batches = iter(
        DataIterator.iter_bucket_batches(
            iterator, max_tokens=2 if fail else 3, length_fn=lambda r: r["length"]
        )
    )
    if fail:
        with pytest.raises(ValueError, match="exceeds"):
            next(batches)
    else:
        next(batches)
        batches.close()
    assert closed == [True]


@pytest.mark.parametrize("use_iterator", [False, True])
@pytest.mark.parametrize("buffer_size", [1, 4, 100])
@pytest.mark.parametrize("prefetch_batches", [0, 1])
def test_dataset_windows(
    ray_start_regular_shared, use_iterator, buffer_size, prefetch_batches
):
    lengths = [3, 1, 2, 5, 0, 4, 2, 1, 3]
    ds = ray.data.from_items(
        [{"id": i, "length": n} for i, n in enumerate(lengths)],
        override_num_blocks=3,
    )
    source = ds.iterator() if use_iterator else ds
    batches = source.iter_bucket_batches(
        max_tokens=5,
        length_fn=lambda row: row["length"],
        buffer_size=buffer_size,
        prefetch_batches=prefetch_batches,
    )
    for _ in range(2):
        ids = []
        for batch in batches:
            assert sum(batch["length"]) <= 5
            assert 0 < len(batch["id"]) <= buffer_size
            assert batch["length"].tolist() == sorted(batch["length"].tolist())
            ids.extend(batch["id"].tolist())
        assert sorted(ids) == list(range(len(lengths)))
        if buffer_size >= len(lengths):
            assert [lengths[i] for i in ids] == sorted(lengths)


def test_empty_dataset(ray_start_regular_shared):
    batches = ray.data.range(0).iter_bucket_batches(
        max_tokens=5, length_fn=lambda row: 1
    )
    assert list(batches) == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
