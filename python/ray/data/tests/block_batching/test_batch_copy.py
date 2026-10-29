import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.batcher import Batcher
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
from ray.data.context import DataContext


@pytest.mark.parametrize("batch_size", [None, 3, 16])
@pytest.mark.parametrize("num_chunks", [8, 32])
@pytest.mark.parametrize("arrow_backed", [False, True])
def test_pandas_copy_preserves_chunk_compaction(
    monkeypatch, batch_size, num_chunks, arrow_backed
):
    monkeypatch.setattr(
        DataContext.get_current(), "enable_arrow_backed_pandas_conversion", arrow_backed
    )
    values = np.arange(128, dtype=np.int64)
    block = pa.table(
        {
            "fragmented": pa.chunked_array(
                [pa.array(chunk) for chunk in np.split(values, num_chunks)]
            ),
            "few_chunks": pa.chunked_array(
                [pa.array(chunk) for chunk in np.split(values, 2)]
            ),
        }
    )
    original_combine = transform_pyarrow.combine_chunked_array
    combined_chunks = []

    def combine(column, *args, **kwargs):
        combined_chunks.append(column.num_chunks)
        return original_combine(column, *args, **kwargs)

    monkeypatch.setattr(transform_pyarrow, "combine_chunked_array", combine)
    batches = list(
        batch_blocks(
            iter([block]),
            batch_size=batch_size,
            batch_format="pandas",
            ensure_copy=True,
        )
    )
    for name in block.column_names:
        np.testing.assert_array_equal(
            np.concatenate([batch[name] for batch in batches]), values
        )
        for batch in batches:
            batch[name] += 1
        assert block[name].to_pylist() == values.tolist()

    # Retain the existing compaction before repeated slicing. The optimization
    # only removes protective scalar copies; it does not change chunk handling.
    assert combined_chunks == ([num_chunks, 2] if batch_size is not None else [])


@pytest.mark.parametrize("batch_format", ["numpy", "pandas", "pyarrow", None])
@pytest.mark.parametrize("ensure_copy", [False, True])
def test_batcher_copy_per_column(batch_format, ensure_copy):
    block = pa.table({"value": [1, 2], "text": ["a", "b"]})
    batcher = Batcher(None, ensure_copy=ensure_copy, batch_format=batch_format)
    batcher.add(block)
    result = batcher.next_batch()

    def address(table, name):
        return table[name].chunk(0).buffers()[-1].address

    assert (address(result, "value") != address(block, "value")) == (
        ensure_copy and batch_format != "pandas"
    )
    assert (address(result, "text") != address(block, "text")) == ensure_copy


@pytest.mark.parametrize("arrow_backed", [False, True])
@pytest.mark.parametrize("batch_size", [None, 3])
@pytest.mark.parametrize("num_chunks", [1, 2])
def test_pandas_scalar_copy_isolation(
    monkeypatch, arrow_backed, batch_size, num_chunks
):
    monkeypatch.setattr(
        DataContext.get_current(), "enable_arrow_backed_pandas_conversion", arrow_backed
    )
    block = pa.table(
        {
            "value": pa.chunked_array(
                [pa.array(c) for c in np.split(np.arange(8), num_chunks)]
            )
        }
    )

    original_combine = transform_pyarrow.combine_chunked_array

    def combine(column, ensure_copy=False):
        assert not ensure_copy, "Scalar columns do not need a protective copy"
        return original_combine(column, ensure_copy=ensure_copy)

    monkeypatch.setattr(transform_pyarrow, "combine_chunked_array", combine)
    batches = list(
        batch_blocks(
            iter([block]),
            batch_size=batch_size,
            batch_format="pandas",
            ensure_copy=True,
        )
    )
    for batch in batches:
        batch["value"] += 10
    assert block["value"].to_pylist() == list(range(8))


@pytest.mark.parametrize("batch_format", ["numpy", "pandas"])
@pytest.mark.parametrize("arrow_backed", [False, True])
@pytest.mark.parametrize("ragged", [False, True])
def test_tensor_copy_isolation(monkeypatch, batch_format, arrow_backed, ragged):
    monkeypatch.setattr(
        DataContext.get_current(), "enable_arrow_backed_pandas_conversion", arrow_backed
    )
    values = np.ones((2, 2))
    if ragged:
        values = np.empty(2, dtype=object)
        values[0], values[1] = np.ones(1), np.ones(2)
    block = pa.table({"tensor": ArrowTensorArray.from_numpy(values), "id": [0, 1]})
    batch = next(
        batch_blocks(iter([block]), batch_format=batch_format, ensure_copy=True)
    )
    tensors = batch["tensor"]
    for i in range(2):
        tensors[i][...] += 10
    original = ArrowBlockAccessor(block).to_numpy()["tensor"]
    for row in original:
        np.testing.assert_array_equal(row, np.ones_like(row))


@pytest.mark.parametrize("arrow_backed", [False, True])
@pytest.mark.parametrize("num_chunks", [1, 2])
@pytest.mark.parametrize(
    "data_type,values",
    [
        (pa.int64(), [1, None, 3, 4]),
        (pa.uint64(), [1, None, 3, 4]),
        (pa.float64(), [1.0, None, 3.0, 4.0]),
        (pa.bool_(), [True, None, False, True]),
        (pa.null(), [None] * 4),
        (pa.timestamp("ns"), [0, None, 2, 3]),
        (pa.timestamp("us", tz="UTC"), [0, None, 2, 3]),
        (pa.date32(), [0, None, 2, 3]),
        (pa.duration("us"), [0, None, 2, 3]),
    ],
)
def test_pandas_scalar_types_remain_isolated(
    monkeypatch, arrow_backed, num_chunks, data_type, values
):
    monkeypatch.setattr(
        DataContext.get_current(), "enable_arrow_backed_pandas_conversion", arrow_backed
    )
    column = pa.array(values, type=data_type)
    chunk_size = len(values) // num_chunks
    block = pa.table(
        {
            "value": pa.chunked_array(
                [column.slice(i, chunk_size) for i in range(0, len(values), chunk_size)]
            )
        }
    )
    expected = ArrowBlockAccessor(block).to_pandas()
    batch = next(batch_blocks(iter([block]), batch_format="pandas", ensure_copy=True))
    pd.testing.assert_frame_equal(batch, expected)
    batch.iloc[0, 0] = batch.iloc[-1, 0]
    batch.iloc[1, 0] = batch.iloc[-1, 0]
    assert block["value"].to_pylist() == column.to_pylist()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
