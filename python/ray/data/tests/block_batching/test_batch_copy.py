import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.batcher import Batcher
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
from ray.data.context import DataContext


@pytest.mark.parametrize("num_chunks", [1, 2, 8])
@pytest.mark.parametrize("batch_size", [None, 3, 8])
@pytest.mark.parametrize("with_strings", [False, True])
def test_numpy_batches_copy_once(monkeypatch, num_chunks, batch_size, with_strings):
    values = np.arange(8, dtype=np.int64)
    block = pa.table(
        {"value": pa.chunked_array([pa.array(c) for c in np.split(values, num_chunks)])}
    )
    if with_strings:
        block = block.append_column("text", pa.array([str(i) for i in values]))

    # Numeric columns must not be combined/copied as Arrow buffers before
    # allocating the final NumPy batch, including when slicing a block.
    original_combine = transform_pyarrow.combine_chunked_array

    def combine(column, *args, **kwargs):
        assert not pa.types.is_integer(column.type)
        return original_combine(column, *args, **kwargs)

    monkeypatch.setattr(transform_pyarrow, "combine_chunked_array", combine)
    batches = list(
        batch_blocks(
            iter([block]), batch_size=batch_size, batch_format="numpy", ensure_copy=True
        )
    )
    np.testing.assert_array_equal(np.concatenate([b["value"] for b in batches]), values)
    for batch in batches:
        assert batch["value"].flags.writeable
        assert not np.shares_memory(batch["value"], values)
        batch["value"] += 10
    np.testing.assert_array_equal(values, np.arange(8))
    assert block["value"].to_pylist() == list(range(8))


def test_numpy_concatenation_is_final_allocation(monkeypatch):
    block = pa.table({"value": pa.chunked_array([[1, 2], [3, 4]])})
    original_concatenate = np.concatenate
    allocations = []

    def concatenate(*args, **kwargs):
        result = original_concatenate(*args, **kwargs)
        allocations.append(result)
        return result

    monkeypatch.setattr(np, "concatenate", concatenate)
    batch = next(batch_blocks(iter([block]), batch_format="numpy", ensure_copy=True))
    assert len(allocations) == 1
    assert batch["value"] is allocations[0]


@pytest.mark.parametrize("batch_format", ["numpy", "pandas"])
@pytest.mark.parametrize("batch_size", [None, 3, 16])
@pytest.mark.parametrize("num_chunks", [8, 32])
@pytest.mark.parametrize("arrow_backed", [False, True])
def test_copy_batches_defragment_many_chunks(
    monkeypatch, batch_format, batch_size, num_chunks, arrow_backed
):
    monkeypatch.setattr(
        DataContext.get_current(), "enable_arrow_backed_pandas_conversion", arrow_backed
    )
    monkeypatch.setattr(
        transform_pyarrow, "MIN_NUM_CHUNKS_TO_TRIGGER_COMBINE_CHUNKS", 16
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
            batch_format=batch_format,
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

    # Compact a fragmented column once before repeated slicing. Avoid copying
    # lightly chunked columns, including when another column is compacted.
    should_compact = num_chunks == 32 and batch_size is not None
    assert combined_chunks == ([32] if should_compact else [])


@pytest.mark.parametrize("chunks", [[], [[]], [[], [1, 2], []]])
def test_writable_numpy_empty_chunks(chunks):
    column = pa.chunked_array(chunks, type=pa.int64())
    block = pa.table({"value": column})
    result = ArrowBlockAccessor(block).to_numpy(ensure_copy=True)["value"]
    np.testing.assert_array_equal(result, column.to_numpy())
    assert result.flags.writeable


def test_zero_copy_numpy_still_shares_source():
    values = np.arange(8)
    batch = next(
        batch_blocks(
            iter([pa.table({"value": values})]),
            batch_format="numpy",
            ensure_copy=False,
        )
    )
    assert np.shares_memory(batch["value"], values)
    assert not batch["value"].flags.writeable


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
@pytest.mark.parametrize("num_chunks", [1, 2])
def test_numpy_scalar_conversion(data_type, values, num_chunks):
    chunks = [
        pa.array(v, type=data_type) for v in np.split(np.array(values), num_chunks)
    ]
    block = pa.table({"value": pa.chunked_array(chunks)})
    accessor = ArrowBlockAccessor(block)
    expected = accessor.to_numpy()["value"]
    actual = accessor.to_numpy(ensure_copy=True)["value"]
    assert actual.dtype == expected.dtype
    np.testing.assert_array_equal(actual, expected)
    assert actual.flags.writeable
    actual[0] = actual[-1]
    assert block["value"].to_pylist() == pa.array(values, type=data_type).to_pylist()


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
        ensure_copy and batch_format not in ("numpy", "pandas")
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

    def unexpected_copy(*args, **kwargs):
        pytest.fail("Scalar pandas columns should not be copied before conversion")

    monkeypatch.setattr(transform_pyarrow, "combine_chunked_array", unexpected_copy)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
