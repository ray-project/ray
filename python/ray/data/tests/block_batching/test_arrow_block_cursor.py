import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.batcher import Batcher
from ray.data._internal.block_batching.arrow_block_cursor import (
    _AlignedArrowBlockCursor,
    _ColumnArrowBlockCursor,
    create_arrow_block_cursor,
)


def _table(aligned):
    values = pa.array(np.arange(16))
    other_values = pa.array(np.arange(16) + 100)
    cuts = [0, 4, 8, 12, 16] if aligned else [0, 3, 7, 11, 16]
    schema = pa.schema(
        [
            pa.field("a", pa.int64(), metadata={b"field": b"value"}),
            pa.field("b", pa.int64()),
        ],
        metadata={b"table": b"value"},
    )
    table = pa.Table.from_arrays(
        [
            pa.chunked_array([values.slice(i, 4) for i in range(0, 16, 4)]),
            pa.chunked_array(
                [other_values.slice(a, b - a) for a, b in zip(cuts, cuts[1:])]
            ),
        ],
        schema=schema,
    )
    if aligned == "mixed":
        table = table.append_column("dense", pa.array(np.arange(16) + 200))
    return table


@pytest.mark.parametrize("aligned", [False, True, "mixed"])
def test_peek_does_not_advance(aligned):
    source = _table(aligned)
    cursor = create_arrow_block_cursor(source)
    assert cursor.peek(9).equals(source.slice(0, 9), check_metadata=True)
    assert cursor.peek(1).equals(source.slice(0, 1), check_metadata=True)
    cursor.advance(1)
    assert cursor.peek(7).equals(source.slice(1, 7), check_metadata=True)
    cursor.advance(7)
    assert cursor.peek(8).equals(source.slice(8, 8), check_metadata=True)


@pytest.mark.parametrize("aligned", [False, True, "mixed"])
def test_failed_peek_can_be_retried(monkeypatch, aligned):
    source = _table(aligned)
    cursor = create_arrow_block_cursor(source)
    cursor.peek(1)
    cursor.advance(1)
    if isinstance(cursor, _AlignedArrowBlockCursor):
        reader = cursor._reader

        class FailingReader:
            calls = 0

            def read_next_batch(self):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("view failed")
                return reader.read_next_batch()

        monkeypatch.setattr(cursor, "_reader", FailingReader())
        with pytest.raises(RuntimeError, match="view failed"):
            cursor.peek(9)
    else:
        assert isinstance(cursor, _ColumnArrowBlockCursor)

        def fail(*args, **kwargs):
            raise RuntimeError("view failed")

        with monkeypatch.context() as patch:
            patch.setattr(cursor, "_make_table", fail)
            with pytest.raises(RuntimeError, match="view failed"):
                cursor.peek(9)
    assert cursor.peek(9).equals(source.slice(1, 9), check_metadata=True)
    cursor.advance(9)
    assert cursor.peek(6).equals(source.slice(10, 6), check_metadata=True)


@pytest.mark.parametrize("sliced", [False, True])
@pytest.mark.parametrize(
    "cuts,cursor_type",
    [
        pytest.param([[0, 4, 9, 19]], _AlignedArrowBlockCursor, id="single-column"),
        pytest.param(
            [[0, 4, 9, 19], [0, 4, 9, 19]],
            _AlignedArrowBlockCursor,
            id="aligned",
        ),
        pytest.param(
            [[0, 4, 9, 19], [0, 3, 8, 19]],
            _ColumnArrowBlockCursor,
            id="same-count-different-boundaries",
        ),
        pytest.param(
            [[0, 4, 9, 19], [0, 5, 19]],
            _ColumnArrowBlockCursor,
            id="different-counts",
        ),
        pytest.param(
            [[0, 19], [0, 4, 9, 19]], _ColumnArrowBlockCursor, id="dense-and-chunked"
        ),
        pytest.param(
            [[0, 0, 4, 4, 9, 19, 19]] * 2,
            _AlignedArrowBlockCursor,
            id="aligned-empty-chunks",
        ),
        pytest.param(
            [[0, 0, 4, 4, 9, 19, 19], [0, 3, 3, 8, 19, 19]],
            _ColumnArrowBlockCursor,
            id="misaligned-empty-chunks",
        ),
        pytest.param(
            [[0, 19], [0, 0, 4, 4, 9, 19, 19]],
            _ColumnArrowBlockCursor,
            id="dense-and-empty-chunks",
        ),
    ],
)
def test_cursor_layouts(cuts, cursor_type, sliced):
    columns = []
    for index, boundaries in enumerate(cuts):
        values = pa.array(
            [None if row % 7 == 3 else index * 100 + row for row in range(23)],
            type=pa.int64(),
        ).slice(2, 19)
        columns.append(
            pa.chunked_array(
                [values.slice(a, b - a) for a, b in zip(boundaries, boundaries[1:])]
            )
        )
    schema = pa.schema(
        [
            pa.field(str(i), column.type, metadata={b"column": str(i).encode()})
            for i, column in enumerate(columns)
        ],
        metadata={b"table": b"value"},
    )
    source = pa.Table.from_arrays(columns, schema=schema)
    if sliced:
        source = source.slice(1, 17)
    source_buffers = [
        {
            buffer.address
            for chunk in column.chunks
            for buffer in chunk.buffers()
            if buffer is not None
        }
        for column in source.columns
    ]
    cursor = create_arrow_block_cursor(source)
    assert isinstance(cursor, cursor_type)
    rng = np.random.default_rng(0)
    offset = 0
    while offset < len(source):
        remaining = len(source) - offset
        num_rows = remaining if offset == 0 else int(rng.integers(1, remaining + 1))
        for requested in (num_rows, 1, num_rows):
            batch = cursor.peek(requested)
            assert batch.equals(source.slice(offset, requested), check_metadata=True)
            for column, buffers in zip(batch.columns, source_buffers):
                for chunk in column.chunks:
                    if len(chunk):
                        assert all(
                            buffer.address in buffers
                            for buffer in chunk.buffers()
                            if buffer is not None
                        )
        consumed = min(3, num_rows)
        cursor.advance(consumed)
        offset += consumed


@pytest.mark.parametrize("prefix_rows", [0, 1])
@pytest.mark.parametrize("sparse_first", [False, True])
@pytest.mark.parametrize("num_sparse_columns", [1, 2])
def test_continuous_columns_stay_continuous(
    prefix_rows, sparse_first, num_sparse_columns
):
    values = np.arange(8192, dtype=np.float64)
    sparse_values = values + 10000
    source = pa.table(
        {
            "dense": values,
            "sparse": pa.chunked_array(
                [pa.array(sparse_values[i : i + 64]) for i in range(0, len(values), 64)]
            ),
        }
    )
    if sparse_first:
        source = source.select(["sparse", "dense"])
    if num_sparse_columns == 2:
        other_values = values + 20000
        source = source.append_column(
            "sparse_two",
            pa.chunked_array(
                [pa.array(other_values[i : i + 32]) for i in range(0, len(values), 32)]
            ),
        )
    schema = pa.schema(
        [
            field.with_metadata({b"column": field.name.encode()})
            for field in source.schema
        ],
        metadata={b"table": b"value"},
    )
    source = pa.Table.from_arrays(source.columns, schema=schema)
    batcher = Batcher(batch_size=512)
    if prefix_rows:
        batcher.add(source.slice(0, prefix_rows))
    batcher.add(source)
    batcher.done_adding()
    batches = []
    while batcher.has_any():
        batch = batcher.next_batch()
        if not prefix_rows or batches:
            assert batch["dense"].num_chunks == 1
            before = pa.total_allocated_bytes()
            arrays = ArrowBlockAccessor(batch).to_numpy()
            assert np.shares_memory(arrays["dense"], values)
            # Only the genuinely fragmented column needs a contiguous copy.
            assert (
                pa.total_allocated_bytes() - before
                <= batch.num_rows * 8 * num_sparse_columns
            )
            del arrays
        batches.append(batch)
    expected = pa.concat_tables([source.slice(0, prefix_rows), source])
    assert pa.concat_tables(batches).equals(expected, check_metadata=True)


def test_columns_do_not_expand_to_union_of_boundaries():
    columns = []
    for shift in range(4):
        values = pa.array(np.arange(16) + shift * 100)
        cuts = [0, 2 + shift, 6 + shift, 10 + shift, 16]
        columns.append(
            pa.chunked_array([values.slice(a, b - a) for a, b in zip(cuts, cuts[1:])])
        )
    source = pa.Table.from_arrays(columns, names=[str(i) for i in range(4)])
    cursor = create_arrow_block_cursor(source)
    batch = cursor.peek(12)
    assert batch.equals(source.slice(0, 12))
    assert sum(c.num_chunks for c in batch.columns) <= sum(
        c.num_chunks for c in source.columns
    )


@pytest.mark.parametrize("batch_size", [1, 17, 256])
def test_column_chunk_visits_are_linear(monkeypatch, batch_size):
    columns = []
    for shift in (0, 3, 7):
        values = pa.array(np.arange(512) + shift * 100)
        cuts = [0, *range(16 + shift, 512, 16), 512]
        columns.append(
            pa.chunked_array([values.slice(a, b - a) for a, b in zip(cuts, cuts[1:])])
        )
    source = pa.Table.from_arrays(columns, names=["a", "b", "c"])
    cursor = _ColumnArrowBlockCursor(source)
    chunk_reads = []

    class CountingColumn:
        def __init__(self, column):
            self.column = column
            self.type = column.type

        def chunk(self, index):
            chunk_reads.append(index)
            return self.column.chunk(index)

    monkeypatch.setattr(
        cursor, "_columns", [CountingColumn(column) for column in source.columns]
    )
    batches = []
    for start in range(0, len(source), batch_size):
        rows = min(batch_size, len(source) - start)
        batches.append(cursor.peek(rows))
        cursor.advance(rows)
    assert pa.concat_tables(batches).equals(source)
    assert len(chunk_reads) <= 2 * sum(c.num_chunks for c in source.columns)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
