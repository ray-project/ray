"""Forward-only Arrow row cursors that preserve physical column chunking.

Peeking may retain source views, but never commits consumption or copies source
payload. Advancement happens only after the caller has built its output batch.

For C source chunks across D columns and Q output batches, normal forward
consumption routes chunk metadata in O(C + D * Q) work. Aligned layouts use a
native reader; other layouts visit each column's boundaries independently,
without expanding every column at the union of all boundaries. Source buffers
remain shared; contiguous output conversion, when needed, belongs to the batch.
"""

from collections import deque
from typing import Union

import pyarrow as pa

ArrowBlockCursor = Union["_AlignedArrowBlockCursor", "_ColumnArrowBlockCursor"]


def create_arrow_block_cursor(table: pa.Table) -> ArrowBlockCursor:
    """Choose a cursor that preserves each column's physical chunk boundaries."""
    assert table.num_rows > 0 and table.num_columns > 0
    if _chunks_are_aligned(table.columns):
        return _AlignedArrowBlockCursor(table)
    return _ColumnArrowBlockCursor(table)


def _chunks_are_aligned(columns) -> bool:
    return len(columns) == 1 or (
        len({column.num_chunks for column in columns}) == 1
        and all(
            len({len(chunk) for chunk in chunks}) == 1
            for chunks in zip(*(column.iterchunks() for column in columns))
        )
    )


class _AlignedArrowBlockCursor:
    """Use Arrow's native reader when every column has the same chunk boundaries."""

    def __init__(self, table: pa.Table):
        self._remaining_rows = table.num_rows
        self._reader = table.to_reader()
        self._pieces: deque[pa.RecordBatch] = deque()
        self._available_rows = 0
        self._head_offset = 0
        self._head_table = None

    def peek(self, num_rows: int) -> pa.Table:
        assert 0 < num_rows <= self._remaining_rows
        while self._available_rows < num_rows:
            batch = self._reader.read_next_batch()
            if batch.num_rows:
                self._pieces.append(batch)
                self._available_rows += batch.num_rows

        head = self._pieces[0]
        if num_rows <= head.num_rows - self._head_offset:
            if self._head_table is None:
                self._head_table = pa.Table.from_batches([head])
            if self._head_offset == 0 and num_rows == head.num_rows:
                return self._head_table
            return self._head_table.slice(self._head_offset, num_rows)

        pieces = []
        needed = num_rows
        offset = self._head_offset
        for batch in self._pieces:
            take = min(needed, batch.num_rows - offset)
            pieces.append(
                batch
                if offset == 0 and take == batch.num_rows
                else batch.slice(offset, take)
            )
            needed -= take
            if needed == 0:
                break
            offset = 0
        return pa.Table.from_batches(pieces)

    def advance(self, num_rows: int) -> None:
        assert 0 < num_rows <= self._available_rows
        self._remaining_rows -= num_rows
        self._available_rows -= num_rows
        while num_rows:
            available = self._pieces[0].num_rows - self._head_offset
            if num_rows >= available:
                self._pieces.popleft()
                self._head_offset = 0
                self._head_table = None
                num_rows -= available
            else:
                self._head_offset += num_rows
                break


class _ColumnArrowBlockCursor:
    """Slice columns independently instead of expanding the union of boundaries."""

    def __init__(self, table: pa.Table):
        columns = table.columns
        self._chunked_indices = [
            index for index, column in enumerate(columns) if column.num_chunks > 1
        ]
        self._single_indices = [
            index for index, column in enumerate(columns) if column.num_chunks <= 1
        ]
        self._columns = [columns[index] for index in self._chunked_indices]
        self._single_table = (
            table.select(self._single_indices) if self._single_indices else None
        )
        self._schema = table.schema
        self._num_rows = table.num_rows
        self._offset = 0
        self._positions = [(0, 0, column.chunk(0)) for column in self._columns]
        self._cache = None

    def _make_table(self, columns, start: int, num_rows: int) -> pa.Table:
        if self._single_table is None:
            return pa.Table.from_arrays(columns, schema=self._schema)

        # Slice continuous columns together in native code. Routing them through
        # the fragmented column loop would add Python work to wide tables.
        singles = self._single_table.slice(start, num_rows)
        if len(columns) == 1:
            index = self._chunked_indices[0]
            return singles.add_column(index, self._schema.field(index), columns[0])
        result_columns = [None] * len(self._schema)
        for index, column in zip(self._single_indices, singles.columns):
            result_columns[index] = column
        for index, column in zip(self._chunked_indices, columns):
            result_columns[index] = column
        return pa.Table.from_arrays(result_columns, schema=self._schema)

    def peek(self, num_rows: int) -> pa.Table:
        assert 0 < num_rows <= self._num_rows - self._offset
        start = self._offset
        stop = start + num_rows
        if self._cache is not None:
            cache_start, cache = self._cache
            if stop <= cache_start + cache.num_rows:
                return cache.slice(start - cache_start, num_rows)

        positions = []
        common_end = self._num_rows
        for column, (index, chunk_start, chunk) in zip(self._columns, self._positions):
            while chunk_start + len(chunk) <= start:
                chunk_start += len(chunk)
                index += 1
                chunk = column.chunk(index)
            positions.append((index, chunk_start, chunk))
            end = chunk_start + len(chunk)
            common_end = min(common_end, end)

        if stop <= common_end:
            # Reuse a single-chunk table while small batches stay within all
            # current chunks. Subsequent peeks use native Table.slice.
            table = self._make_table(
                [
                    chunk.slice(start - chunk_start, common_end - start)
                    for _, chunk_start, chunk in positions
                ],
                start,
                common_end - start,
            )
            result = table if table.num_rows == num_rows else table.slice(0, num_rows)
            self._cache = (start, table)
        else:
            result_columns = []
            for column, (index, chunk_start, chunk) in zip(self._columns, positions):
                offset = start - chunk_start
                needed = num_rows
                pieces = []
                while needed:
                    take = min(needed, len(chunk) - offset)
                    if take:
                        pieces.append(
                            chunk
                            if offset == 0 and take == len(chunk)
                            else chunk.slice(offset, take)
                        )
                        needed -= take
                    if needed:
                        index += 1
                        chunk = column.chunk(index)
                        offset = 0
                result_columns.append(
                    pieces[0]
                    if len(pieces) == 1
                    else pa.chunked_array(pieces, type=column.type)
                )
            result = self._make_table(result_columns, start, num_rows)
            self._cache = None

        # Keep positions at the start of this peek, not its end. A failed output
        # build can repeat the peek; only advance() commits logical consumption.
        self._positions = positions
        return result

    def advance(self, num_rows: int) -> None:
        assert 0 < num_rows <= self._num_rows - self._offset
        self._offset += num_rows
