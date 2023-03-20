import pytest
import time
from typing import Iterator, List, Tuple
from unittest import mock

import numpy as np
import pandas as pd
import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data._internal.block_batching import (
    BlockPrefetcher,
    LogicalBatch,
    ResolvedLogicalBatch,
    batch_block_refs,
    batch_blocks,
    _bundle_block_refs_to_logical_batches,
    _local_shuffle_logical_batches,
    _prefetch_batches_locally,
    _blocks_to_batches,
    _format_batches,
    _collate,
    _make_async_gen,
)

def block_generator(num_rows: int, num_blocks: int) -> Iterator[Tuple[Block, BlockMetadata]]:
    for i in range(num_blocks):
        yield pa.table({"foo": [i] * num_rows}), BlockMetadata(
            num_rows=num_rows,
            size_bytes=0,
            schema=None,
            input_files=[],
            exec_stats=None,
            )

def logical_batch_generator(num_rows: int, num_blocks: int, batch_size: int = None) -> Iterator[LogicalBatch]:
    yield from _bundle_block_refs_to_logical_batches(block_generator(num_rows=num_rows, num_blocks=num_blocks), batch_size=batch_size)

def test_batch_block_refs():
    with mock.patch(
        "ray.data._internal.block_batching._prefetch_blocks"
    ) as mock_prefetch, mock.patch(
        "ray.data._internal.block_batching.batch_blocks"
    ) as mock_batch_blocks:
        block_iter = block_generator(2, 2)
        batch_iter = batch_block_refs(block_iter)
        for _ in batch_iter:
            pass
        assert mock_prefetch.call_count == 1
        assert mock_batch_blocks.call_count == 1


def test_batch_blocks():
    with mock.patch(
        "ray.data._internal.block_batching._blocks_to_batches"
    ) as mock_batch, mock.patch(
        "ray.data._internal.block_batching._format_batches"
    ) as mock_format:
        block_iter = block_generator(2, 2)
        batch_iter = batch_blocks(block_iter)
        for _ in batch_iter:
            pass
        assert mock_batch.call_count == 1
        assert mock_format.call_count == 1


def test_bundle_block_refs_to_logical_batches():
    # Case 1: `batch_size` is None.
    num_blocks = 4
    num_rows_per_block = 2
    batch_size = None
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = _bundle_block_refs_to_logical_batches(iter(block_refs), batch_size=batch_size)
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0]], 0, None, num_rows_per_block),
        LogicalBatch(1, [block_refs[1][0]], 0, None, num_rows_per_block),
        LogicalBatch(2, [block_refs[2][0]], 0, None, num_rows_per_block),
        LogicalBatch(3, [block_refs[3][0]], 0, None, num_rows_per_block),
    ]

    # Case 2: Multiple batches in a block (`batch_size` is 1).
    num_blocks = 2
    num_rows_per_block = 2
    batch_size = 1
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = _bundle_block_refs_to_logical_batches(iter(block_refs), batch_size=batch_size)
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0]], 0, 1, batch_size),
        LogicalBatch(1, [block_refs[0][0]], 1, None, batch_size),
        LogicalBatch(2, [block_refs[1][0]], 0, 1, batch_size),
        LogicalBatch(3, [block_refs[1][0]], 1, None, batch_size),
    ]

    # Case 3: Multiple blocks in a batch (`batch_size` is 2)
    num_blocks = 4
    num_rows_per_block = 1
    batch_size = 2
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = _bundle_block_refs_to_logical_batches(iter(block_refs), batch_size=batch_size)
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0], block_refs[1][0]], 0, None, batch_size),
        LogicalBatch(1, [block_refs[2][0], block_refs[3][0]], 0, None, batch_size),
    ]

    # Case 4: Batches overlap across multiple blocks unevenly
    num_blocks = 4
    num_rows_per_block = 2
    batch_size = 3
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = _bundle_block_refs_to_logical_batches(iter(block_refs), batch_size=batch_size)
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0], block_refs[1][0]], 0, 1, batch_size),
        LogicalBatch(1, [block_refs[1][0], block_refs[2][0]], 1, None, batch_size),
        LogicalBatch(2, [block_refs[3][0]], 0, None, 2) # Leftover block.
    ]

    # Case 5: Batches overlap across multiple blocks unevenly, dropping the last
    # incomplete batch.
    num_blocks = 4
    num_rows_per_block = 2
    batch_size = 3
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = _bundle_block_refs_to_logical_batches(iter(block_refs), batch_size=batch_size, drop_last=True)
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0], block_refs[1][0]], 0, 1, batch_size),
        LogicalBatch(1, [block_refs[1][0], block_refs[2][0]], 1, None, batch_size),
    ]

def test_local_shuffle_logical_batches():
    # Case 1: Shuffle buffer min size is smaller than a batch.
    # In this case, there is effectively no shuffling since the buffer 
    # never contains more than 1 batch.
    shuffle_seed = 42
    num_blocks = 4
    num_rows_per_block = 2
    shuffle_buffer_min_size = 1
    logical_batches = list(logical_batch_generator(num_rows_per_block, num_blocks))
    shuffled_batches = list(_local_shuffle_logical_batches(iter(logical_batches), shuffle_buffer_min_size=shuffle_buffer_min_size, shuffle_seed=shuffle_seed))
    assert shuffled_batches == logical_batches

    # Case 2: Shuffle buffer min size is greater than a batch.
    shuffle_seed = 42
    num_blocks = 4
    num_rows_per_block = 1
    shuffle_buffer_min_size = 2
    logical_batches = list(logical_batch_generator(num_rows_per_block, num_blocks))
    shuffled_batches = list(_local_shuffle_logical_batches(iter(logical_batches), shuffle_buffer_min_size=shuffle_buffer_min_size, shuffle_seed=shuffle_seed))
    assert shuffled_batches == [
        LogicalBatch.from_logical_batch(logical_batches[0], 0),
        LogicalBatch.from_logical_batch(logical_batches[1], 1),
        LogicalBatch.from_logical_batch(logical_batches[3], 2),
        LogicalBatch.from_logical_batch(logical_batches[2], 3)
    ]

@pytest.mark.parametrize("num_batches_to_prefetch", [1, 2])
def test_prefetch_batches_locally(num_batches_to_prefetch):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, blocks: List[Block]):
            self.windows.append(blocks)

    num_blocks = 10
    prefetcher = DummyPrefetcher()
    logical_batches = list(logical_batch_generator(1, num_blocks))
    prefetch_batch_iter = _prefetch_batches_locally(
        iter(logical_batches), prefetcher=prefetcher, num_batches_to_prefetch=num_batches_to_prefetch
    )

    # Test that we are actually prefetching.
    # We should prefetch a new set of batches after the current set
    # finishes.
    sets_prefetched = 1
    output_batches = []
    for i, batch in enumerate(prefetch_batch_iter):
        if i % num_batches_to_prefetch == 0:
            # If all the batches are already prefetched, then skip the check.
            if not sets_prefetched*num_batches_to_prefetch >= len(logical_batches):
                assert len(prefetcher.windows) == sets_prefetched + 1
        sets_prefetched = len(prefetcher.windows)
        output_batches.append(batch)

    windows = prefetcher.windows
    assert all(len(window) == num_batches_to_prefetch for window in windows)

    # Check that the output iterator is the same as the input iterator.
    assert output_batches == logical_batches

@pytest.mark.parametrize("block_size", [10])
def test_blocks_to_batches(block_size):
    num_blocks = 5
    batch_size = 3
    logical_batches = list(logical_batch_generator(block_size, num_blocks, batch_size=batch_size))

    def create_resolved_batch(logical_batch):
        return ResolvedLogicalBatch(
            logical_batch.batch_idx,
            blocks=logical_batch.block_refs,
            starting_block_idx=logical_batch.starting_block_idx,
            ending_block_idx=logical_batch.ending_block_idx,
            num_rows=logical_batch.num_rows
        )

    resolved_logical_batches = [create_resolved_batch(batch) for batch in logical_batches]
    created_batches = list(_blocks_to_batches(
        iter(resolved_logical_batches)
    ))


    for i, output in enumerate(created_batches):
        batch_idx, batch = output
        assert i == batch_idx
        assert len(batch) == resolved_logical_batches[i].num_rows

@pytest.mark.parametrize("batch_format", ["pandas", "numpy", "pyarrow"])
def test_format_batches(batch_format):
    blocks = [output[0] for output in block_generator(num_rows=2, num_blocks=2)]
    batch_iter = _format_batches(enumerate(blocks), batch_format=batch_format)

    for i, output in enumerate(batch_iter):
        batch_idx, batch = output
        assert batch_idx == i
        if batch_format == "pandas":
            assert isinstance(batch, pd.DataFrame)
        elif batch_format == "arrow":
            assert isinstance(batch, pa.Table)
        elif batch_format == "numpy":
            assert isinstance(batch, dict)
            assert isinstance(batch["foo"], np.ndarray)

def test_collate():
    import pyarrow.compute as pc
    def collate_fn(batch):
        return pa.table({"bar": [1] * 2})
    
    blocks = [output[0] for output in block_generator(num_rows=2, num_blocks=2)]
    batch_iter = _collate(enumerate(blocks), collate_fn=collate_fn)

    for i, output in enumerate(batch_iter):
        batch_idx, batch = output
        assert batch_idx == i
        assert batch == pa.table({"bar": [1] * 2})


def test_make_async_gen():
    """Tests that make_async_gen overlaps compute."""

    num_items = 10

    def gen():
        for i in range(num_items):
            time.sleep(2)
            yield i

    def sleep_udf(item):
        time.sleep(3)
        return item

    iterator = _make_async_gen(gen())

    start_time = time.time()
    outputs = []
    for item in iterator:
        outputs.append(sleep_udf(item))
    end_time = time.time()

    assert outputs == list(range(num_items))

    assert end_time - start_time < num_items * 3 + 3


def test_make_async_gen_buffer_size():
    """Tests that multiple items can be prefetched at a time
    with larger buffer size."""

    num_items = 5

    def gen():
        for i in range(num_items):
            time.sleep(1)
            yield i

    def sleep_udf(item):
        time.sleep(5)
        return item

    iterator = _make_async_gen(gen(), prefetch_buffer_size=4)

    start_time = time.time()

    # Only sleep for first item.
    sleep_udf(next(iterator))

    # All subsequent items should already be prefetched and should be ready.
    for _ in iterator:
        pass
    end_time = time.time()

    # 1 second for first item, 5 seconds for udf, 0.5 seconds buffer
    assert end_time - start_time < 6.5


# Test for 3 cases
# 1. Batch size is less than block size
# 2. Batch size is more than block size
# 3. Block size is not divisble by batch size
@pytest.mark.parametrize("batch_size", [4, 10, 7])
def test_async_batch_fetching(batch_size):
    blocks = block_generator(num_blocks=5, num_rows=8)

    def sleep_batch_format(batch_iter, *args, **kwargs):
        for batch in batch_iter:
            time.sleep(2)
            yield batch

    with mock.patch(
        "ray.data._internal.block_batching._format_batches", sleep_batch_format
    ):
        batch_iter = batch_blocks(
            batch_size=batch_size, blocks=blocks, prefetch_batches=1
        )
        outputs = []
        start_time = time.time()
        for batch in batch_iter:
            time.sleep(3)
            outputs.append(batch)
        end_time = time.time()

    total_time = end_time - start_time
    # Total time should be based on number of times the udf is called
    # (which is equal to len(outputs)).
    # The 2 seconds sleep in sleep_batch_format is overlapped, so does not count
    # towards total time.
    assert total_time < len(outputs) * 3 + 3

    # There should be no dropped rows.
    assert sum(len(output_batch) for output_batch in outputs) == 40, sum(
        len(output_batch) for output_batch in outputs
    )  # 5 blocks with 8 rows each.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
