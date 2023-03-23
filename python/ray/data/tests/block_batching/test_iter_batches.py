from copy import copy
import pytest
from typing import Iterator, List, Tuple
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata
from ray.data._internal.block_batching.interfaces import (
    Batch,
    LogicalBatch,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.iter_batches import (
    bundle_block_refs_to_logical_batches,
    local_shuffle_logical_batches,
    prefetch_batches_locally,
    resolve_logical_batch,
    construct_batch_from_logical_batch,
    format_batches,
    collate,
    trace_deallocation_for_batch,
    restore_from_original_order,
)


def block_generator(
    num_rows: int, num_blocks: int
) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
    for i in range(num_blocks):
        yield ray.put(pa.table({"foo": [i] * num_rows})), BlockMetadata(
            num_rows=num_rows,
            size_bytes=0,
            schema=None,
            input_files=[],
            exec_stats=None,
        )


def logical_batch_generator(
    num_rows: int, num_blocks: int, batch_size: int = None
) -> Iterator[LogicalBatch]:
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        block_generator(num_rows=num_rows, num_blocks=num_blocks), batch_size=batch_size
    )
    return logical_batch_iter


def resolved_logical_batch_generator(
    num_rows: int, num_blocks: int, batch_size: int = None
):
    logical_batch_iter = logical_batch_generator(num_rows, num_blocks, batch_size)
    for logical_batch in logical_batch_iter:
        logical_batch.resolve()
        yield logical_batch


def test_bundle_block_refs_to_logical_batches(ray_start_regular_shared):
    # Case 1: `batch_size` is None.
    num_blocks = 4
    num_rows_per_block = 2
    batch_size = None
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        iter(block_refs), batch_size=batch_size
    )
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
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        iter(block_refs), batch_size=batch_size
    )
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
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        iter(block_refs), batch_size=batch_size
    )
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
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        iter(block_refs), batch_size=batch_size
    )
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0], block_refs[1][0]], 0, 1, batch_size),
        LogicalBatch(1, [block_refs[1][0], block_refs[2][0]], 1, None, batch_size),
        LogicalBatch(2, [block_refs[3][0]], 0, None, 2),  # Leftover block.
    ]

    # Case 5: Batches overlap across multiple blocks unevenly, dropping the last
    # incomplete batch.
    num_blocks = 4
    num_rows_per_block = 2
    batch_size = 3
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        iter(block_refs), batch_size=batch_size, drop_last=True
    )
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [
        LogicalBatch(0, [block_refs[0][0], block_refs[1][0]], 0, 1, batch_size),
        LogicalBatch(1, [block_refs[1][0], block_refs[2][0]], 1, None, batch_size),
    ]


def test_local_shuffle_logical_batches(ray_start_regular_shared):
    # Case 1: Shuffle buffer min size is smaller than a batch.
    # In this case, there is effectively no shuffling since the buffer
    # never contains more than 1 batch.
    shuffle_seed = 42
    num_blocks = 4
    num_rows_per_block = 2
    shuffle_buffer_min_size = 1
    logical_batches = list(logical_batch_generator(num_rows_per_block, num_blocks))
    shuffled_batches = list(
        local_shuffle_logical_batches(
            iter(logical_batches),
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
        )
    )
    assert shuffled_batches == logical_batches

    # Case 2: Shuffle buffer min size is greater than a batch.
    shuffle_seed = 42
    num_blocks = 4
    num_rows_per_block = 1
    shuffle_buffer_min_size = 2
    logical_batches = list(logical_batch_generator(num_rows_per_block, num_blocks))
    shuffled_batches = list(
        local_shuffle_logical_batches(
            iter(logical_batches),
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
        )
    )

    expected_output_ordering = [0, 1, 3, 2]
    expected_output = [copy(logical_batches[i]) for i in expected_output_ordering]
    for i in range(len(expected_output)):
        expected_output[i].batch_idx = i

    assert shuffled_batches == expected_output


@pytest.mark.parametrize("num_batches_to_prefetch", [1, 2])
def test_prefetch_batches_locally(ray_start_regular_shared, num_batches_to_prefetch):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, blocks: List[Block]):
            self.windows.append(blocks)

    num_blocks = 10
    prefetcher = DummyPrefetcher()
    logical_batches = list(logical_batch_generator(1, num_blocks))
    prefetch_batch_iter = prefetch_batches_locally(
        iter(logical_batches),
        prefetcher=prefetcher,
        num_batches_to_prefetch=num_batches_to_prefetch,
    )

    # Test that we are actually prefetching.
    # We should prefetch a new set of batches after the current set
    # finishes.
    sets_prefetched = 1
    output_batches = []
    for i, batch in enumerate(prefetch_batch_iter):
        if i % num_batches_to_prefetch == 0:
            # If all the batches are already prefetched, then skip the check.
            if not sets_prefetched * num_batches_to_prefetch >= len(logical_batches):
                assert len(prefetcher.windows) == sets_prefetched + 1
        sets_prefetched = len(prefetcher.windows)
        output_batches.append(batch)

    windows = prefetcher.windows
    assert all(len(window) == num_batches_to_prefetch for window in windows)

    # Check that the output iterator is the same as the input iterator.
    assert output_batches == logical_batches


def test_resolve_logical_batches(ray_start_regular_shared):
    logical_batches = list(logical_batch_generator(1, 1))
    resolved_iter = resolve_logical_batch(iter(logical_batches))
    assert next(resolved_iter).blocks == ray.get(logical_batches[0].block_refs)


@pytest.mark.parametrize("block_size", [1, 10])
def test_construct_batch_from_logical_batch(ray_start_regular_shared, block_size):
    num_blocks = 5
    batch_size = 3
    logical_batches = list(
        resolved_logical_batch_generator(block_size, num_blocks, batch_size=batch_size)
    )

    created_batches = list(construct_batch_from_logical_batch(iter(logical_batches)))

    for i, batch in enumerate(created_batches):
        assert i == batch.batch_idx
        assert len(batch.data) == logical_batches[i].num_rows


@pytest.mark.parametrize("batch_format", ["pandas", "numpy", "pyarrow"])
def test_format_batches(ray_start_regular_shared, batch_format):
    batches = [
        Batch(i, ray.get(data[0]), None)
        for i, data in enumerate(block_generator(num_rows=2, num_blocks=2))
    ]
    batch_iter = format_batches(batches, batch_format=batch_format)

    for i, batch in enumerate(batch_iter):
        assert batch.batch_idx == i
        if batch_format == "pandas":
            assert isinstance(batch.data, pd.DataFrame)
        elif batch_format == "arrow":
            assert isinstance(batch.data, pa.Table)
        elif batch_format == "numpy":
            assert isinstance(batch.data, dict)
            assert isinstance(batch.data["foo"], np.ndarray)


def test_collate(ray_start_regular_shared):
    def collate_fn(batch):
        return pa.table({"bar": [1] * 2})

    batches = [
        Batch(i, ray.get(data[0]), None)
        for i, data in enumerate(block_generator(num_rows=2, num_blocks=2))
    ]
    batch_iter = collate(batches, collate_fn=collate_fn)

    for i, batch in enumerate(batch_iter):
        assert batch.batch_idx == i
        assert batch.data == pa.table({"bar": [1] * 2})


@patch.object(ray.data._internal.block_batching.iter_batches, "trace_deallocation")
@pytest.mark.parametrize("eager_free", [True, False])
def test_trace_deallocation(mock, eager_free):
    batches = [Batch(0, 0, LogicalBatch(0, [0], 0, None, 1))]
    batch_iter = trace_deallocation_for_batch(iter(batches), eager_free=eager_free)
    # Test that the underlying batch is not modified.
    assert next(batch_iter) == batches[0]
    mock.assert_called_once_with(0, loc="iter_batches", free=eager_free)


def test_restore_from_original_order():
    base_iterator = [
        Batch(1, None, None),
        Batch(0, None, None),
        Batch(3, None, None),
        Batch(2, None, None),
    ]

    ordered = list(restore_from_original_order(iter(base_iterator)))
    idx = [batch.batch_idx for batch in ordered]
    assert idx == [0, 1, 2, 3]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
