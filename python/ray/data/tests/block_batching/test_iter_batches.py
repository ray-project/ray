import pytest
from typing import Iterator, List, Tuple

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.iter_batches import (
    bundle_block_refs_to_logical_batches,
    prefetch_batches_locally,
    restore_from_original_order,
)


def block_generator(
    num_rows: int, num_blocks: int
) -> Iterator[Tuple[Block, BlockMetadata]]:
    for i in range(num_blocks):
        yield pa.table({"foo": [i] * num_rows}), BlockMetadata(
            num_rows=num_rows,
            size_bytes=0,
            schema=None,
            input_files=[],
            exec_stats=None,
        )


def test_bundle_block_refs_to_logical_batches():
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
        [block_refs[0][0]],
        [block_refs[1][0]],
        [block_refs[2][0]],
        [block_refs[3][0]],
    ]

    # Case 2: Multiple batches in a block (`batch_size` is 1).
    # There should be no overlap.
    num_blocks = 2
    num_rows_per_block = 2
    batch_size = 1
    block_iter = block_generator(num_rows=num_rows_per_block, num_blocks=num_blocks)
    block_refs = list(block_iter)
    logical_batch_iter = bundle_block_refs_to_logical_batches(
        iter(block_refs), batch_size=batch_size
    )
    logical_batches = list(logical_batch_iter)
    assert logical_batches == [[block_refs[0][0]], [block_refs[1][0]]]

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
        [block_refs[0][0], block_refs[1][0]],
        [block_refs[2][0], block_refs[3][0]],
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
        [block_refs[0][0], block_refs[1][0]],
        [block_refs[2][0]],
        [block_refs[3][0]],  # Leftover block.
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
        [block_refs[0][0], block_refs[1][0]],
        [block_refs[2][0]],
    ]


@pytest.mark.parametrize("num_batches_to_prefetch", [1, 2])
def test_prefetch_batches_locally(num_batches_to_prefetch):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, blocks: List[Block]):
            self.windows.append(blocks)

    num_batches = 10
    prefetcher = DummyPrefetcher()
    block_iter = iter([[i] for i in range(num_batches)])
    prefetch_block_iter = prefetch_batches_locally(
        block_iter,
        prefetcher=prefetcher,
        num_batches_to_prefetch=num_batches_to_prefetch,
    )

    batch_count = 1
    for _ in prefetch_block_iter:
        batch_count += 1
        if batch_count < num_batches:
            # Test that we are actually prefetching.
            assert len(prefetcher.windows) == batch_count

    windows = prefetcher.windows
    assert all(len(window) == num_batches_to_prefetch for window in windows)


def test_restore_from_original_order():
    base_iterator = [
        Batch(1, None),
        Batch(0, None),
        Batch(3, None),
        Batch(2, None),
    ]

    ordered = list(restore_from_original_order(iter(base_iterator)))
    idx = [batch.batch_idx for batch in ordered]
    assert idx == [0, 1, 2, 3]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
