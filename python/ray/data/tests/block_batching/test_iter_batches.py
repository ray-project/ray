import pytest
from typing import Iterator, List, Tuple

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.iter_batches import (
    prefetch_batches_locally,
    restore_original_order,
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


@pytest.mark.parametrize("num_batches_to_prefetch", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
def test_prefetch_batches_locally(num_batches_to_prefetch, batch_size):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, blocks: List[Block]):
            if batch_size is None:
                assert len(blocks) == num_batches_to_prefetch
            else:
                assert (
                    sum(len(block) for block in blocks)
                    >= batch_size * num_batches_to_prefetch
                )
            self.windows.append(blocks)

    num_blocks = 10
    num_rows = 2
    prefetcher = DummyPrefetcher()
    blocks = list(block_generator(num_blocks=num_blocks, num_rows=num_rows))
    prefetch_block_iter = prefetch_batches_locally(
        iter(blocks),
        prefetcher=prefetcher,
        num_batches_to_prefetch=num_batches_to_prefetch,
        batch_size=batch_size,
    )

    block_count = 0
    prefetched_blocks = []
    previous_num_windows = 1

    for block in prefetch_block_iter:
        prefetched_blocks.append(block)
        block_count += 1
        remaining_rows = (num_blocks - block_count) * num_rows
        if batch_size is None and block_count < num_blocks - num_batches_to_prefetch:
            # Test that we are actually prefetching in advance if this is not the last
            # block.
            assert len(prefetcher.windows) == previous_num_windows + 1
            previous_num_windows = len(prefetcher.windows)
        elif (
            batch_size is not None
            and remaining_rows > batch_size * num_batches_to_prefetch
        ):
            # Test that we are actually prefetching in advance if this is not the last
            # batch.
            assert len(prefetcher.windows) == previous_num_windows + 1
            previous_num_windows = len(prefetcher.windows)

    # Test that original blocks are unchanged.
    assert prefetched_blocks == [block for block, metadata, in blocks]


def test_restore_from_original_order():
    base_iterator = [
        Batch(1, None),
        Batch(0, None),
        Batch(3, None),
        Batch(2, None),
    ]

    ordered = list(restore_original_order(iter(base_iterator)))
    idx = [batch.batch_idx for batch in ordered]
    assert idx == [0, 1, 2, 3]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
