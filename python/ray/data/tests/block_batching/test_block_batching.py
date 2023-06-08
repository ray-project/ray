from typing import List
from unittest import mock

import pyarrow as pa
import pytest

from ray.data._internal.block_batching.block_batching import (
    BlockPrefetcher,
    _prefetch_blocks,
    batch_block_refs,
    batch_blocks,
)
from ray.data.block import Block


def block_generator(num_rows: int, num_blocks: int):
    for _ in range(num_blocks):
        yield pa.table({"foo": [1] * num_rows})


def test_batch_block_refs():
    with mock.patch(
        "ray.data._internal.block_batching.block_batching._prefetch_blocks"
    ) as mock_prefetch, mock.patch(
        "ray.data._internal.block_batching.block_batching.batch_blocks"
    ) as mock_batch_blocks:
        block_iter = block_generator(2, 2)
        batch_iter = batch_block_refs(block_iter)
        for _ in batch_iter:
            pass
        assert mock_prefetch.call_count == 1
        assert mock_batch_blocks.call_count == 1


def test_batch_blocks():
    with mock.patch(
        "ray.data._internal.block_batching.block_batching.blocks_to_batches"
    ) as mock_batch, mock.patch(
        "ray.data._internal.block_batching.block_batching.format_batches"
    ) as mock_format:
        block_iter = block_generator(2, 2)
        batch_iter = batch_blocks(block_iter)
        for _ in batch_iter:
            pass
        assert mock_batch.call_count == 1
        assert mock_format.call_count == 1


@pytest.mark.parametrize("num_blocks_to_prefetch", [1, 2])
def test_prefetch_blocks(num_blocks_to_prefetch):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, blocks: List[Block]):
            self.windows.append(blocks)

    num_blocks = 10
    prefetcher = DummyPrefetcher()
    block_iter = block_generator(num_rows=1, num_blocks=num_blocks)
    prefetch_block_iter = _prefetch_blocks(
        block_iter, prefetcher=prefetcher, num_blocks_to_prefetch=num_blocks_to_prefetch
    )

    block_count = 1
    for _ in prefetch_block_iter:
        block_count += 1
        if block_count < num_blocks:
            # Test that we are actually prefetching.
            assert len(prefetcher.windows) == block_count

    windows = prefetcher.windows
    assert all(len(window) == num_blocks_to_prefetch for window in windows)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
