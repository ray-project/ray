import pytest
import time
from typing import List
from unittest import mock

import pyarrow as pa

from ray.data.block import Block
from ray.data._internal.block_batching.block_batching import (
    BlockPrefetcher,
    batch_block_refs,
    batch_blocks,
    _prefetch_blocks,
)


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
        "ray.data._internal.block_batching.util.format_batches",
        sleep_batch_format,
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
