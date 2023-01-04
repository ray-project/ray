import pytest
from typing import List
from unittest import mock

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray.data.block import Block
from ray.data._internal.block_batching import BlockPrefetcher, batch_block_refs, batch_blocks, _prefetch_blocks, _blocks_to_batches, _format_batches


def block_generator(num_rows: int, num_blocks: int):
    for _ in range(num_blocks):
        yield pa.table({"foo": [1] * num_rows})


def test_batch_block_refs():
    with mock.patch("ray.data._internal.block_batching._prefetch_blocks") as mock_prefetch, mock.patch("ray.data._internal.block_batching.batch_blocks") as mock_batch_blocks:
            block_iter = block_generator(2, 2)
            batch_iter = batch_block_refs(block_iter)
            for _ in batch_iter:
                pass
            assert mock_prefetch.call_count == 1
            assert mock_batch_blocks.call_count == 1

def test_batch_blocks():
    with mock.patch("ray.data._internal.block_batching._blocks_to_batches") as mock_batch, mock.patch("ray.data._internal.block_batching._format_batches") as mock_format:
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
    prefetch_block_iter = _prefetch_blocks(block_iter, prefetcher=prefetcher, num_blocks_to_prefetch=num_blocks_to_prefetch)
   
    block_count = 1
    for _ in prefetch_block_iter:
        block_count += 1
        if block_count < num_blocks:
            # Test that we are actually prefetching.
            assert len(prefetcher.windows) == block_count

    windows = prefetcher.windows
    assert all(len(window) == num_blocks_to_prefetch for window in windows)

@pytest.mark.parametrize("block_size", [1, 10])
@pytest.mark.parametrize("drop_last", [True, False])
def test_blocks_to_batches(block_size, drop_last):
    num_blocks = 5
    block_iter = block_generator(num_rows=block_size, num_blocks=num_blocks)

    batch_size = 3
    batch_iter = _blocks_to_batches(block_iter, batch_size=batch_size, drop_last=drop_last)

    if drop_last:
        for batch in batch_iter:
            assert len(batch) == batch_size
    else:
        full_batches = 0
        leftover_batches = 0

        dataset_size = block_size * num_blocks
        for batch in batch_iter:
            if len(batch) == batch_size:
                full_batches += 1
            if len(batch) == (dataset_size % batch_size):
                leftover_batches += 1
        
        assert leftover_batches == 1
        assert full_batches == (dataset_size // batch_size)

@pytest.mark.parametrize("batch_format", ["pandas", "numpy", "pyarrow"])
def test_format_batches(batch_format):
     block_iter = block_generator(num_rows=2, num_blocks=2)
     batch_iter = _format_batches(block_iter, batch_format=batch_format)

     for batch in batch_iter:
        if batch_format == "pandas":
            assert isinstance(batch, pd.DataFrame)
        elif batch_format == "arrow":
            assert isinstance(batch, pa.Table)
        elif batch_format == "numpy":
            assert isinstance(batch, dict)
            assert isinstance(batch["foo"], np.ndarray)

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))