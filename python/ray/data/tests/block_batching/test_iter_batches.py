import itertools
import queue
import threading
import time
from typing import Iterator, List, Tuple

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.block_batching.interfaces import Batch, BlockPrefetcher
from ray.data._internal.block_batching.iter_batches import (
    iter_batches,
    prefetch_batches_locally,
    restore_original_order,
)
from ray.data.block import Block, BlockMetadata


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


def test_finalize_fn_uses_single_thread(ray_start_regular_shared):
    """Tests that finalize_fn is not run with multiple threads."""
    block_refs_iter = itertools.starmap(
        lambda block, metadata: (ray.put(block), metadata),
        block_generator(num_blocks=20, num_rows=2),
    )

    q = queue.Queue()
    semaphore = threading.Semaphore(value=1)

    def finalize_enforce_single_thread(batch):
        already_acquired = not semaphore.acquire(blocking=False)
        if already_acquired:
            e = AssertionError("finalize_fn is being run concurrently.")
            q.put(e, block=True)
        semaphore.release()
        return batch

    # Test that finalize_fn is called in a single thread,
    # even if prefetch_batches is set.
    output_batches = iter_batches(
        block_refs_iter,
        dataset_tag="dataset",
        collate_fn=lambda batch: batch,
        finalize_fn=finalize_enforce_single_thread,
        prefetch_batches=4,
    )

    # Force execution of the iterator.
    # This step should not raise an exception.
    list(output_batches)

    try:
        e = q.get(block=False, timeout=0.1)
        raise e
    except queue.Empty:
        pass


# Test for 3 cases
# 1. Batch size is less than block size
# 2. Batch size is more than block size
# 3. Block size is not divisble by batch size
@pytest.mark.parametrize("batch_size", [1, 4, 3])
@pytest.mark.parametrize("drop_last", [True, False])
@pytest.mark.parametrize("prefetch_batches", [0, 1])
def test_iter_batches_e2e(
    ray_start_regular_shared, batch_size, drop_last, prefetch_batches
):
    def collate_fn(batch: pd.DataFrame):
        return batch + 1

    block_refs_iter = itertools.starmap(
        lambda block, metadata: (ray.put(block), metadata),
        block_generator(num_blocks=4, num_rows=2),
    )

    output_batches = iter_batches(
        block_refs_iter,
        dataset_tag="dataset",
        batch_size=batch_size,
        prefetch_batches=prefetch_batches,
        batch_format="pandas",
        collate_fn=collate_fn,
        drop_last=drop_last,
    )

    output_batches = list(output_batches)

    assert len(output_batches) > 0
    for df in output_batches:
        # Check batch formatting.
        assert isinstance(df, pd.DataFrame)
        # Check batch size.
        if batch_size == 3 and not drop_last:
            assert len(df) in {2, 3}
        else:
            assert len(df) == batch_size

    concat_df = pd.concat(output_batches)
    # Test that collate_fn is applied.
    assert concat_df["foo"].iloc[0] == 1
    # Make sure order is preserved.
    for i in range(len(concat_df) - 1):
        assert concat_df["foo"].iloc[i + 1] >= concat_df["foo"].iloc[i]


def test_iter_batches_e2e_async(ray_start_regular_shared):
    """We add time.sleep in 3 places:
    1. In the base generator to simulate streaming executor blocking on next results.
    2. In the collate_fn to simulate expensive slicing/formatting/collation
    3. In the user thread to simulate training.
    """

    def collate_fn(batch):
        time.sleep(2)
        return batch

    block_refs_iter = itertools.starmap(
        lambda block, metadata: (ray.put(block), metadata),
        block_generator(num_blocks=20, num_rows=2),
    )
    start_time = time.time()
    output_batches = iter_batches(
        block_refs_iter,
        dataset_tag="dataset",
        batch_size=None,
        collate_fn=collate_fn,
        prefetch_batches=4,
    )
    batches = []
    for batch in output_batches:
        time.sleep(1.5)
        batches.append(batch)
    end_time = time.time()

    # 20 batches, 1.5 second sleep. Should be less than 45 seconds, even with some
    # overhead.
    # If there was no overlap, then we would expect this to take at least 20*2.5 = 50
    assert end_time - start_time < 45, end_time - start_time

    assert len(batches) == 20
    assert all(len(batch) == 2 for batch in batches)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
