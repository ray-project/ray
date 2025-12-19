import queue
import threading
import time
from typing import Iterator, List

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BatchMetadata,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.iter_batches import (
    BatchIterator,
    prefetch_batches_locally,
    restore_original_order,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


def ref_bundle_generator(num_rows: int, num_blocks: int) -> Iterator[RefBundle]:
    for i in range(num_blocks):
        block = pa.table({"foo": [i] * num_rows})
        metadata = BlockMetadata(
            num_rows=num_rows,
            size_bytes=0,
            input_files=[],
            exec_stats=None,
        )
        schema = block.schema
        yield RefBundle(
            blocks=((ray.put(block), metadata),), owns_blocks=True, schema=schema
        )


@pytest.mark.parametrize("num_batches_to_prefetch", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
def test_prefetch_batches_locally(
    ray_start_regular_shared, num_batches_to_prefetch, batch_size
):
    class DummyPrefetcher(BlockPrefetcher):
        def __init__(self):
            self.windows = []

        def prefetch_blocks(self, block_refs: List[ObjectRef[Block]]):
            if batch_size is None:
                assert len(block_refs) == num_batches_to_prefetch
            else:
                assert (
                    sum(len(ray.get(block_ref)) for block_ref in block_refs)
                    >= batch_size * num_batches_to_prefetch
                )
            self.windows.append(block_refs)

    num_blocks = 10
    num_rows = 2
    prefetcher = DummyPrefetcher()
    ref_bundles = list(ref_bundle_generator(num_blocks=num_blocks, num_rows=num_rows))
    prefetch_block_iter = prefetch_batches_locally(
        iter(ref_bundles),
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
    expected_blocks = []
    for ref_bundle in ref_bundles:
        expected_blocks.extend(ref_bundle.block_refs)
    assert prefetched_blocks == expected_blocks


def test_restore_from_original_order():
    base_iterator = [
        Batch(BatchMetadata(batch_idx=1), None),
        Batch(BatchMetadata(batch_idx=0), None),
        Batch(BatchMetadata(batch_idx=3), None),
        Batch(BatchMetadata(batch_idx=2), None),
    ]

    ordered = list(restore_original_order(iter(base_iterator)))
    idx = [batch.metadata.batch_idx for batch in ordered]
    assert idx == [0, 1, 2, 3]


def test_finalize_fn_uses_single_thread(ray_start_regular_shared):
    """Tests that finalize_fn is not run with multiple threads."""
    ref_bundles_iter = ref_bundle_generator(num_blocks=20, num_rows=2)

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
    output_batches = BatchIterator(
        ref_bundles_iter,
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

    ref_bundles_iter = ref_bundle_generator(num_blocks=4, num_rows=2)

    output_batches = BatchIterator(
        ref_bundles_iter,
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

    ref_bundles = ref_bundle_generator(num_blocks=20, num_rows=2)
    start_time = time.time()
    output_batches = BatchIterator(
        ref_bundles,
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
