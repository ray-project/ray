import time

import pyarrow as pa
import pytest

import ray
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops.transform_pyarrow import try_combine_chunked_columns
from ray.data._internal.batcher import (
    SHUFFLE_BUFFER_COMPACTION_THRESHOLD,
    Batcher,
    ShufflingBatcher,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockAccessor


def gen_block(num_rows):
    return pa.table({"foo": [1] * num_rows})


def test_shuffling_batcher():
    batch_size = 5
    buffer_size = 20

    with pytest.raises(
        ValueError, match="Must specify a batch_size if using a local shuffle."
    ):
        ShufflingBatcher(batch_size=None, shuffle_buffer_min_size=buffer_size)

    # Should not raise error.
    ShufflingBatcher(batch_size=batch_size, shuffle_buffer_min_size=batch_size - 1)

    batcher = ShufflingBatcher(
        batch_size=batch_size,
        shuffle_buffer_min_size=buffer_size,
    )

    total_added = 0
    total_yielded = 0

    def add_and_check(num_rows, expect_has_batch):
        """Add a block and verify has_batch() matches expectation."""
        nonlocal total_added
        batcher.add(gen_block(num_rows))
        total_added += num_rows
        assert batcher.has_batch() == expect_has_batch, (
            f"after adding {num_rows}: has_batch()={batcher.has_batch()}, "
            f"expected {expect_has_batch} "
            f"(compacted={batcher._num_compacted_rows()}, "
            f"uncompacted={batcher._num_uncompacted_rows()}, "
            f"total={batcher._num_rows()})"
        )

    def next_and_check(
        expect_full_batch=True,
        expect_has_batch_after=True,
    ):
        """Consume one batch and verify size and post-state."""
        nonlocal total_yielded
        batch = batcher.next_batch()
        total_yielded += len(batch)
        if expect_full_batch:
            assert (
                len(batch) == batch_size
            ), f"expected full batch of {batch_size}, got {len(batch)}"
        else:
            assert len(batch) <= batch_size
        assert batcher.has_batch() == expect_has_batch_after, (
            f"after next_batch: has_batch()={batcher.has_batch()}, "
            f"expected {expect_has_batch_after} "
            f"(compacted={batcher._num_compacted_rows()}, "
            f"uncompacted={batcher._num_uncompacted_rows()}, "
            f"total={batcher._num_rows()})"
        )

    # Before any data is added, there should be no batches.
    assert not batcher.has_batch()
    assert not batcher.has_any()

    # Add blocks incrementally. All rows go into the pending buffer,
    # and has_batch will return False until enough rows accumulate.
    add_and_check(3, expect_has_batch=False)  # total=3
    add_and_check(7, expect_has_batch=False)  # total=10
    add_and_check(10, expect_has_batch=False)  # total=20

    # After adding 15 more (total=35), total - batch_size = 30 >= min_rows_to_trigger.
    add_and_check(15, expect_has_batch=True)  # total=35

    # All 35 rows are still uncompacted since no next_batch() has been called.
    assert batcher._shuffle_buffer is None
    assert batcher._builder.num_rows() == 35

    # Consume one batch — this triggers the first compaction.
    next_and_check(expect_full_batch=True, expect_has_batch_after=True)
    assert batcher._shuffle_buffer is not None  # compaction happened
    assert batcher._builder.num_rows() == 0  # all rows moved to compacted buffer

    # Add more data while consuming.
    add_and_check(20, expect_has_batch=True)  # total grows

    # Consume batches. Each must be full since we're still streaming.
    while batcher.has_batch():
        batch = batcher.next_batch()
        assert len(batch) == batch_size
        total_yielded += batch_size

    # Streaming exhausted: remaining rows <= batch_size (not enough to trigger
    # has_batch without more data or done_adding).
    assert batcher._num_rows() <= batch_size

    # Add a partial amount and signal done.
    batcher.add(gen_block(8))
    total_added += 8
    batcher.done_adding()

    # Drain remaining full batches via next_and_check.
    while batcher.has_batch():
        remaining_after = batcher._num_rows() - batch_size
        next_and_check(
            expect_full_batch=True,
            expect_has_batch_after=remaining_after >= batch_size,
        )

    # Final partial batch.
    if batcher.has_any():
        next_and_check(expect_full_batch=False, expect_has_batch_after=False)

    # All rows must be accounted for.
    assert total_yielded == total_added
    assert not batcher.has_any()


def test_batching_pyarrow_table_with_many_chunks():
    """Make sure batching a pyarrow table with many chunks is fast.

    See https://github.com/ray-project/ray/issues/31108 for more details.
    """
    num_chunks = 5000
    batch_size = 1024

    batches = []
    for _ in range(num_chunks):
        batch = {}
        for i in range(10):
            batch[str(i)] = list(range(batch_size))
        batches.append(pa.Table.from_pydict(batch))

    block = pa.concat_tables(batches, promote=True)

    start = time.perf_counter()
    batcher = Batcher(batch_size, ensure_copy=False)
    batcher.add(block)
    batcher.done_adding()
    while batcher.has_any():
        batcher.next_batch()
    duration = time.perf_counter() - start
    assert duration < 10

    start = time.perf_counter()
    shuffling_batcher = ShufflingBatcher(
        batch_size=batch_size, shuffle_buffer_min_size=batch_size
    )
    shuffling_batcher.add(block)
    shuffling_batcher.done_adding()
    while shuffling_batcher.has_any():
        shuffling_batcher.next_batch()
    duration = time.perf_counter() - start
    assert duration < 30


@pytest.mark.parametrize(
    "batch_size,local_shuffle_buffer_size",
    [(1, 1), (10, 1), (1, 10), (10, 1000), (1000, 10)],
)
def test_shuffling_batcher_grid(batch_size, local_shuffle_buffer_size):
    ds = ray.data.range_tensor(10000, shape=(130,))
    start = time.time()
    count = 0
    for batch in ds.iter_batches(
        batch_size=batch_size, local_shuffle_buffer_size=local_shuffle_buffer_size
    ):
        count += len(batch["data"])
    print((ds.size_bytes() / 1e9) / (time.time() - start), "GB/s")
    assert count == 10000


@pytest.mark.parametrize(
    "batch_size,local_shuffle_buffer_size",
    [(1, 1), (10, 1), (1, 10), (10, 100), (100, 10)],
)
def test_local_shuffle_determinism(batch_size, local_shuffle_buffer_size):
    # Preserve order so that the blocks are in the same order prior to shuffling.
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True
    TEST_ITERATIONS = 10

    ds = ray.data.range(1000)
    batch_map = {}
    for i in range(TEST_ITERATIONS):
        for batch in ds.iter_batches(
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=0,
        ):
            if i == 0:
                batch_map[batch["id"][0]] = batch
            else:
                # Check that batch is the same as the first dataset's batch
                assert all(batch_map[batch["id"][0]]["id"] == batch["id"])


def test_local_shuffle_buffer_warns_if_too_large(shutdown_only):
    ray.shutdown()
    ray.init(object_store_memory=128 * 1024 * 1024)

    # Each row is 16 MiB * 8 = 128 MiB
    ds = ray.data.range_tensor(2, shape=(16, 1024, 1024))

    # Test that Ray Data emits a warning if the local shuffle buffer size would cause
    # spilling.
    with pytest.warns(UserWarning, match="shuffle buffer"):
        # Each row is 128 MiB and the shuffle buffer size is 2 rows, so expect at least
        # 256 MiB of memory usage > 128 MiB total on node.
        batches = ds.iter_batches(batch_size=1, local_shuffle_buffer_size=2)
        next(iter(batches))


def _collect_rows_full_method(blocks, batch_size, buffer_size, seed):
    """Reference implementation using the old full-shuffle method.

    Materializes a fully shuffled copy of the buffer on each compaction,
    then yields contiguous slices. Used to validate the incremental index method.
    """
    shuffle_buffer_min_size = max(buffer_size, batch_size)

    min_rows_to_yield_batch = max(
        1, int(shuffle_buffer_min_size * SHUFFLE_BUFFER_COMPACTION_THRESHOLD)
    )

    builder = DelegatingBlockBuilder()
    shuffle_buffer = None
    batch_head = 0
    shuffle_seed = seed

    for block in blocks:
        if BlockAccessor.for_block(block).num_rows() > 0:
            builder.add_block(block)

    done_adding = True
    rows = []

    while True:
        compacted = 0
        if shuffle_buffer is not None:
            compacted = max(
                0, BlockAccessor.for_block(shuffle_buffer).num_rows() - batch_head
            )
        uncompacted = builder.num_rows()
        num_rows = compacted + uncompacted

        has_batch = num_rows >= batch_size
        has_any = num_rows > 0

        if not (has_batch or (done_adding and has_any)):
            break

        # Compaction: merge uncompacted rows into shuffle buffer.
        if uncompacted > 0 and (done_adding or compacted <= min_rows_to_yield_batch):
            if shuffle_buffer is not None:
                if batch_head > 0:
                    block_acc = BlockAccessor.for_block(shuffle_buffer)
                    shuffle_buffer = block_acc.slice(batch_head, block_acc.num_rows())
                builder.add_block(shuffle_buffer)
            shuffle_buffer = builder.build()
            shuffle_buffer = BlockAccessor.for_block(shuffle_buffer).random_shuffle(
                shuffle_seed
            )
            if shuffle_seed is not None:
                shuffle_seed += 1
            if isinstance(BlockAccessor.for_block(shuffle_buffer), ArrowBlockAccessor):
                shuffle_buffer = try_combine_chunked_columns(shuffle_buffer)
            builder = DelegatingBlockBuilder()
            batch_head = 0

        buf_size = BlockAccessor.for_block(shuffle_buffer).num_rows()
        bs = min(batch_size, buf_size - batch_head)
        batch = BlockAccessor.for_block(shuffle_buffer).slice(
            batch_head, batch_head + bs
        )
        batch_head += bs
        rows.extend(batch.column("val").to_pylist())

    return rows


@pytest.mark.parametrize(
    "batch_size,buffer_size,num_blocks,block_size",
    [
        (5, 20, 10, 10),
        (1, 10, 5, 20),
        (10, 10, 3, 50),
        (7, 30, 8, 15),
        (100, 100, 2, 200),
    ],
)
def test_incremental_index_matches_full_method(
    batch_size, buffer_size, num_blocks, block_size
):
    """Verify that the incremental index method yields the same multiset of
    rows as the old full-shuffle reference implementation."""

    seed = 42
    blocks = [
        pa.table({"val": list(range(i * block_size, (i + 1) * block_size))})
        for i in range(num_blocks)
    ]

    # Incremental index method (current implementation).
    batcher = ShufflingBatcher(
        batch_size=batch_size,
        shuffle_buffer_min_size=buffer_size,
        shuffle_seed=seed,
    )
    for block in blocks:
        batcher.add(block)
    batcher.done_adding()
    rows_index = []
    while batcher.has_batch() or batcher.has_any():
        batch = batcher.next_batch()
        rows_index.extend(batch.column("val").to_pylist())

    # Full-shuffle reference.
    rows_full = _collect_rows_full_method(blocks, batch_size, buffer_size, seed)

    total_rows = num_blocks * block_size
    assert len(rows_index) == total_rows
    assert len(rows_full) == total_rows
    assert sorted(rows_index) == sorted(rows_full) == list(range(total_rows))


def test_no_partial_batch_mid_stream():
    """has_batch() must not return True when total rows < batch_size.

    With SHUFFLE_BUFFER_COMPACTION_THRESHOLD < 1.0, _min_rows_to_yield_batch
    can be less than batch_size. If we drain the compacted buffer below
    batch_size while no uncompacted rows are available, has_batch() must
    return False — otherwise next_batch() would return a partial batch
    mid-stream.
    """
    batch_size = 10
    buffer_size = 10  # common case: equal to batch_size

    batcher = ShufflingBatcher(
        batch_size=batch_size,
        shuffle_buffer_min_size=buffer_size,
        shuffle_seed=0,
    )

    # Add enough rows to trigger compaction and yield some batches.
    batcher.add(gen_block(35))

    # Consume batches until the compacted buffer is partially drained.
    batches = []
    while batcher.has_batch():
        batch = batcher.next_batch()
        batches.append(batch)
        # Every batch returned mid-stream must be full.
        assert (
            len(batch) == batch_size
        ), f"got partial batch of {len(batch)} rows mid-stream"

    # At this point has_batch() is False. There may be leftover rows
    # (< batch_size) but they should not be yielded until done_adding.
    leftover = batcher._num_rows()
    assert leftover < batch_size

    # After done_adding, the remaining rows should drain as a partial batch.
    batcher.done_adding()
    assert batcher.has_any()
    final_batch = batcher.next_batch()
    assert len(final_batch) == leftover

    total = sum(len(b) for b in batches) + len(final_batch)
    assert total == 35


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
