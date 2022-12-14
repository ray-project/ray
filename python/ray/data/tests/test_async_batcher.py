import pytest
import time

import pyarrow as pa

import ray
from ray.data._internal.batcher import AsyncBatcher, Batcher


def gen_block(num_rows):
    return pa.table({"foo": [1] * num_rows})


# Test for 3 cases
# 1. Batch size is less than block size
# 2. Batch size is more than block size
# 3. Block size is not divisble by batch size
@pytest.mark.parametrize("batch_size", [4, 10, 7])
def test_async_batcher(batch_size):
    """Tests that async batcher overlaps compute."""

    class SleepBatcher(Batcher):
        def next_batch(self):
            time.sleep(2)
            return super().next_batch()

    base_batcher = SleepBatcher(batch_size=batch_size)
    async_batcher = AsyncBatcher(base_batcher=base_batcher, fetch_timeout_s=5)

    def sleep_udf(batch):
        time.sleep(3)
        return batch

    for _ in range(5):
        new_block = gen_block(num_rows=8)
        async_batcher.add(new_block)
    async_batcher.done_adding()

    start_time = time.time()
    outputs = []
    while async_batcher.has_any():
        next_batch = async_batcher.next_batch()
        outputs.append(sleep_udf(next_batch))
    end_time = time.time()

    total_time = end_time - start_time
    # Total time should be based on number of times the udf is called
    # (which is equal to len(outputs)).
    # The 2 seconds sleep in next_batch is overlapped, so does not count
    # towards total time.
    assert total_time < len(outputs) * 3 + 3  # 3 second buffer for very first batch.

    # There should be no dropped rows.
    assert sum(len(output_batch) for output_batch in outputs) == 40, sum(
        len(output_batch) for output_batch in outputs
    )  # 10 blocks with 8 rows each.


def test_async_batcher_timeout():
    class SleepBatcher(Batcher):
        def next_batch(self):
            time.sleep(2)
            return super().next_batch()

    base_batcher = SleepBatcher(batch_size=1)
    async_batcher = AsyncBatcher(base_batcher=base_batcher, fetch_timeout_s=1)

    for _ in range(2):
        new_block = gen_block(num_rows=1)
        async_batcher.add(new_block)
    async_batcher.done_adding()

    # First batch is fetched on the main thread, so no error.
    async_batcher.next_batch()

    # Subsequent batches go through the queue, which will raise an error.
    with pytest.raises(RuntimeError):
        async_batcher.next_batch()


def test_async_batcher_buffer_size():
    """Tests that multiple batches can be prefetched at a time with
    larger buffer size."""

    class SleepBatcher(Batcher):
        def _next_batch(self):
            time.sleep(1)
            return super()._next_batch()

    base_batcher = SleepBatcher(batch_size=4)
    async_batcher = AsyncBatcher(
        base_batcher=base_batcher, fetch_timeout_s=5, buffer_max_size=4
    )

    def sleep_udf(batch):
        time.sleep(5)
        return batch

    block = gen_block(num_rows=20)

    async_batcher.add(block)
    async_batcher.done_adding()

    sleep_udf(async_batcher.next_batch())

    # All 4 batches should be buffered after applying the udf on the first batch.
    assert async_batcher.buffer.qsize() == 4


def map_batches_test(dataset, batch_size, sleep_time, prefetch_batches) -> int:
    def sleep_udf(batch):
        time.sleep(sleep_time)
        return batch

    start_time = time.time()
    dataset.map_batches(
        sleep_udf,
        batch_size=batch_size,
        batch_format="numpy",
        prefetch_batches=prefetch_batches,
    )
    total_time = time.time() - start_time
    return total_time


def test_async_map_batches(shutdown_only):
    """Tests that async map_batches with a non-CPU bound UDF is
    faster than sync map_batches."""
    # Large batch size makes batch formatting much slower, making overlap more useful.
    # This batching is overlapped with udf computation.
    dataset_size = 200
    batch_size = dataset_size / 5
    sleep_time = 0.5

    dataset = ray.data.range_tensor(dataset_size, shape=(100, 100, 100), parallelism=1)
    assert dataset.num_blocks() == 1

    # Do a dummy map_batches run to address any initial overhead.
    dataset.map_batches(lambda x: x, batch_size=batch_size)

    total_time_with_prefetch = map_batches_test(
        dataset, batch_size, sleep_time, prefetch_batches=1
    )

    total_time_no_prefetch = map_batches_test(
        dataset, batch_size, sleep_time, prefetch_batches=0
    )

    assert total_time_with_prefetch < total_time_no_prefetch


def iter_batches_test(dataset, batch_size, sleep_time, prefetch_batches) -> int:
    def sleep_udf(batch):
        time.sleep(sleep_time)
        return batch

    start_time = time.time()
    for batch in dataset.iter_batches(
        batch_size=batch_size, batch_format="numpy", prefetch_batches=prefetch_batches
    ):
        sleep_udf(batch)
    total_time = time.time() - start_time
    return total_time


@pytest.mark.parametrize("local_shuffle_buffer_size", [None, 20, 50])
def test_async_iter_batches(shutdown_only, local_shuffle_buffer_size):
    """Tests that async iter_batches with a non-CPU bound UDF is faster
    than sync map_batches."""
    # Test 1 block with large batch size.
    # Large batch size makes batch formatting much slower, making overlap more useful.
    # This batching is overlapped with udf computation.
    block_size = 200
    batch_size = 40
    sleep_time = 0.5

    dataset = ray.data.range_tensor(block_size, shape=(100, 100, 100), parallelism=1)
    # Do a dummy iter_batches run to address any initial overhead.
    for _ in dataset.iter_batches():
        pass

    total_time_with_prefetch = iter_batches_test(
        dataset, batch_size, sleep_time, prefetch_batches=1
    )

    total_time_no_prefetch = iter_batches_test(
        dataset, batch_size, sleep_time, prefetch_batches=0
    )

    assert total_time_with_prefetch < total_time_no_prefetch


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
