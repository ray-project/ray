import pytest
import sys
import time

import pyarrow as pa

import ray
from ray.data._internal.batcher import AsyncBatcher, Batcher
from ray.data.context import DatasetContext

def gen_block(num_rows):
    return pa.table({"foo": [1] * num_rows})


# Test for 3 cases
# 1. Batch size is less than block size
# 2. Batch size is more than block size
# 3. Block size is not divisble by batch size
@pytest.mark.parametrize("batch_size", [4, 10, 7])
def test_async_batcher(batch_size):
    """Tests that async batcher overlaps compute"""
    class SleepBatcher(Batcher):
        def next_batch(self, batch_format: str = "default"):
            time.sleep(2)
            return super().next_batch(batch_format)

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
    # Total time should be based on number of times the udf is called (which is equal to len(outputs)).
    # The 2 seconds sleep in next_batch is overlapped, so does not count towards total time.
    assert total_time < len(outputs)*3 + 3 # 3 second buffer for very first batch.

    # There should be no dropped rows.
    assert sum(len(output_batch) for output_batch in outputs) == 40, sum(len(output_batch) for output_batch in outputs)  # 10 blocks with 8 rows each.

def test_async_batcher_timeout():
    class SleepBatcher(Batcher):
        def next_batch(self, batch_format: str = "default"):
            time.sleep(2)
            return super().next_batch(batch_format)

    base_batcher = SleepBatcher(batch_size=1)
    async_batcher = AsyncBatcher(base_batcher=base_batcher, fetch_timeout_s=1)

    for _ in range(2):
        new_block = gen_block(num_rows=1)
        async_batcher.add(new_block)
    async_batcher.done_adding()

    async_batcher.next_batch() # First batch is fetched on the main thread, so no error.
    with pytest.raises(RuntimeError):
        async_batcher.next_batch() # Subsequent batches go through the queue, which will raise an error.

def test_async_batcher_buffer_size():
    pass

def test_async_map_batches(shutdown_only):
    # Test 1 large block with small batch size, which results in a lot of batching.
    # This batching is overlapped with udf computation.
    block_size = 1000
    batch_size = 10

    context = DatasetContext.get_current()
    context.actor_prefetcher_enabled = False
    
    dataset = ray.data.range(block_size, parallelism=1)
    
    def sleep_udf(batch):
        time.sleep(1)
        return batch

    start_time = time.time()
    dataset.map_batches(sleep_udf, batch_size=batch_size)
    total_time = time.time() - start_time

    print(f"total time: {total_time}")
    assert total_time < block_size / batch_size + 1 # 1 second buffer.

@pytest.mark.parametrize("local_shuffle_buffer_size", [None, 5])
def test_async_iter_batches(shutdown_only, local_shuffle_buffer_size):
    # Test 1 large block with small batch size, which results in a lot of batching.
    # This batching is overlapped with udf computation.
    block_size = 1000
    batch_size = 10
    
    dataset = ray.data.range(block_size, parallelism=1)
    
    def sleep_udf(batch):
        time.sleep(1)
        return batch

    start_time = time.time()
    for batch in dataset.iter_batches(batch_size=batch_size, local_shuffle_buffer_size=local_shuffle_buffer_size):
        sleep_udf(batch)
    total_time = time.time() - start_time

    assert total_time < block_size / batch_size + 1 # 1 second buffer.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))