import ray
import time

import numpy as np

# Create a dataset with 30 GiB, and 512 MiB/block
NUM_GB = 30
BLOCK_SIZE_GB = 0.5

# 80*80*4*8*5000 = 1 GiB
ds = ray.data.range_tensor(
    5000 * NUM_GB, shape=(80, 80, 4), parallelism=int(NUM_GB / BLOCK_SIZE_GB)
)

print("Starting consume the dataset")
start = time.perf_counter()
batch_start = start
num_batches, num_bytes = 0, 0
batch_delays = []

for batch in ds.iter_batches():
    num_batches += 1
    batch_delay = time.perf_counter() - batch_start
    batch_delays.append(batch_delay)
    assert isinstance(batch, np.ndarray)
    num_bytes += batch.nbytes
    batch_start = time.perf_counter()

duration = time.perf_counter() - start

print("Total time to iterate all data:", duration)
print("Total number of batches:", num_batches)
print(
    "P50/P95/Max batch delay (s)",
    np.quantile(batch_delays, 0.5),
    np.quantile(batch_delays, 0.95),
    np.max(batch_delays),
)
print("Total number of bytes read:", round(num_bytes / (1024 * 1024), 2), "MiB")
print("Mean throughput", round(num_bytes / (1024 * 1024) / duration, 2), "MiB/s")
