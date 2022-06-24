import argparse
import ray
import time
import sys

import pandas as pd
import numpy as np

parser = argparse.ArgumentParser("Benchmarking spilling performance")
parser.add_argument("--data-type", type=int, default=0)  # 0: tensor 1: parquet
parser.add_argument(
    "--batch-size",
    type=int,
    default=2500,
)
parser.add_argument("--num-files", type=int, default=30)
args = parser.parse_args()


def create_dataset():
    if args.data_type == 0:
        # Tensor data: create a dataset with 30 GiB, and 512 MiB/block
        NUM_GB = 30
        BLOCK_SIZE_GB = 0.5
        # 80*80*4*8*5000 = 1 GiB
        ds = ray.data.range_tensor(
            5000 * NUM_GB, shape=(80, 80, 4), parallelism=int(NUM_GB / BLOCK_SIZE_GB)
        )
    else:
        assert args.data_type == 1
        # Parquet data
        files = [
            f"s3://ursa-labs-taxi-data/{year}/{str(month).zfill(2)}/data.parquet"
            for year in range(2017, 2019) for month in range(1, 13)
        ]
        ds = ray.data.read_parquet(files)
    return ds


print("Starting consume the dataset")
start = time.perf_counter()
batch_start = start
num_batches, num_bytes = 0, 0
batch_delays = []

ds = create_dataset()
print("ds:", ds)
for batch in ds.iter_batches():
    num_batches += 1
    batch_delay = time.perf_counter() - batch_start
    batch_delays.append(batch_delay)

    if isinstance(batch, pd.DataFrame):
        num_bytes += int(batch.memory_usage(index=True, deep=True).sum())
    elif isinstance(batch, np.ndarray):
        num_bytes += batch.nbytes
    else:
        # NOTE: This isn't recursive and will just return the size of
        # the object pointers if list of non-primitive types.
        num_bytes += sys.getsizeof(batch)

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
