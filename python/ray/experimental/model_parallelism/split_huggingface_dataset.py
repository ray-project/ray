import time
from datasets import load_dataset

import ray


AUTH_TOKEN = "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"
CACHE_DIR = "/mnt/cluster_storage/bigcode"
NUM_SHARDS = 2_000
SPLIT_PARQUET_PREFIX = "/mnt/cluster_storage/bigcode/split_parquet/shard"
MAX_CONCURRENT_TASKS = 96 * 2


# full dataset (3TB of data)
print(f"Loading dataset from {CACHE_DIR}")
start = time.perf_counter()
ds = load_dataset("bigcode/the-stack-dedup", split="train", use_auth_token=AUTH_TOKEN, cache_dir=CACHE_DIR)
print(f"Finished loading huggingface dataset after {time.perf_counter() - start:0.2f} secs.")


@ray.remote(num_cpus=0.5)
def shard_and_write_task(ds, num_shards, index):
    shard = ds.shard(num_shards, index, contiguous=True)
    print(f"Spliting and writing shard {index} of {num_shards} with {shard.num_rows} rows to {SPLIT_PARQUET_PREFIX}_{index}.parquet ...")
    shard.to_parquet(f"{SPLIT_PARQUET_PREFIX}_{index}.parquet")


shard_and_write_tasks = []
for i in range(NUM_SHARDS):
    if len(shard_and_write_tasks) > MAX_CONCURRENT_TASKS:
        ready_refs, shard_and_write_tasks = ray.wait(shard_and_write_tasks, num_returns=1)
        ray.get(ready_refs)

    shard_and_write_tasks.append(
        shard_and_write_task.remote(ds, NUM_SHARDS, i)
    )

start = time.perf_counter()
ray.get(shard_and_write_tasks)
print(f"Sharing and writing {NUM_SHARDS} shards took {time.perf_counter() - start:0.2f} secs.")