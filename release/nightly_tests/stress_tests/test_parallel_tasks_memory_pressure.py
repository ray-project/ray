import argparse
import random
import time
from math import ceil

import numpy as np

import ray
from ray._private.utils import get_system_memory, get_used_memory

parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-tasks",
    help="number of tasks to process in total",
    default="20",
    type=int,
)

parser.add_argument(
    "--mem-pct-per-task",
    help="memory to allocate per task as a fraction of the node's available memory",
    default="0.45",
    type=float,
)


@ray.remote(max_retries=-1)
def allocate_memory(
    target_bytes: int,
    *,
    num_chunks: int = 10,
    allocate_interval_s: float = 10,
):
    chunks = []
    for _ in range(num_chunks):
        chunk = np.empty(ceil(target_bytes / num_chunks), dtype=np.uint8)
        chunk.fill(1)
        chunks.append(chunk)

        # If all tasks try to allocate memory at the same time,
        # the memory monitor might not be able to kill them in time.
        # To avoid this, we introduce jitter in the sleep interval.
        time.sleep(allocate_interval_s * random.random())


def main(*, num_tasks: int, mem_pct_per_task: float):
    # First run some warmup tasks before estimating the steady state memory consumption.
    print(f"Running {num_tasks} warm up tasks, each allocating 100 MiB.")
    # ray.get([allocate_memory.remote(100 * 1024**2, num_chunks=1) for _ in range(num_tasks)])
    print("Warm up tasks finished.")

    total_bytes, used_bytes = get_system_memory(), get_used_memory()
    bytes_per_task = int((total_bytes - used_bytes) * mem_pct_per_task)
    gib_per_task = bytes_per_task / 1024**3

    # When a task or actor is killed by the memory monitor
    # it will be retried with exponential backoff.
    start = time.time()
    print(f"Running {num_tasks} tasks, each allocating {gib_per_task:.2f} GiB.")
    ray.get([allocate_memory.remote(bytes_per_task) for _ in range(num_tasks)])
    end = time.time()

    print(f"Tasks completed in {end-start:.2f} seconds.")


if __name__ == "__main__":
    main(**vars(parser.parse_args()))
