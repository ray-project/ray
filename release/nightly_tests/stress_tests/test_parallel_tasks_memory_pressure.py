"""Release test script to test Ray OOM killer on parallel tasks.

This test submits a set of parallel tasks, each of which allocates a configured portion
of the available memory on the host. The number of CPUs available (and therefore number
of tasks running in parallel) and the amount of memory allocated by each task is
expected to be tuned to trigger the Ray memory monitor.

The expected behavior of the test is:
    - When all tasks run in parallel, they exceed the memory monitor's threshold for
      the node.
    - The memory monitor should kill workers running the tasks. The policy should
      select the tasks that have started running more recently, allowing the workload
      to progress despite frequent OOMs.
    - The tasks should retry infinitely due to our current default policy for OOM.
    - All of the tasks should eventually complete successfully.
"""
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
    help="Total number of tasks to execute.",
    type=int,
    default=8,
)

parser.add_argument(
    "--mem-pct-per-task",
    help="Fraction of the node's available memory to allocate per task.",
    type=float,
    default=0.5,
)


@ray.remote
def allocate_memory(
    target_bytes: int,
    *,
    num_chunks: int = 10,
    allocate_interval_s: float = 5,
) -> int:
    chunks = []
    total_allocated_bytes = 0
    for _ in range(num_chunks):
        chunk = np.empty(ceil(target_bytes / num_chunks), dtype=np.uint8)
        chunk.fill(1)
        chunks.append(chunk)
        total_allocated_bytes += len(chunk)

        # If all tasks try to allocate memory at the same time,
        # the memory monitor might not be able to kill them in time.
        # To avoid this, we introduce jitter in the sleep interval.
        time.sleep(allocate_interval_s * random.random())

    return total_allocated_bytes


def main(*, num_tasks: int, mem_pct_per_task: float):
    ray.init()

    # First run some warmup tasks before estimating the steady state memory consumption.
    warm_up_start = time.time()
    print(f"Running {num_tasks} warm up tasks, each allocating 100 MiB.")
    ray.get(
        [
            allocate_memory.remote(100 * 1024**2, num_chunks=1)
            for _ in range(num_tasks)
        ]
    )
    print(f"Warm up tasks finished in {time.time()-warm_up_start:.2f}s.")

    total_bytes, used_bytes = get_system_memory(), get_used_memory()
    bytes_per_task = int((total_bytes - used_bytes) * mem_pct_per_task)
    gib_per_task = bytes_per_task / 1024**3

    # When a task or actor is killed by the memory monitor
    # it will be retried with exponential backoff.
    start = time.time()
    print(f"Running {num_tasks} tasks, each allocating {gib_per_task:.2f} GiB.")
    unready = [allocate_memory.remote(bytes_per_task) for _ in range(num_tasks)]
    while len(unready) > 0:
        [ready], unready = ray.wait(unready, num_returns=1)
        assert ray.get(ready) >= bytes_per_task
        print(
            f"{num_tasks-len(unready)} / {num_tasks} tasks have completed in {time.time()-start:.2f}s."
        )

    end = time.time()
    print(f"All tasks completed in {end-start:.2f} seconds.")


if __name__ == "__main__":
    main(**vars(parser.parse_args()))
