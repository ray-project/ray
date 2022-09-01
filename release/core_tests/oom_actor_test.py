
"""Job submission test

This test runs a basic Tune job on a remote cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

from math import ceil
import time
import ray
import psutil


def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> None:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = node_mem.total * pct - used
    assert bytes_needed > 0, "node has less memory than what is requested"
    return bytes_needed


@ray.remote(max_retries=-1)
def inf_retry(
    allocate_bytes: int, num_chunks: int = 10, allocate_interval_s: float = 0
):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    return end - start


if __name__ == "__main__":
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    ray.get(inf_retry.remote(bytes_to_alloc))
    