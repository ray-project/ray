"""oom task test

Saturates the cluster with tasks trying to OOM the node.

Test owner: clarng

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


@ray.remote(max_retries=100)
def try_to_oom(
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
    task_refs = [
        try_to_oom.remote(allocate_bytes=bytes_to_alloc, allocate_interval_s=1)
        for _ in range(16)
    ]
    ray.get(task_refs)
    print("PASSED: Tasks trying to OOM did not crash the cluster")
