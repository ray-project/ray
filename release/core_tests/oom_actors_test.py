"""oom actor test

Saturates the cluster with actors trying to OOM the node.

Test owner: clarng

Acceptance criteria: Should run through and print "PASSED"
"""

from math import ceil
import time
import ray
import psutil
import pytest


def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> None:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = node_mem.total * pct - used
    assert bytes_needed > 0, "node has less memory than what is requested"
    return bytes_needed


@ray.remote(num_cpus=1, max_restarts=100)
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes: int, sleep_time_s: int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(allocate_bytes / 8)
        self.leaks.append(new_list)

        time.sleep(sleep_time_s / 1000)

    def get_worker_id(self):
        return ray._private.worker.global_worker.core_worker.get_worker_id().hex()

    def get_actor_id(self):
        return ray._private.worker.global_worker.core_worker.get_actor_id().hex()


if __name__ == "__main__":
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    actor_refs = [
        Leaker.remote().allocate.remote(allocate_bytes=bytes_to_alloc)
        for _ in range(16)
    ]
    with pytest.raises(ray.exceptions.RayActorError) as _:
      ray.get(actor_refs)
    print("PASSED: Actors trying to OOM did not crash the cluster")
