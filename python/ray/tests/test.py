from math import ceil
import time

import psutil

import ray

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

@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes: int, sleep_time_s: int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(allocate_bytes / 8)
        self.leaks.append(new_list)

        time.sleep(sleep_time_s / 1000)


ray.init('auto')
leaker = Leaker.remote()

bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
ray.get(inf_retry.remote(allocate_bytes = bytes_to_alloc))




