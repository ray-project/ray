import gc
from math import ceil
import os
import pickle
import platform
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager

import numpy as np
import psutil
import pytest

import ray



def n():
    return 1

@ray.remote
def t1():
    return n()

@ray.remote
def t2():
    return n()


@ray.remote(max_retries=-1)
def inf_retry(allocate_bytes : int, num_chunks : int = 10, allocate_interval_s : float = 0):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0]*ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    return end - start

@ray.remote(max_retries=0)
def no_retry(allocate_bytes : int, num_chunks : int = 10, allocate_interval_s : float = 0):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0]*ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    return end - start

@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes : int, sleep_time_s : int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0]*ceil(allocate_bytes / 8) 
        self.leaks.append(new_list)
        
        time.sleep(sleep_time_s / 1000)

    
def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> None:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = (node_mem.total * pct - used)
    assert bytes_needed > 0
    return bytes_needed


def test_memory_pressure_kill_worker(shutdown_only):
    node_high_memory_usage_fraction = 0.7
    memory_monitor_interval_ms = 100

    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "node_high_memory_usage_fraction": node_high_memory_usage_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
        },
    )

    leaker = Leaker.remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.90)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(leaker.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))


def test_memory_pressure_kill_newest_worker(shutdown_only):
    node_high_memory_usage_fraction = 0.7
    memory_monitor_interval_ms = 100

    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "node_high_memory_usage_fraction": node_high_memory_usage_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
        },
    )

    leaker1 = Leaker.options(name="leaker1").remote()
    leaker2 = Leaker.options(name="leaker2").remote()
    
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.4)
    ray.get(leaker1.allocate.remote(bytes_to_alloc, 0))
    
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker2.allocate.remote(bytes_to_alloc, 0))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.8)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(leaker2.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))
    
    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "leaker1" in actors
    


def test_memory_pressure_above_single_actor(shutdown_only):
    node_high_memory_usage_fraction = 0.8
    memory_monitor_interval_ms = 200

    ray.init(
        num_cpus=10,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "node_high_memory_usage_fraction": node_high_memory_usage_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
        },
    )

    # Ensure the current usage is below memory monitor threshold
    bytes_needed = get_additional_bytes_to_reach_memory_usage_pct(node_high_memory_usage_fraction)
    assert bytes_needed > 0

    # Ensure a single task's memory usage does not trigger the memory monitor, which will kill it
    bytes_used_per_task = bytes_needed * 0.9

    start = time.time()
    num_tasks = 5
    result_refs = []
    result_refs.append(no_retry.remote(bytes_used_per_task, 5, memory_monitor_interval_ms / 1000))
    result_refs.extend([inf_retry.remote(bytes_used_per_task, 5, memory_monitor_interval_ms / 1000) for _ in range(num_tasks-1)])

    
    results = ray.get(result_refs)

    end = time.time()

    print(f'time taken {end - start}, time per remote call {results}')


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
