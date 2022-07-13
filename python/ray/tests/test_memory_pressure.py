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



@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes, node_high_memory_monitor_min_interval_s):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0]*ceil(allocate_bytes / 8) 
        self.leaks.append(new_list)
        
        time.sleep(node_high_memory_monitor_min_interval_s * 3)

    
def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> None:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = (node_mem.total * pct - used)
    assert bytes_needed > 0
    return bytes_needed


def test_memory_pressure_kill_worker(shutdown_only):
    node_high_memory_usage_fraction = 0.7
    node_high_memory_monitor_min_interval_s = 1

    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "node_high_memory_usage_fraction": node_high_memory_usage_fraction,
            "node_high_memory_monitor_min_interval_s": node_high_memory_monitor_min_interval_s,
        },
    )

    leaker = Leaker.remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker.allocate.remote(bytes_to_alloc, node_high_memory_monitor_min_interval_s))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.85)
    with pytest.raises(ray.exceptions.RayActorError) as exception:
        ray.get(leaker.allocate.remote(bytes_to_alloc, node_high_memory_monitor_min_interval_s))

    # with pytest.raises(ValueError) as _:
    #     ray.get_actor("leaker2")


def test_memory_pressure_kill_newest_worker(shutdown_only):
    node_high_memory_usage_fraction = 0.7
    node_high_memory_monitor_min_interval_s = 3

    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "node_high_memory_usage_fraction": node_high_memory_usage_fraction,
            "node_high_memory_monitor_min_interval_s": node_high_memory_monitor_min_interval_s,
        },
    )

    leaker1 = Leaker.options(name="leaker1").remote()
    leaker2 = Leaker.options(name="leaker2").remote()
    
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.4)
    ray.get(leaker1.allocate.remote(bytes_to_alloc, 0))
    
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker2.allocate.remote(bytes_to_alloc, 0))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.8)
    ray.get(leaker1.allocate.remote(bytes_to_alloc, node_high_memory_monitor_min_interval_s))
    
    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "leaker1" in actors

if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
