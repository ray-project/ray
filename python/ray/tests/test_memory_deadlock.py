import sys
import time
import threading

import pytest

import ray

from ray.tests.test_memory_pressure import (
    ray_with_memory_monitor,
    allocate_memory,
    Leaker,
    get_additional_bytes_to_reach_memory_usage_pct,
    memory_usage_threshold_fraction,
)


# Utility for allocating locally (ray not involved)
def alloc_mem(bytes):
    chunks = 10
    mem = []
    bytes_per_chunk = bytes // 8 // chunks
    for _ in range(chunks):
        mem.append([0] * bytes_per_chunk)
    return mem


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_churn_long_running(
    ray_with_memory_monitor,
):
    long_running_bytes = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction - 0.1
    )
    ray.get(
        allocate_memory.options(max_retries=1).remote(
            long_running_bytes, post_allocate_sleep_s=30
        )
    )
    small_bytes = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction + 0.2
    )
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(allocate_memory.options(max_retries=1).remote(small_bytes))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_task_with_nested_task(
    ray_with_memory_monitor,
):
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        bytes1 = get_additional_bytes_to_reach_memory_usage_pct(
            memory_usage_threshold_fraction - 0.1
        )
        bytes2 = (
            get_additional_bytes_to_reach_memory_usage_pct(
                memory_usage_threshold_fraction + 0.2
            )
            - bytes1
        )
        ray.get(
            task_with_nested_task.remote(
                task_bytes=bytes1, nested_task_bytes=bytes2, barrier=None
            )
        )


@ray.remote
def task_with_nested_actor(
    first_fraction, second_fraction, actor_allocation_first=True
):
    leaker = Leaker.options(max_restarts=1, max_task_retries=1).remote()
    first_bytes = get_additional_bytes_to_reach_memory_usage_pct(first_fraction)
    if actor_allocation_first:
        ray.get(leaker.allocate.remote(first_bytes))
        second_bytes = get_additional_bytes_to_reach_memory_usage_pct(second_fraction)
        dummy = alloc_mem(second_bytes)
    else:
        dummy = alloc_mem(first_bytes)
        second_bytes = get_additional_bytes_to_reach_memory_usage_pct(second_fraction)
        ray.get(leaker.allocate.remote(second_bytes))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_task_with_nested_actor_swap_order(
    ray_with_memory_monitor,
):
    with pytest.raises(ray.exceptions.RayTaskError) as _:
        ray.get(
            task_with_nested_actor.remote(
                first_fraction=memory_usage_threshold_fraction - 0.1,
                second_fraction=memory_usage_threshold_fraction + 0.25,
                actor_allocation_first=False,
            )
        )


@ray.remote
class ActorWithNestedTask:
    def __init__(self):
        self.mem = []

    def alloc_local(self, num_bytes):
        self.mem = alloc_mem(num_bytes)

    def alloc_remote(self, num_bytes):
        ray.get(allocate_memory.options(max_retries=1).remote(num_bytes))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_actor_with_nested_task(
    ray_with_memory_monitor,
):
    leaker = ActorWithNestedTask.options(max_restarts=1, max_task_retries=1).remote()
    bytes_first = get_additional_bytes_to_reach_memory_usage_pct(
        (memory_usage_threshold_fraction + 0.25) / 2
    )
    ray.get(leaker.alloc_local.remote(bytes_first))

    bytes_second = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction + 0.25
    )
    with pytest.raises(ray.exceptions.RayTaskError) as _:
        ray.get(leaker.alloc_remote.remote(bytes_second))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_actor_with_nested_task_two(
    ray_with_memory_monitor,
):
    leaker1 = ActorWithNestedTask.options(max_restarts=1, max_task_retries=1).remote()
    leaker2 = ActorWithNestedTask.options(max_restarts=1, max_task_retries=1).remote()
    one_third = (memory_usage_threshold_fraction + 0.25) / 3
    bytes_first = get_additional_bytes_to_reach_memory_usage_pct(one_third)
    ray.get(leaker1.alloc_local.remote(bytes_first))
    bytes_second = get_additional_bytes_to_reach_memory_usage_pct(2 * one_third)
    ray.get(leaker2.alloc_local.remote(bytes_second))

    bytes_third = get_additional_bytes_to_reach_memory_usage_pct(3 * one_third)
    with pytest.raises(ray.exceptions.RayTaskError) as _:
        ray.get(leaker1.alloc_remote.remote(bytes_third))


# Used for syncing allocations
@ray.remote
class BarrierActor:
    def __init__(self, num_objects):
        self.barrier = threading.Barrier(num_objects, timeout=30)

    def wait_all_done(self):
        self.barrier.wait()


@ray.remote
def task_with_nested_task(task_bytes, nested_task_bytes, barrier=None):
    dummy = alloc_mem(task_bytes)
    if barrier:
        ray.get(barrier.wait_all_done.remote())
    ray.get(allocate_memory.options(max_retries=0).remote(nested_task_bytes))
    # Barrier again before exiting, so task #1 will not prematurely finish and release memory.
    if barrier:
        ray.get(barrier.wait_all_done.remote())


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_multiple_tasks_with_nested_task(
    ray_with_memory_monitor,
):
    bytes_first = get_additional_bytes_to_reach_memory_usage_pct(0.3)
    thirty_percent = get_additional_bytes_to_reach_memory_usage_pct(0.6) - bytes_first

    barrier = BarrierActor.options(
        max_restarts=1, max_task_retries=1, max_concurrency=2
    ).remote(2)

    first = task_with_nested_task.remote(bytes_first, thirty_percent, barrier)
    second = task_with_nested_task.remote(thirty_percent, thirty_percent, barrier)
    ray.get(first)
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(second)
