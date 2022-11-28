# flake8: noqa
import ray

ray.init(
    _system_config={
        "memory_usage_threshold": 0.4,
    },
)
# fmt: off
# __oom_start__
import ray

@ray.remote(max_retries=0)
def allocate_memory():
    chunks = []
    bits_to_allocate = 8 * 100 * 1024 * 1024  # ~0.1 GiB
    while True:
        chunks.append([0] * bits_to_allocate)


try:
    ray.get(allocate_memory.remote())
except ray.exceptions.OutOfMemoryError as ex:
    print("task failed with OutOfMemoryError, which is expected")
# __oom_end__
# fmt: on


# fmt: off
# __two_actors_start__
from math import ceil
import ray
from ray._private.utils import (
    get_system_memory,
)  # do not use outside of this example as these are private methods.
from ray._private.utils import (
    get_used_memory,
)  # do not use outside of this example as these are private methods.


# estimates the number of bytes to allocate to reach the desired memory usage percentage.
def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
    used = get_used_memory()
    total = get_system_memory()
    bytes_needed = int(total * pct) - used
    assert (
        bytes_needed > 0
    ), "memory usage is already above the target. Increase the target percentage."
    return bytes_needed


@ray.remote
class MemoryHogger:
    def __init__(self):
        self.allocations = []

    def allocate(self, bytes_to_allocate: float) -> None:
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(bytes_to_allocate / 8)
        self.allocations.append(new_list)


first_actor = MemoryHogger.options(
    max_restarts=1, max_task_retries=1, name="first_actor"
).remote()
second_actor = MemoryHogger.options(
    max_restarts=0, max_task_retries=0, name="second_actor"
).remote()

# each task requests 0.3 of the system memory when the memory threshold is 0.4.
allocate_bytes = get_additional_bytes_to_reach_memory_usage_pct(0.3)

first_actor_task = first_actor.allocate.remote(allocate_bytes)
second_actor_task = second_actor.allocate.remote(allocate_bytes)

error_thrown = False
try:
    ray.get(first_actor_task)
except ray.exceptions.RayActorError as ex:
    error_thrown = True
    print("first actor was killed by memory monitor")
assert error_thrown

ray.get(second_actor_task)
print("finished second actor")
# __two_actors_end__
# fmt: on
