# fmt: off
# __oom_start__
import ray

@ray.remote
def allocate_memory():
    chunks = []
    bits_to_allocate = 8 * 1024 * 1024 * 1024 # 1 GiB
    while True:
        chunks.append([0] * bits_to_allocate)

ray.get(allocate_memory.remote())
# __oom_end__
# fmt: on


# fmt: off
# __two_actors_start__
from math import ceil
import ray
from ray._private.utils import get_system_memory # do not use outside of this example as these are private methods.
from ray._private.utils import get_used_memory # do not use outside of this example as these are private methods.

# estimates the number of bytes to allocate to reach the desired memory usage percentage.
def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
    used = get_used_memory()
    total = get_system_memory()
    bytes_needed = int(total * pct) - used
    assert bytes_needed > 0, "memory usage is already above the target. Increase the target percentage."
    return bytes_needed

@ray.remote
class MemoryHogger:
    def __init__(self):
        self.allocations = []

    def allocate(self, bytes_to_allocate: float) -> None:
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(bytes_to_allocate / 8)
        self.allocations.append(new_list)

first_actor = MemoryHogger.options(max_restarts=1, max_task_retries=1, name="first_actor").remote()
second_actor = MemoryHogger.options(max_restarts=0, max_task_retries=0, name="second_actor").remote()

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


# fmt: off
# __set_num_cpus_summary_start__
first_actor = MemoryHogger.options(max_restarts=1, max_task_retries=1, name="first_actor", num_cpus=2).remote()
second_actor = MemoryHogger.options(max_restarts=0, max_task_retries=0, name="second_actor", num_cpus=2).remote()
# __set_num_cpus_summary_end__
# fmt: on


# fmt: off
# __set_num_cpus_start__
from math import ceil
import ray
from ray._private.utils import get_system_memory # do not use outside of this example as these are private methods.
from ray._private.utils import get_used_memory # do not use outside of this example as these are private methods.

# estimates the number of bytes to allocate to reach the desired memory usage percentage.
def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
    used = get_used_memory()
    total = get_system_memory()
    bytes_needed = int(total * pct) - used
    assert bytes_needed > 0, "memory usage is already above the target. Increase the target percentage."
    return bytes_needed

@ray.remote
class MemoryHogger:
    def __init__(self):
        self.allocations = []

    def allocate(self, bytes_to_allocate: float) -> None:
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(bytes_to_allocate / 8)
        self.allocations.append(new_list)

first_actor = MemoryHogger.options(max_restarts=1, max_task_retries=1, name="first_actor", num_cpus=2).remote()
second_actor = MemoryHogger.options(max_restarts=0, max_task_retries=0, name="second_actor", num_cpus=2).remote()

# each task requests 0.3 of the system memory when the memory threshold is 0.4.
allocate_bytes = get_additional_bytes_to_reach_memory_usage_pct(0.3)
      
first_actor_task = first_actor.allocate.remote(allocate_bytes)
second_actor_task = second_actor.allocate.remote(allocate_bytes)

ray.get(first_actor_task)
ray.kill(first_actor)
print("finished first actor")

ray.get(second_actor_task)
print("finished second actor")
# __set_num_cpus_end__
# fmt: on


# fmt: off
# __memory_summary_start__
# each task requests 0.55 of the system memory when the memory threshold is 0.8.
allocate_bytes = get_additional_bytes_to_reach_memory_usage_pct(0.55)

first_actor = MemoryHogger.options(max_restarts=1, max_task_retries=1, name="first_actor", memory=allocate_bytes).remote()
second_actor = MemoryHogger.options(max_restarts=0, max_task_retries=0, name="second_actor", memory=allocate_bytes).remote()
# __memory_summary_end__
# fmt: on


# fmt: off
# __set_memory_start__
from math import ceil
import ray
from ray._private.utils import get_system_memory # do not use outside of this example as these are private methods.
from ray._private.utils import get_used_memory # do not use outside of this example as these are private methods.

# estimates the number of bytes to allocate to reach the desired memory usage percentage.
def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
    used = get_used_memory()
    total = get_system_memory()
    bytes_needed = int(total * pct) - used
    assert bytes_needed > 0, "memory usage is already above the target. Increase the target percentage."
    return bytes_needed

@ray.remote
class MemoryHogger:
    def __init__(self):
        self.allocations = []

    def allocate(self, bytes_to_allocate: float) -> None:
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(bytes_to_allocate / 8)
        self.allocations.append(new_list)

# each task requests 0.55 of the system memory when the memory threshold is 0.8.
allocate_bytes = get_additional_bytes_to_reach_memory_usage_pct(0.55)

first_actor = MemoryHogger.options(max_restarts=1, max_task_retries=1, name="first_actor", memory=allocate_bytes).remote()
second_actor = MemoryHogger.options(max_restarts=0, max_task_retries=0, name="second_actor", memory=allocate_bytes).remote()

first_actor_task = first_actor.allocate.remote(allocate_bytes)
second_actor_task = second_actor.allocate.remote(allocate_bytes)

ray.get(first_actor_task)
ray.kill(first_actor)
print("finished first actor")

ray.get(second_actor_task)
print("finished second actor")
# __set_memory_end__
# fmt: on
