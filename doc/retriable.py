from math import ceil
import ray
from ray._private.utils import get_system_memory # do not use outside of the example as these are private methods.
from ray._private.utils import get_used_memory # do not use outside of the example as these are private methods.

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

    # def get_worker_id(self):
    #     return ray._private.worker.global_worker.core_worker.get_worker_id().hex()

    # def get_actor_id(self):
        # return ray._private.worker.global_worker.core_worker.get_actor_id().hex()

# @ray.remote
# def allocate_memory_to(pct: float) -> None:
#     chunks = []
#     bytes_to_allocate = get_additional_bytes_to_reach_memory_usage_pct(pct)

#     # each element occupies 8 bytes.
    # chunks.append([0] * bytes_to_allocate / 8)
    
# each task requests 0.75 of the system memory when the memory threshold is 0.8. One will suceed while the other fails.
first_actor = MemoryHogger.options(max_restarts=1, max_task_retries=1).remote()
second_actor = MemoryHogger.options(max_restarts=0, max_task_retries=0).remote()

allocate_bytes = get_additional_bytes_to_reach_memory_usage_pct(0.75)
      
first_actor_task = first_actor.allocate.remote(allocate_bytes)
second_actor_task = second_actor.allocate.remote(allocate_bytes)

# the first task will fail while the second task that is not-retriable will complete.
error_thrown = False
try:
    ray.get(first_actor_task)
except ray.exceptions.OutOfMemoryError:
    error_thrown = True
    print("first actor failed due to OutOfMemoryError, which is expected")
assert error_thrown == True

ray.get(second_actor_task)
