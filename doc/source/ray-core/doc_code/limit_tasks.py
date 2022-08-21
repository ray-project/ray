import numpy as np

import ray

ray.init()


# __defining_actor_start__
@ray.remote
class Actor:
    def heavy_compute(self, large_array):
        # taking a long time...
        return


# __defining_actor_end__


# __creating_actor_start__
actor = Actor.remote()
# __creating_actor_end__

# __executing_task_start__
result_refs = []
results = []
max_in_flight_tasks = 1000
for i in range(1_000_000):
    large_array = np.zeros(1_000_000)

    # Allow 1000 in flight calls
    # For example, if i = 5000, this call blocks until that
    # 4000 of the object_refs in result_refs are ready
    # and available.
    if len(result_refs) > max_in_flight_tasks:
        # update result_refs to only
        # track the remaining tasks.
        num_ready = len(result_refs) - max_in_flight_tasks
        newly_completed, result_refs = ray.wait(result_refs, num_returns=num_ready)
        for completed_ref in newly_completed:
            results.append(ray.get(completed_ref))

    result_refs.append(actor.heavy_compute.remote(large_array))

newly_completed, result_refs = ray.wait(result_refs, num_returns=len(result_refs))
for completed_ref in newly_completed:
    results.append(ray.get(completed_ref))
# __executing_task_end__
