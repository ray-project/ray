# __anti_pattern_start__
import ray
import numpy as np

ray.init()


def process_results(results):
    # custom process logic
    pass


@ray.remote
def return_big_object():
    return np.zeros(1024 * 10)


NUM_TASKS = 1000

object_refs = [return_big_object.remote() for _ in range(NUM_TASKS)]
# This will fail with heap out-of-memory
# or object store out-of-space if NUM_TASKS is large enough.
results = ray.get(object_refs)
process_results(results)
# __anti_pattern_end__

# __better_approach_start__
BATCH_SIZE = 100

while object_refs:
    # Process results in the finish order instead of the submission order.
    ready_object_refs, object_refs = ray.wait(object_refs, num_returns=BATCH_SIZE)
    # The node only needs enough space to store
    # a batch of objects instead of all objects.
    results = ray.get(ready_object_refs)
    process_results(results)
# __better_approach_end__
