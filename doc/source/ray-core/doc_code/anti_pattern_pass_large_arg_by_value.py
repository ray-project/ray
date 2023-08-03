# __anti_pattern_start__
import ray
import numpy as np

ray.init()


@ray.remote
def func(large_arg, i):
    return len(large_arg) + i


large_arg = np.zeros(1024 * 1024)

# 10 copies of large_arg are stored in the object store.
outputs = ray.get([func.remote(large_arg, i) for i in range(10)])
# __anti_pattern_end__

# __better_approach_start__
# 1 copy of large_arg is stored in the object store.
large_arg_ref = ray.put(large_arg)
outputs = ray.get([func.remote(large_arg_ref, i) for i in range(10)])
# __better_approach_end__
