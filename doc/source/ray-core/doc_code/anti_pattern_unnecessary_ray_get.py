# __anti_pattern_start__
import ray
import numpy as np

ray.init()


@ray.remote
def generate_rollout():
    return np.ones((10000, 10000))


@ray.remote
def reduce(rollout):
    return np.sum(rollout)


# `ray.get()` downloads the result here.
rollout = ray.get(generate_rollout.remote())
# Now we have to reupload `rollout`
reduced = ray.get(reduce.remote(rollout))
# __anti_pattern_end__

assert reduced == 100000000

# __better_approach_start__
# Don't need ray.get here.
rollout_obj_ref = generate_rollout.remote()
# Rollout object is passed by reference.
reduced = ray.get(reduce.remote(rollout_obj_ref))
# __better_approach_end__

assert reduced == 100000000
