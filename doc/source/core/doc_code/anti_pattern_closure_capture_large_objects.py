# __anti_pattern_start__
import ray
import numpy as np

ray.init()

large_object = np.zeros(10 * 1024 * 1024)


@ray.remote
def f1():
    return len(large_object)  # large_object is serialized along with f1!


ray.get(f1.remote())
# __anti_pattern_end__

# __better_approach_1_start__
large_object_ref = ray.put(np.zeros(10 * 1024 * 1024))


@ray.remote
def f2(large_object):
    return len(large_object)


# Large object is passed through object store.
ray.get(f2.remote(large_object_ref))
# __better_approach_1_end__

# __better_approach_2_start__
large_object_creator = lambda: np.zeros(10 * 1024 * 1024)  # noqa E731


@ray.remote
def f3():
    large_object = (
        large_object_creator()
    )  # Lambda is small compared with the large object.
    return len(large_object)


ray.get(f3.remote())
# __better_approach_2_end__
