import ray
import numpy as np


@ray.remote
def task_with_small_return_value_bad():
    small_return_value = 1
    # The value will be stored in the object store
    # and the reference is returned to the caller.
    small_return_value_ref = ray.put(small_return_value)
    return small_return_value_ref


@ray.remote
def task_with_small_return_value_good():
    small_return_value = 1
    # Ray will return the value inline to the caller
    # which is faster than the previous approach.
    return small_return_value


assert ray.get(ray.get(task_with_small_return_value_bad.remote())) == ray.get(
    task_with_small_return_value_good.remote()
)


@ray.remote
def task_with_large_return_value_bad():
    large_return_value = np.zeros(10 * 1024 * 1024)
    large_return_value_ref = ray.put(large_return_value)
    return large_return_value_ref


@ray.remote
def task_with_large_return_value_good():
    # Both approaches will store the large array to the object store
    # but this is better since it's faster and more fault tolerant.
    large_return_value = np.zeros(10 * 1024 * 1024)
    return large_return_value


assert np.array_equal(
    ray.get(ray.get(task_with_large_return_value_bad.remote())),
    ray.get(task_with_large_return_value_good.remote()),
)


@ray.remote
class Actor:
    @ray.method(num_returns=1)
    def task_with_static_returns_bad(self):
        return_value_1_ref = ray.put(1)
        return_value_2_ref = ray.put(2)
        return (return_value_1_ref, return_value_2_ref)

    @ray.method(num_returns=2)
    def task_with_static_returns_good(self):
        # This is faster and more fault tolerant.
        return_value_1 = 1
        return_value_2 = 2
        return (return_value_1, return_value_2)


actor = Actor.remote()
assert ray.get(ray.get(actor.task_with_static_returns_bad.remote())[0]) == ray.get(
    actor.task_with_static_returns_good.remote()[0]
)
assert ray.get(ray.get(actor.task_with_static_returns_bad.remote())[1]) == ray.get(
    actor.task_with_static_returns_good.remote()[1]
)


@ray.remote(num_returns=1)
def task_with_dynamic_returns_bad(n):
    return_value_refs = []
    for i in range(n):
        return_value_refs.append(ray.put(np.zeros(i * 1024 * 1024)))
    return return_value_refs


# Note: currently actor tasks don't support dynamic returns
# so the previous approach needs to be used if you want to
# return dynamic number of objects from an actor task.
@ray.remote(num_returns="dynamic")
def task_with_dynamic_returns_good(n):
    for i in range(n):
        yield np.zeros(i * 1024 * 1024)


assert np.array_equal(
    ray.get(ray.get(task_with_dynamic_returns_bad.remote(2))[0]),
    ray.get(next(iter(ray.get(task_with_dynamic_returns_good.remote(2))))),
)
