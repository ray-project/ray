# flake8: noqa

# __nested_start__
import ray


@ray.remote
def f():
    return 1


@ray.remote
def g():
    # Call f 4 times and return the resulting object refs.
    return [f.remote() for _ in range(4)]


@ray.remote
def h():
    # Call f 4 times, block until those 4 tasks finish,
    # retrieve the results, and return the values.
    return ray.get([f.remote() for _ in range(4)])


# __nested_end__


# __yield_start__
@ray.remote(num_cpus=1, num_gpus=1)
def g():
    return ray.get(f.remote())


# __yield_end__
