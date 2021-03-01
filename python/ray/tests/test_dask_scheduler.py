import dask
import dask.array as da
import pytest

import ray
from ray.util.dask import ray_dask_get


def test_ray_dask_basic(ray_start_regular_shared):
    @ray.remote
    def stringify(x):
        return "The answer is {}".format(x)

    zero_id = ray.put(0)

    def add(x, y):
        # Can retrieve ray objects from inside Dask.
        zero = ray.get(zero_id)
        # Can call Ray methods from inside Dask.
        return ray.get(stringify.remote(x + y + zero))

    add = dask.delayed(add)

    @ray.remote
    def call_add():
        z = add(2, 4)
        # Can call Dask graphs from inside Ray.
        return z.compute(scheduler=ray_dask_get)

    ans = ray.get(call_add.remote())
    assert ans == "The answer is 6", ans


def test_ray_dask_persist(ray_start_regular_shared):
    arr = da.ones(5) + 2
    result = arr.persist(scheduler=ray_dask_get)
    assert isinstance(next(iter(result.dask.values())), ray.ObjectRef)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
