import sys
import unittest

import dask
import dask.array as da
import dask.dataframe as dd
import pytest
import numpy as np
import pandas as pd

import ray
from ray.util.client.common import ClientObjectRef
from ray.util.dask.callbacks import ProgressBarCallback


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
def test_ray_dask_basic(ray_start_1_cpu):
    from ray.util.dask import ray_dask_get, enable_dask_on_ray, \
        disable_dask_on_ray

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

    expected = "The answer is 6"
    # Test with explicit scheduler argument.
    assert add(2, 4).compute(scheduler=ray_dask_get) == expected

    # Test with config setter.
    enable_dask_on_ray()
    assert add(2, 4).compute() == expected
    disable_dask_on_ray()

    # Test with config setter as context manager.
    with enable_dask_on_ray():
        assert add(2, 4).compute() == expected

    # Test within Ray task.

    @ray.remote
    def call_add():
        z = add(2, 4)
        with ProgressBarCallback():
            r = z.compute(scheduler=ray_dask_get)
        return r

    ans = ray.get(call_add.remote())
    assert ans == "The answer is 6", ans


@unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
def test_ray_dask_persist(ray_start_1_cpu):
    from ray.util.dask import ray_dask_get
    arr = da.ones(5) + 2
    result = arr.persist(scheduler=ray_dask_get)
    assert isinstance(
        next(iter(result.dask.values())), (ray.ObjectRef, ClientObjectRef))


def test_sort_with_progress_bar(ray_start_1_cpu):
    from ray.util.dask import ray_dask_get
    npartitions = 10
    df = dd.from_pandas(
        pd.DataFrame(
            np.random.randint(0, 100, size=(100, 2)), columns=["age",
                                                               "grade"]),
        npartitions=npartitions)
    # We set max_branch=npartitions in order to ensure that the task-based
    # shuffle happens in a single stage, which is required in order for our
    # optimization to work.
    sorted_with_pb = None
    sorted_without_pb = None
    with ProgressBarCallback():
        sorted_with_pb = df.set_index(
            ["age"], shuffle="tasks", max_branch=npartitions).compute(
                scheduler=ray_dask_get, _ray_enable_progress_bar=True)
    sorted_without_pb = df.set_index(
        ["age"], shuffle="tasks",
        max_branch=npartitions).compute(scheduler=ray_dask_get)
    assert sorted_with_pb.equals(sorted_without_pb)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
