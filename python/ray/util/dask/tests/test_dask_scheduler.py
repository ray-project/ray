import pytest
import sys
import unittest

import dask
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd

import ray
from ray.util.client.common import ClientObjectRef
from ray.util.dask.callbacks import ProgressBarCallback
from ray.tests.conftest import *  # noqa


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


@unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
def test_spread_scheduling(ray_start_cluster):
    # Make sure the default scheduling for dask on ray is spread.
    from ray.util.dask import ray_dask_get
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address="auto")
    cluster.add_node(num_cpus=4)

    def set_aggregate(x, y):
        return {x, y}

    def get_ip():
        import time
        time.sleep(3)
        return ray.get_runtime_context().node_id.hex()

    get_ip = dask.delayed(get_ip)
    set_aggregate = dask.delayed(set_aggregate)

    # Schedule 3 tasks at the same time. If the
    # scheduler policy is spread, it should be spread to 2 nodes.
    node_ids = set_aggregate(get_ip(),
                             get_ip()).compute(scheduler=ray_dask_get)
    print(node_ids)
    assert len(node_ids) == 2


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
