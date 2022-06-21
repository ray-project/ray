import sys
import unittest

import dask
import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

import ray
from ray.tests.conftest import *  # noqa: F403, F401
from ray.util.client.common import ClientObjectRef
from ray.util.dask import disable_dask_on_ray, enable_dask_on_ray, ray_dask_get
from ray.util.dask.callbacks import ProgressBarCallback


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_enable_dask_on_ray():
    with enable_dask_on_ray():
        yield


@unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
def test_ray_dask_basic(ray_start_1_cpu):
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


def test_ray_dask_resources(ray_start_cluster, ray_enable_dask_on_ray):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1, resources={"other_pin": 1})
    pinned_node = cluster.add_node(num_cpus=1, num_gpus=1, resources={"pin": 1})

    ray.init(address=cluster.address)

    def get_node_id():
        return ray._private.worker.global_worker.node.unique_id

    # Test annotations on collection.
    with dask.annotate(ray_remote_args=dict(num_cpus=1, resources={"pin": 0.01})):
        c = dask.delayed(get_node_id)()
    result = c.compute(optimize_graph=False)

    assert result == pinned_node.unique_id

    # Test annotations on compute.
    c = dask.delayed(get_node_id)()
    with dask.annotate(ray_remote_args=dict(num_gpus=1, resources={"pin": 0.01})):
        result = c.compute(optimize_graph=False)

    assert result == pinned_node.unique_id

    # Test compute global Ray remote args.
    c = dask.delayed(get_node_id)
    result = c().compute(ray_remote_args={"resources": {"pin": 0.01}})

    assert result == pinned_node.unique_id

    # Test annotations on collection override global resource.
    with dask.annotate(ray_remote_args=dict(resources={"pin": 0.01})):
        c = dask.delayed(get_node_id)()
    result = c.compute(
        ray_remote_args=dict(resources={"other_pin": 0.01}), optimize_graph=False
    )

    assert result == pinned_node.unique_id

    # Test top-level resources raises an error.
    with pytest.raises(ValueError):
        with dask.annotate(resources={"pin": 0.01}):
            c = dask.delayed(get_node_id)()
        result = c.compute(optimize_graph=False)
    with pytest.raises(ValueError):
        c = dask.delayed(get_node_id)
        result = c().compute(resources={"pin": 0.01})


@unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
def test_ray_dask_persist(ray_start_1_cpu):
    arr = da.ones(5) + 2
    result = arr.persist(scheduler=ray_dask_get)
    assert isinstance(
        next(iter(result.dask.values())), (ray.ObjectRef, ClientObjectRef)
    )


def test_sort_with_progress_bar(ray_start_1_cpu):
    npartitions = 10
    df = dd.from_pandas(
        pd.DataFrame(
            np.random.randint(0, 100, size=(100, 2)), columns=["age", "grade"]
        ),
        npartitions=npartitions,
    )
    # We set max_branch=npartitions in order to ensure that the task-based
    # shuffle happens in a single stage, which is required in order for our
    # optimization to work.
    sorted_with_pb = None
    sorted_without_pb = None
    with ProgressBarCallback():
        sorted_with_pb = df.set_index(
            ["age"], shuffle="tasks", max_branch=npartitions
        ).compute(scheduler=ray_dask_get, _ray_enable_progress_bar=True)
    sorted_without_pb = df.set_index(
        ["age"], shuffle="tasks", max_branch=npartitions
    ).compute(scheduler=ray_dask_get)
    assert sorted_with_pb.equals(sorted_without_pb)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
