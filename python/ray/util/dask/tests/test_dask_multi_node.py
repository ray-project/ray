import sys

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

import ray
from ray.tests.conftest import *  # noqa: F403, F401
from ray.util.dask import enable_dask_on_ray


@pytest.fixture
def ray_enable_dask_on_ray():
    with enable_dask_on_ray():
        yield


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

    def get_node_id(row):
        return pd.Series(ray._private.worker.global_worker.node.unique_id)

    # Test annotations on compute.
    df = dd.from_pandas(
        pd.DataFrame(np.random.randint(0, 2, size=(2, 2)), columns=["age", "grade"]),
        npartitions=2,
    )
    c = df.apply(get_node_id, axis=1, meta={0: str})
    with dask.annotate(ray_remote_args=dict(num_gpus=1, resources={"pin": 0.01})):
        result = c.compute(optimize_graph=False)
    assert result[0].iloc[0] == pinned_node.unique_id

    # Test compute global Ray remote args.
    c = df.apply(get_node_id, axis=1, meta={0: str})
    result = c.compute(
        ray_remote_args={"resources": {"pin": 0.01}}, optimize_graph=False
    )
    assert result[0].iloc[0] == pinned_node.unique_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
