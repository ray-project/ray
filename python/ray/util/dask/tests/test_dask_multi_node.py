import sys

import dask
import pytest

import ray
from ray.tests.conftest import *  # noqa: F403, F401
from ray.util.dask import enable_dask_on_ray

pytestmark = pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="Skip dask tests for Python version 3.12+"
)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
