import os
import sys
import unittest.mock
import signal
import subprocess

import pytest

import ray
import ray._private.services
from ray.client_builder import ClientContext
from ray.cluster_utils import Cluster
from ray.util.client.common import ClientObjectRef
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.util.client.worker import Worker
from ray._private.test_utils import wait_for_condition, enable_external_redis
from ray.util.virtual_cluster import (
    VirtualCluster,
    virtual_cluster,
    remove_virtual_cluster,
)


def assert_is_superset(big, small):
    assert all(big.get(k, 0) == v for k, v in small.items()), f"{big=}, {small=}"


@pytest.mark.parametrize(
    "ray_start_cluster",
    [{"num_nodes": 3, "resources": {"my_resource": 5}}],
    indirect=True,
)
def test_virtual_cluster_basic(ray_start_cluster):
    """
    Basic test.
    Cluster: 3 nodes, each with 5 my_resource and 1 CPU.
    VC: 5 bundles, each with 1 my_resource and 0.1 CPU.
    Can allocate resources, can run tasks. Can release resources after exit.
    """
    cluster = ray_start_cluster

    assert_is_superset(ray.available_resources(), {"my_resource": 15, "CPU": 3})

    vc = virtual_cluster([{"my_resource": 1, "CPU": 0.1}] * 5)
    ray.get(vc.ready())

    # resources occupied by the vc.
    assert_is_superset(ray.available_resources(), {"my_resource": 10, "CPU": 2.5})

    with vc:
        # resources within VC
        assert_is_superset(ray.available_resources(), {"my_resource": 5, "CPU": 0.5})

        @ray.remote(num_cpus=0.1, resources={"my_resource": 1})
        def f():
            return 1

        tasks = [f.remote() for _ in range(5)]
        assert sum(ray.get(tasks)) == 5

    # Leaving VC but the VC is still there...
    assert_is_superset(ray.available_resources(), {"my_resource": 10, "CPU": 2.5})

    remove_virtual_cluster(vc.id.hex())
    # TODO: no way to wait for removal of VC, so we can't check the resources.


if __name__ == "__main__":
    import sys

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
