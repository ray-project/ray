from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import pytest
try:
    import pytest_timeout
except ModuleNotFoundError:
    pytest_timeout = None
import time

import ray
from ray.test.cluster_utils import Cluster


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=1)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def cluster_start():
    # Start the Ray processes.
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "resources": dict(CPU=1),
            "_internal_config": json.dumps({
                "num_heartbeats_timeout": 10
            })
        })
    yield cluster
    ray.shutdown()
    cluster.shutdown()


# TODO(rliaw): The proper way to do this is to have the pytest config setup.
@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(10)
def test_replenish_resources(ray_start):
    cluster_resources = ray.global_state.cluster_resources()
    available_resources = ray.global_state.available_resources()
    assert cluster_resources == available_resources

    @ray.remote
    def cpu_task():
        pass

    ray.get(cpu_task.remote())
    resources_reset = False

    while not resources_reset:
        available_resources = ray.global_state.available_resources()
        resources_reset = (cluster_resources == available_resources)
    assert resources_reset


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(10)
def test_uses_resources(ray_start):
    cluster_resources = ray.global_state.cluster_resources()

    @ray.remote
    def cpu_task():
        time.sleep(1)

    cpu_task.remote()
    resource_used = False

    while not resource_used:
        available_resources = ray.global_state.available_resources()
        resource_used = available_resources[
            "CPU"] == cluster_resources["CPU"] - 1

    assert resource_used


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(20)
def test_add_remove_cluster_resources(cluster_start):
    """Tests that Global State API is consistent with actual cluster."""
    cluster = cluster_start
    assert ray.global_state.cluster_resources()["CPU"] == 1
    nodes = []
    nodes += [cluster.add_node(resources=dict(CPU=1))]
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 2

    cluster.remove_node(nodes.pop())
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1

    for i in range(5):
        nodes += [cluster.add_node(resources=dict(CPU=1))]
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 6
