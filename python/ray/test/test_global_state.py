from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import time

import ray


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=1)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_replenish_resources(ray_start):
    cluster_resources = ray.global_state.cluster_resources()
    available_resources = ray.global_state.available_resources()
    assert cluster_resources == available_resources

    @ray.remote
    def cpu_task():
        pass

    ray.get(cpu_task.remote())
    start = time.time()
    resources_reset = False

    timeout = 10
    while not resources_reset and time.time() - start < timeout:
        available_resources = ray.global_state.available_resources()
        resources_reset = (cluster_resources == available_resources)

    assert resources_reset


def test_uses_resources(ray_start):
    cluster_resources = ray.global_state.cluster_resources()

    @ray.remote
    def cpu_task():
        time.sleep(1)

    cpu_task.remote()
    resource_used = False

    start = time.time()
    timeout = 10
    while not resource_used and time.time() - start < timeout:
        available_resources = ray.global_state.available_resources()
        resource_used = available_resources[
            "CPU"] == cluster_resources["CPU"] - 1

    assert resource_used
