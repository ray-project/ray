from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray


def setup_module():
    if not ray.worker.global_worker.connected:
        ray.init(num_cpus=1)

    # Finish initializing Ray. Otherwise available_resources() does not
    # reflect resource use of submitted tasks
    ray.get(cpu_task.remote(0))


@ray.remote(num_cpus=1)
def cpu_task(seconds):
    time.sleep(seconds)


class TestAvailableResources(object):
    timeout = 10

    def test_no_tasks(self):
        cluster_resources = ray.global_state.cluster_resources()
        available_resources = ray.global_state.available_resources()
        assert cluster_resources == available_resources

    def test_replenish_resources(self):
        cluster_resources = ray.global_state.cluster_resources()

        ray.get(cpu_task.remote(0))
        start = time.time()
        resources_reset = False

        while not resources_reset and time.time() - start < self.timeout:
            available_resources = ray.global_state.available_resources()
            resources_reset = (cluster_resources == available_resources)

        assert resources_reset

    def test_uses_resources(self):
        cluster_resources = ray.global_state.cluster_resources()
        task_id = cpu_task.remote(1)
        start = time.time()
        resource_used = False

        while not resource_used and time.time() - start < self.timeout:
            available_resources = ray.global_state.available_resources()
            resource_used = available_resources[
                "CPU"] == cluster_resources["CPU"] - 1

        assert resource_used

        ray.get(task_id)  # clean up to reset resources
