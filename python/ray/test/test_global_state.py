from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray


def setup_module():
    if not ray.worker.global_worker.connected:
        ray.init()

        # Finish initializing Ray. Otherwise available_resources() does not
        # reflect resource use of submitted tasks
        ray.get(noop.remote())


@ray.remote(num_cpus=1)
def cpu_task():
    time.sleep(0.5)


@ray.remote
def noop():
    return


class TestAvailableResources(object):
    def test_no_tasks(self):
        cluster_resources = ray.global_state.cluster_resources()
        available_resources = ray.global_state.cluster_resources()
        assert cluster_resources == available_resources

    def test_cpu_task(self):
        cluster_resources = ray.global_state.cluster_resources()

        # Check the resource use of a launched task
        task_id = cpu_task.remote()
        available_resources = ray.global_state.available_resources()
        assert cluster_resources["CPU"] == available_resources["CPU"] + 1

        # Check that the resource is replenished
        ray.get(task_id)
        available_resources = ray.global_state.cluster_resources()
        assert cluster_resources == available_resources
