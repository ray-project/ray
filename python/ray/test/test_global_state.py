from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import pytest
import subprocess
import ray

REDIS_PORT = 6543


@ray.remote(num_cpus=1)
def cpu_task(seconds):
    time.sleep(seconds)


def _get_raylet_pid(raylet_socket):
    output = subprocess.check_output("ps -a".split(" "))
    all_processes_split = output.decode("ascii").split("\n")
    search_term = "python/ray/core/src/ray/raylet/raylet {}".format(
        raylet_socket)
    return [
        x.strip().split(" ")[0] for x in all_processes_split
        if search_term in x
    ][0]


class TestAvailableResources(object):
    @classmethod
    def setup_class(cls):
        if not ray.worker.global_worker.connected:
            ray.init(num_cpus=1)

        # Finish initializing Ray. Otherwise available_resources() does not
        # reflect resource use of submitted tasks
        ray.get(cpu_task.remote(0))

    @classmethod
    def teardown_class(cls):
        ray.shutdown()

    def test_no_tasks(self):
        cluster_resources = ray.global_state.cluster_resources()
        available_resources = ray.global_state.cluster_resources()
        assert cluster_resources == available_resources

    @pytest.mark.timeout(10)
    def test_replenish_resources(self):
        cluster_resources = ray.global_state.cluster_resources()

        ray.get(cpu_task.remote(0))
        start = time.time()
        resources_reset = False

        while not resources_reset:
            resources_reset = (
                cluster_resources == ray.global_state.available_resources())

        assert resources_reset

    @pytest.mark.timeout(10)
    def test_uses_resources(self):
        cluster_resources = ray.global_state.cluster_resources()
        task_id = cpu_task.remote(1)
        start = time.time()
        resource_used = False

        while not resource_used:
            available_resources = ray.global_state.available_resources()
            resource_used = available_resources[
                "CPU"] == cluster_resources["CPU"] - 1

        assert resource_used

        ray.get(task_id)  # clean up to reset resources


class TestMultiNodeState(object):
    @classmethod
    def setup_class(cls):
        subprocess.check_call("ray start --head --redis-port "
                              "{port} --num-cpus 1 --use-raylet".format(
                                  port=REDIS_PORT).split(" "))
        ray.init(redis_address="localhost:{}".format(REDIS_PORT))

    @classmethod
    def teardown_class(cls):
        subprocess.check_call("ray stop".split(" "))
        ray.shutdown()

    @pytest.mark.timeout(20)
    def test_add_remove_client(self):
        """Tests client table is correct after node removal."""
        clients = ray.global_state.client_table()
        assert len(clients) == 1
        head_raylet_pid = _get_raylet_pid(clients[0]["RayletSocketName"])

        subprocess.check_call(
            "ray start --redis-address localhost:{port} "
            "--num-cpus 1 --use-raylet".format(port=REDIS_PORT).split(" "))

        clients = ray.global_state.client_table()
        assert len(clients) == 2
        assert sum(cl["Resources"].get("CPU") for cl in clients) == 2

        worker_raylet_pid = _get_raylet_pid(clients[1]["RayletSocketName"])
        assert head_raylet_pid != worker_raylet_pid

        subprocess.check_output(["kill", str(worker_raylet_pid)])

        # wait for heartbeat
        while all(cl_entries["IsInsertion"] for cl_entries in clients):
            clients = ray.global_state.client_table()
            time.sleep(1)
        assert sum(cl["Resources"].get("CPU", 0) for cl in clients) == 1
