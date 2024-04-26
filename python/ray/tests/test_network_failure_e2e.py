import sys
import json

from time import sleep
import pytest
from ray._private.test_utils import wait_for_condition
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import gen_head_node, gen_worker_node


SLEEP_TASK_SCRIPTS = """
import ray
ray.init(address="localhost:6379")
@ray.remote(max_retries=-1)
def f():
    import time
    time.sleep(10000)
ray.get([f.remote() for _ in range(2)])
"""

head = gen_head_node(
    {
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "1000",
        "RAY_health_check_failure_threshold": "2",
    }
)

worker = gen_worker_node(
    {
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "1000",
        "RAY_health_check_failure_threshold": "2",
    }
)


def test_network_task_submit(head, worker, gcs_network):
    network = gcs_network
    # https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.Container.exec_run
    head.exec_run(
        cmd=f"python -c '{SLEEP_TASK_SCRIPTS}'",
        detach=True,
        environment=[
            "RAY_grpc_client_keepalive_time_ms=1000",
            "RAY_grpc_client_keepalive_timeout_ms=1000",
        ],
    )
    sleep(3)

    def check_task_running(n=None):
        output = head.exec_run(cmd="ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            if n is not None and n != len(tasks_json):
                return False
            return all([task["state"] == "RUNNING" for task in tasks_json])
        return False

    # list_task make sure all tasks are running
    wait_for_condition(lambda: check_task_running(2))

    # partition the network between head and worker
    # https://docker-py.readthedocs.io/en/stable/networks.html#docker.models.networks.Network.disconnect
    network.disconnect(worker.name)
    print("Disconnected network")
    sleep(2)
    # kill the worker to simulate the spot shutdown
    worker.kill()
    print("Killed worker")
    sleep(2)

    def check_dead_node():
        output = head.exec_run(cmd="ray list nodes --format json")
        if output.exit_code == 0:
            nodes_json = json.loads(output.output)
            print("nodes_json:", json.dumps(nodes_json, indent=2))
            for node in nodes_json:
                if node["state"] == "DEAD" and not node["is_head_node"]:
                    return True
        return False

    wait_for_condition(check_dead_node)
    print("found dead node")

    def check_task_not_running():
        output = head.exec_run(cmd="ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            return all([task["state"] != "RUNNING" for task in tasks_json])
        return False

    def check_task_state(n=0, state="RUNNING"):
        output = head.exec_run(cmd="ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            return n == sum([task["state"] == state for task in tasks_json])
        return False

    # we set num_cpus=0 for head node.
    # which ensures no task was scheduled on the head node.
    # This is important so the test doesn't just spuriously hang.

    # list_task make sure all tasks are not running => working!
    wait_for_condition(check_task_not_running)
    print("tasks are not running")

    # list_task make sure all tasks are pending node assignment => working!
    wait_for_condition(lambda: check_task_state(2, "PENDING_NODE_ASSIGNMENT"))
    print("tasks are pending node assignment")


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
