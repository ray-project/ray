import json
from time import sleep
import pytest
from ray._private.test_utils import wait_for_condition
from ray.tests.conftest_docker import gen_head_node, gen_worker_node, gcs_network


sleep_task_scripts = """
import ray
ray.init(address="localhost:6379")
@ray.remote(max_retries=-1)
def f():
    import time
    time.sleep(10000)
ray.get([f.remote() for _ in range(2)])
"""

head_node = gen_head_node({
    "RAY_grpc_keepalive_time_ms": "1000",
    "RAY_grpc_client_keepalive_time_ms": "1000",
    "RAY_health_check_initial_delay_ms": "1000",
    "RAY_health_check_period_ms": "1000",
    "RAY_health_check_timeout_ms": "1000",
    "RAY_health_check_failure_threshold": "2",
})

worker_node = gen_worker_node({
    "RAY_grpc_keepalive_time_ms": "1000",
    "RAY_grpc_client_keepalive_time_ms": "1000",
    "RAY_health_check_initial_delay_ms": "1000",
    "RAY_health_check_period_ms": "1000",
    "RAY_health_check_timeout_ms": "1000",
    "RAY_health_check_failure_threshold": "2",
})


def test_network_task_submit(head_node, worker_node, gcs_network):
    head, worker, network = head_node, worker_node, gcs_network

    # https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.Container.exec_run
    head.exec_run(cmd=f"python -c '{sleep_task_scripts}'", detach=True)

    time.sleep(5)

    # list_task make sure all tasks are running
    output = head.exec_run(cmd=f"ray list tasks --format json")
    assert output.exit_code == 0
    tasks_json = json.loads(output.output)
    print("tasks_json:", json.dumps(tasks_json, indent=2))
    num_running_tasks = sum([1 for task in tasks_json if task["state"] == "RUNNING"])
    assert num_running_tasks == 2

    # partition the network between head and worker
    # https://docker-py.readthedocs.io/en/stable/networks.html#docker.models.networks.Network.disconnect
    network.disconnect(worker.name)
    print("Disconnected network")
    time.sleep(2)
    # kill the worker to simulate the spot shutdown
    worker.kill()
    print("Killed worker")
    time.sleep(2)

    def check_dead_node():
        output = head.exec_run(cmd=f"ray list nodes --format json")
        if output.exit_code == 0:
            nodes_json = json.loads(output.output)
            print("nodes_json:", json.dumps(nodes_json, indent=2))
            for node in nodes_json:
              if node["state"] == "DEAD" and node["is_head_node"] == False:
                  return True
        return False

    wait_for_condition(check_dead_node)
    print("found dead node")

    # list_task make sure all tasks are not running => working!
    def check_task_not_running():
        output = head.exec_run(cmd=f"ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            return all([task["state"] != "RUNNING" for task in tasks_json])
        return False
    wait_for_condition(check_task_not_running)

    network.connect(worker.name)
    worker.restart()

    def check_task_running():
        output = head.exec_run(cmd=f"ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            return all([task["state"] == "RUNNING" for task in tasks_json])
        return False
    wait_for_condition(check_task_running)
