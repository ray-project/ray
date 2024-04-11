import json
from time import sleep

from ray._private.test_utils import wait_for_condition
from ray.tests.conftest_docker import *  # noqa


sleep_task_scripts = """
import ray
ray.init(address="localhost:6379")
@ray.remote(max_retries=-1)
def f():
    import time
    time.sleep(10000)
ray.get([f.remote() for _ in range(2)])
"""

def test_network_task_submit(docker_cluster_with_network):
    head, worker, network = docker_cluster_with_network

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
    time.sleep(10)

    # kill the worker to simulate the spot shutdown
    worker.kill()
    print("Killed worker")
    time.sleep(10)

    # List node make sure the node is gone
    output = head.exec_run(cmd=f"ray list nodes --format json")
    assert output.exit_code == 0
    nodes_json = json.loads(output.output)
    print("nodes_json:", json.dumps(nodes_json, indent=2))
    found_dead_node = False
    for node in nodes_json:
      if node["state"] == "DEAD" and node["is_head_node"] == False:
        found_dead_node = True
        break
    assert found_dead_node

    print("found dead node")
    time.sleep(10)
    # list_task make sure all tasks are not running => working!
    output = head.exec_run(cmd=f"ray list tasks --format json")
    assert output.exit_code == 0
    tasks_json = json.loads(output.output)
    print("tasks_json:", json.dumps(tasks_json, indent=2))
    for task in tasks_json:
      assert task["state"] != "RUNNING"
  
    print("ensured tasks are not running")
