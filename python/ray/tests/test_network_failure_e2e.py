import json
import sys
import threading
from time import sleep

import pytest

from ray._common.test_utils import wait_for_condition
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import gen_head_node, gen_worker_node

SLEEP_TASK_SCRIPTS = """
import ray
ray.init()
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
    envs={
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "1000",
        "RAY_health_check_failure_threshold": "2",
    },
    num_cpus=8,
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

    # Previously grpc client will only send 2 ping frames when there is no
    # data/header frame to be sent.
    # keepalive interval is 1s. So after 3s it wouldn't send anything and
    # failed the test previously.
    sleep(3)

    # partition the network between head and worker
    # https://docker-py.readthedocs.io/en/stable/networks.html#docker.models.networks.Network.disconnect
    network.disconnect(worker.name)
    print("Disconnected network")

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
    print("observed node died")

    # Previously under network partition, the tasks would stay in RUNNING state
    # and hanging forever.
    # We write this test to check that.
    def check_task_not_running():
        output = head.exec_run(cmd="ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            return all([task["state"] != "RUNNING" for task in tasks_json])
        return False

    # we set num_cpus=0 for head node.
    # which ensures no task was scheduled on the head node.
    wait_for_condition(check_task_not_running)

    # After the fix, we should observe that the tasks are not running.
    # `ray list tasks` would show two FAILED and
    # two PENDING_NODE_ASSIGNMENT states.

    def check_task_pending(n=0):
        output = head.exec_run(cmd="ray list tasks --format json")
        if output.exit_code == 0:
            tasks_json = json.loads(output.output)
            print("tasks_json:", json.dumps(tasks_json, indent=2))
            return n == sum(
                [task["state"] == "PENDING_NODE_ASSIGNMENT" for task in tasks_json]
            )
        return False

    wait_for_condition(lambda: check_task_pending(2))


head2 = gen_head_node(
    {
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "100000",
        "RAY_health_check_failure_threshold": "20",
    }
)

worker2 = gen_worker_node(
    envs={
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "100000",
        "RAY_health_check_failure_threshold": "20",
    },
    num_cpus=2,
)


def test_transient_network_error(head2, worker2, gcs_network):
    # Test to make sure the head node and worker node
    # connection can be recovered from transient network error.
    network = gcs_network

    check_two_nodes = """
import ray
from ray._common.test_utils import wait_for_condition

ray.init()
wait_for_condition(lambda: len(ray.nodes()) == 2)
"""
    result = head2.exec_run(cmd=f"python -c '{check_two_nodes}'")
    assert result.exit_code == 0, result.output.decode("utf-8")

    # Simulate transient network error
    worker_ip = worker2._container.attrs["NetworkSettings"]["Networks"][network.name][
        "IPAddress"
    ]
    network.disconnect(worker2.name, force=True)
    sleep(2)
    network.connect(worker2.name, ipv4_address=worker_ip)

    # Make sure the connection is recovered by scheduling
    # an actor.
    check_actor_scheduling = """
import ray
from ray._common.test_utils import wait_for_condition

ray.init()

@ray.remote(num_cpus=1)
class Actor:
    def ping(self):
        return 1

actor = Actor.remote()
ray.get(actor.ping.remote())
wait_for_condition(lambda: ray.available_resources()["CPU"] == 1.0)
"""
    result = head2.exec_run(cmd=f"python -c '{check_actor_scheduling}'")
    assert result.exit_code == 0, result.output.decode("utf-8")


head3 = gen_head_node(
    {
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "100000",
        "RAY_health_check_failure_threshold": "20",
    }
)

worker3 = gen_worker_node(
    envs={
        "RAY_grpc_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_time_ms": "1000",
        "RAY_grpc_client_keepalive_timeout_ms": "1000",
        "RAY_health_check_initial_delay_ms": "1000",
        "RAY_health_check_period_ms": "1000",
        "RAY_health_check_timeout_ms": "100000",
        "RAY_health_check_failure_threshold": "20",
    },
    num_cpus=2,
)


def test_async_actor_task_retry(head3, worker3, gcs_network):
    # Test that if transient network error happens
    # after an async actor task is submitted and being executed,
    # a secon attempt will be submitted and executed after the
    # first attempt finishes.
    network = gcs_network

    driver = """
import asyncio
import ray
from ray.util.state import list_tasks

ray.init(namespace="test")

@ray.remote(num_cpus=0.1, name="counter", lifetime="detached")
class Counter:
  def __init__(self):
    self.count = 0

  def inc(self):
    self.count = self.count + 1
    return self.count

  @ray.method(max_task_retries=-1)
  def get(self):
    return self.count

@ray.remote(num_cpus=0.1, max_task_retries=-1)
class AsyncActor:
  def __init__(self, counter):
    self.counter = counter

  async def run(self):
    count = await self.counter.get.remote()
    if count == 0:
      # first attempt
      await self.counter.inc.remote()
      while len(list_tasks(
            filters=[("name", "=", "AsyncActor.run")])) < 2:
        # wait for second attempt to be made
        await asyncio.sleep(1)
      # wait until the second attempt reaches the actor
      await asyncio.sleep(2)
      await self.counter.inc.remote()
      return "first"
    else:
      # second attempt
      # make sure second attempt only runs
      # after first attempt finishes
      assert count == 2
      return "second"

counter = Counter.remote()
async_actor = AsyncActor.remote(counter)
assert ray.get(async_actor.run.remote()) == "second"
"""

    check_async_actor_run_is_called = """
import ray
from ray._common.test_utils import wait_for_condition
ray.init(namespace="test")

wait_for_condition(lambda: ray.get_actor("counter") is not None)
counter = ray.get_actor("counter")
wait_for_condition(lambda: ray.get(counter.get.remote()) == 1)
"""

    def inject_transient_network_failure():
        try:
            result = head3.exec_run(
                cmd=f"python -c '{check_async_actor_run_is_called}'"
            )
            assert result.exit_code == 0, result.output.decode("utf-8")

            worker_ip = worker3._container.attrs["NetworkSettings"]["Networks"][
                network.name
            ]["IPAddress"]
            network.disconnect(worker3.name, force=True)
            sleep(2)
            network.connect(worker3.name, ipv4_address=worker_ip)
        except Exception as e:
            print(f"Network failure injection failed {e}")

    t = threading.Thread(target=inject_transient_network_failure, daemon=True)
    t.start()

    result = head3.exec_run(
        cmd=f"python -c '{driver}'",
    )
    assert result.exit_code == 0, result.output.decode("utf-8")


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
