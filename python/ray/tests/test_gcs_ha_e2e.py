import pytest
import sys
import threading
from time import sleep
from ray._private.test_utils import wait_for_condition
from pytest_docker_tools import container, fetch, network
from pytest_docker_tools import wrappers
from http.client import HTTPConnection


class Container(wrappers.Container):
    def ready(self):
        self._container.reload()
        if self.status == "exited":
            from pytest_docker_tools.exceptions import ContainerFailed

            raise ContainerFailed(
                self,
                f"Container {self.name} has already exited before "
                "we noticed it was ready",
            )

        if self.status != "running":
            return False

        networks = self._container.attrs["NetworkSettings"]["Networks"]
        for (_, n) in networks.items():
            if not n["IPAddress"]:
                return False

        if "Ray runtime started" in super().logs():
            return True
        return False

    def client(self):
        port = self.ports["8000/tcp"][0]
        return HTTPConnection(f"localhost:{port}")


gcs_network = network(driver="bridge")

redis_image = fetch(repository="redis:latest")

redis = container(
    image="{redis_image.id}",
    network="{gcs_network.name}",
    command=(
        "redis-server --save 60 1 --loglevel" " warning --requirepass 5241590000000000"
    ),
)

def make_head_node(num_cpus=0, envs=None):
    if envs is None:
        envs = {}
    envs["RAY_REDIS_ADDRESS"] = "{redis.ips.primary}:6379"

    return container(
        image="ray_ci:v1",
        name="gcs",
        network="{gcs_network.name}",
        command=[
            "ray",
            "start",
            "--head",
            "--block",
            "--num-cpus",
            f"{num_cpus}",
            # Fix the port of raylet to make sure raylet restarts at the same
            # ip:port is treated as a different raylet.
            "--node-manager-port",
            "9379",
        ],
        environment=envs,
        wrapper_class=Container,
        ports={
            "8000/tcp": None,
        },
    )


head_node = make_head_node(num_cpus=0)

def make_worker_node():
    return container(
        image="ray_ci:v1",
        network="{gcs_network.name}",
        command=[
            "ray",
            "start",
            "--address",
            "gcs:6379",
            "--block",
            # Fix the port of raylet to make sure raylet restarts at the same
            # ip:port is treated as a different raylet.
            "--node-manager-port",
            "9379",
        ],
        environment={"RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379"},
        wrapper_class=Container,
        ports={
            "8000/tcp": None,
        },
    )

worker_node = make_worker_node()
worker_node_2 = make_worker_node()

@pytest.fixture
def docker_cluster(head_node, worker_node):
    yield (head_node, worker_node)


scripts = """
import ray
import json
from fastapi import FastAPI
app = FastAPI()
from ray import serve
ray.init(address="auto", namespace="g")

@serve.deployment(name="Counter", route_prefix="/api", version="v1")
@serve.ingress(app)
class Counter:
    def __init__(self):
        self.count = 0

    @app.get("/")
    def get(self):
        return {{"count": self.count}}

    @app.get("/incr")
    def incr(self):
        self.count += 1
        return {{"count": self.count}}

    @app.get("/decr")
    def decr(self):
        self.count -= 1
        return {{"count": self.count}}

    @app.get("/pid")
    def pid(self):
        import os
        return {{"pid": os.getpid()}}

serve.start(detached=True, dedicated_cpu=True)

Counter.options(num_replicas={num_replicas}).deploy()
"""

check_script = """
import requests
import json
if {num_replicas} == 1:
    b = json.loads(requests.get("http://127.0.0.1:8000/api/").text)["count"]
    for i in range(5):
        response = requests.get("http://127.0.0.1:8000/api/incr")
        assert json.loads(response.text) == {{"count": i + b + 1}}

pids = {{
    json.loads(requests.get("http://127.0.0.1:8000/api/pid").text)["pid"]
    for _ in range(5)
}}

print(pids)
assert len(pids) == {num_replicas}
"""


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_server_basic(docker_cluster):
    # This test covers the basic cases for gcs ha (serve ha)
    # - It starts the serve on worker nodes.
    # - Check the deployment is OK
    # - Stop headnode
    # - Check the serve app is running healthy
    # - Start a reconfig (2 replicas) and it'll hang
    # - Start head node. The script will continue once GCS is back
    # - Make sure two replicas are there

    # TODO(iycheng): Update serve to better integrate with GCS HA:
    #   - Make sure no task can run in the raylet where GCS is deployed.

    head, worker = docker_cluster
    output = worker.exec_run(cmd=f"python -c '{scripts.format(num_replicas=1)}'")
    assert output.exit_code == 0
    assert b"Adding 1 replica to deployment 'Counter'." in output.output
    # somehow this is not working and the port is not exposed to the host.
    # worker_cli = worker.client()
    # print(worker_cli.request("GET", "/api/incr"))

    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=1)}'")

    assert output.exit_code == 0

    # Kill the head node
    head.kill()

    # Make sure serve is still working
    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=1)}'")
    assert output.exit_code == 0

    # Script is running on another thread so that it won't block the main thread.
    def reconfig():
        worker.exec_run(cmd=f"python -c '{scripts.format(num_replicas=2)}'")

    t = threading.Thread(target=reconfig)
    t.start()

    # make sure the script started
    sleep(5)

    # serve reconfig should continue once GCS is back
    head.restart()

    t.join()

    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=2)}'")
    assert output.exit_code == 0


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_nodes_liveness(docker_cluster):
    get_nodes_script = """
import ray
ray.init("auto")
print(sum([1 if n["Alive"] else 0 for n in ray.nodes()]))
"""
    head, worker = docker_cluster

    def check_alive(n):
        output = worker.exec_run(cmd=f"python -c '{get_nodes_script}'")
        assert output.exit_code == 0
        text = output.output.decode().strip().split("\n")[-1]
        print("Alive nodes: ", text)
        return n == int(text)

    # Make sure two nodes are alive
    wait_for_condition(check_alive, n=2)
    print("head killed")
    head.kill()

    sleep(2)

    head.restart()
    # When GCS restarts, a new raylet is added
    # and the old dead raylet is going to take a while to be marked dead.
    # So there should be 3 alive nodes
    wait_for_condition(check_alive, timeout=10, n=3)
    # Later, GCS detect the old raylet dead and the alive nodes will be 2
    wait_for_condition(check_alive, timeout=30, n=2)

head_node_1cpu = make_head_node(num_cpus=1, envs={"RAY_gcs_server_request_timeout_seconds": "3"})

@pytest.fixture
def docker_cluster_1cpu(head_node_1cpu, worker_node):
    yield (head_node_1cpu, worker_node)

@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_deadnode_actor_call(docker_cluster_1cpu):
    # This test is to test the behavior when the head node is down for actors:
    #    The actor call will hang forever until the GCS is back and broadcast
    #    the death of the actor.
    #    If max_task_retries=-1, client will retry it once actor is recreated.
    #    Otherwise, exception will be thrown.

    head, worker = docker_cluster_1cpu

    create_actor_script = """
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init("auto", namespace="n")

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Actor:
    def hello(self):
        return "world"

strategy = NodeAffinitySchedulingStrategy(
    node_id=ray.get_runtime_context().node_id,
    soft=True)

a = Actor.options(
        name="hello",
        lifetime="detached",
        scheduling_strategy=strategy).remote()

print(ray.get(a.hello.remote()))
"""
    output = head.exec_run(cmd=f"python -c '{create_actor_script}'")
    assert output.exit_code == 0

    def kill_head():
        # sleep 3 seconds and then kill head
        sleep(3)
        head.kill()
        print("head node killed")

    t = threading.Thread(target=kill_head)

    fetch_from_actor_script = """
import ray
ray.init("auto", namespace="n")
a = ray.get_actor("hello")

import time

assert ray.get(a.hello.remote()) == "world"

for i in range(10):
    assert ray.get(a.hello.remote()) == "world"
    print("iter=", i)
    time.sleep(1)
"""

    def fetch_data():
        nonlocal output
        output = worker.exec_run(cmd=f"python -c '{fetch_from_actor_script}'")

    f = threading.Thread(target=fetch_data)

    t.start()
    f.start()

    t.join()
    f.join(timeout=10)

    assert f.is_alive()

    head.restart()

    f.join()
    assert not f.is_alive()
    assert output.exit_code == 0


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_ray_deadnode_object_fetch(docker_cluster_1cpu):
    # This test is to verify even the owner died, as long as it's going to restart,
    # we shall still be able to fetch the object it created if the object has copy
    # some place.
    head, worker = docker_cluster_1cpu
    create_actor_script = """
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init("auto", namespace="n")

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Actor:
    def hello(self):
        return ray.put("world")

strategy = NodeAffinitySchedulingStrategy(
    node_id=ray.get_runtime_context().node_id,
    soft=True)

a = Actor.options(
        name="hello",
        lifetime="detached",
        scheduling_strategy=strategy).remote()

print(ray.get(ray.get(a.hello.remote())))
"""
    output = head.exec_run(cmd=f"python -c '{create_actor_script}'")
    assert output.exit_code == 0

    create_cache_script = """
import ray
import time
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init("auto", namespace="n")

@ray.remote(max_restarts=-1, max_task_retries=-1)
class Cache:
    def __init__(self):
        self.data = None
    def hello(self):
        if self.data is None:
            a = ray.get_actor("hello")
            self.data = ray.get(a.hello.remote())
        return self.data

strategy = NodeAffinitySchedulingStrategy(
    node_id=ray.get_runtime_context().node_id,
    soft=True)

c = Cache.options(name="cache", get_if_exists=True).remote()
for i in range({num_iters}):
    assert ray.get(ray.get(c.hello.remote())) == "world"
    print(i, time.time(), ray.get(ray.get(c.hello.remote())))
    time.sleep(1)
"""
    output = worker.exec_run(cmd=f"python -c '{create_cache_script.format(num_iters=1)}'")
    assert output.exit_code == 0

    def kill_head():
        # sleep 3 seconds and then kill head
        sleep(2)
        head.kill()
        print("head node killed")
        sleep(1)
        head.restart()

    t = threading.Thread(target=kill_head)
    t.start()

    output = worker.exec_run(cmd=f"python -c '{create_cache_script.format(num_iters=4)}'")
    print(output.output.decode())
    assert output.exit_code == 0
    print(output.output.decode())
    t.join()



if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
