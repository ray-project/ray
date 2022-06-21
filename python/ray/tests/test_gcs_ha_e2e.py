import pytest
import sys
import threading
from time import sleep

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

header_node = container(
    image="ray_ci:v1",
    name="gcs",
    network="{gcs_network.name}",
    command=["ray", "start", "--head", "--block", "--num-cpus", "0"],
    environment={"RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379"},
    wrapper_class=Container,
    ports={
        "8000/tcp": None,
    },
)

worker_node = container(
    image="ray_ci:v1",
    network="{gcs_network.name}",
    command=["ray", "start", "--address", "gcs:6379", "--block"],
    environment={"RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379"},
    wrapper_class=Container,
    ports={
        "8000/tcp": None,
    },
)


@pytest.fixture
def docker_cluster(header_node, worker_node):
    yield (header_node, worker_node)


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

    header, worker = docker_cluster
    output = worker.exec_run(cmd=f"python -c '{scripts.format(num_replicas=1)}'")
    assert output.exit_code == 0
    assert b"Adding 1 replicas to deployment 'Counter'." in output.output
    # somehow this is not working and the port is not exposed to the host.
    # worker_cli = worker.client()
    # print(worker_cli.request("GET", "/api/incr"))

    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=1)}'")

    assert output.exit_code == 0

    # Kill the head node
    header.kill()

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
    header.restart()

    t.join()

    output = worker.exec_run(cmd=f"python -c '{check_script.format(num_replicas=2)}'")
    assert output.exit_code == 0


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
