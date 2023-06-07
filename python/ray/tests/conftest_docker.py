import time
import pytest
from pytest_docker_tools import container, fetch, network, volume
from pytest_docker_tools import wrappers

# If you need to debug tests using fixtures in this file,
# comment in the volume
# mounts in the head node and worker node containers below and use
# the repro-ci.py script to spin up an instance. The test
# setup is a little intricate, as it uses docker-in-docker.
# You need to ssh into the host machine, find the
# docker-in-docker container with
#
# docker ps
#
# Log into the container with
#
# docker exec -it <dind-daemon container id> sh
#
# And run
#
# mkdir -p /tmp/ray
# chmod 777 /tmp/ray
#
# Now you can re-run the test and the logs will show
# up in /tmp/ray in the docker-in-docker container.
# Good luck!


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
        from http.client import HTTPConnection

        port = self.ports["8000/tcp"][0]
        return HTTPConnection(f"localhost:{port}")

    def print_logs(self):
        for (name, content) in self.get_files("/tmp"):
            print(f"===== log start:  {name} ====")
            print(content.decode())


gcs_network = network(driver="bridge")

redis_image = fetch(repository="redis:latest")

redis = container(
    image="{redis_image.id}",
    network="{gcs_network.name}",
    command=("redis-server --save 60 1 --loglevel" " warning"),
)

head_node_vol = volume()
worker_node_vol = volume()
head_node_container_name = "gcs" + str(int(time.time()))
head_node = container(
    image="ray_ci:v1",
    name=head_node_container_name,
    network="{gcs_network.name}",
    command=[
        "ray",
        "start",
        "--head",
        "--block",
        "--num-cpus",
        "0",
        # Fix the port of raylet to make sure raylet restarts at the same
        # ip:port is treated as a different raylet.
        "--node-manager-port",
        "9379",
    ],
    volumes={"{head_node_vol.name}": {"bind": "/tmp", "mode": "rw"}},
    environment={"RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379"},
    wrapper_class=Container,
    ports={
        "8000/tcp": None,
    },
    # volumes={
    #     "/tmp/ray/": {"bind": "/tmp/ray/", "mode": "rw"}
    # },
)

worker_node = container(
    image="ray_ci:v1",
    network="{gcs_network.name}",
    command=[
        "ray",
        "start",
        "--address",
        f"{head_node_container_name}:6379",
        "--block",
        # Fix the port of raylet to make sure raylet restarts at the same
        # ip:port is treated as a different raylet.
        "--node-manager-port",
        "9379",
    ],
    volumes={"{worker_node_vol.name}": {"bind": "/tmp", "mode": "rw"}},
    environment={"RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379"},
    wrapper_class=Container,
    ports={
        "8000/tcp": None,
    },
    # volumes={
    #     "/tmp/ray/": {"bind": "/tmp/ray/", "mode": "rw"}
    # },
)


@pytest.fixture
def docker_cluster(head_node, worker_node):
    yield (head_node, worker_node)
