import time
import pytest
from pytest_docker_tools import container, fetch, network, volume
from pytest_docker_tools import wrappers
import subprocess
from typing import List

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


def gen_head_node(envs):
    return container(
        image="rayproject/ray:ha_integration",
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
        environment=envs,
        wrapper_class=Container,
        ports={
            "8000/tcp": None,
        },
        # volumes={
        #     "/tmp/ray/": {"bind": "/tmp/ray/", "mode": "rw"}
        # },
    )


def gen_worker_node(envs):
    return container(
        image="rayproject/ray:ha_integration",
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
        environment=envs,
        wrapper_class=Container,
        ports={
            "8000/tcp": None,
        },
        # volumes={
        #     "/tmp/ray/": {"bind": "/tmp/ray/", "mode": "rw"}
        # },
    )


head_node = gen_head_node(
    {
        "RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379",
        "RAY_raylet_client_num_connect_attempts": "10",
        "RAY_raylet_client_connect_timeout_milliseconds": "100",
    }
)

worker_node = gen_worker_node(
    {
        "RAY_REDIS_ADDRESS": "{redis.ips.primary}:6379",
        "RAY_raylet_client_num_connect_attempts": "10",
        "RAY_raylet_client_connect_timeout_milliseconds": "100",
    }
)


@pytest.fixture
def docker_cluster(head_node, worker_node):
    yield (head_node, worker_node)


def run_in_container(cmds: List[List[str]], container_id: str):
    outputs = []
    for cmd in cmds:
        docker_cmd = ["docker", "exec", container_id] + cmd
        print(f"Executing command: {docker_cmd}", time.time())
        resp = subprocess.check_output(docker_cmd, stderr=subprocess.STDOUT)
        output = resp.decode("utf-8").strip()
        print(f"Output: {output}")
        outputs.append(output)

    return outputs


IMAGE_NAME = "rayproject/ray:runtime_env_container"
NESTED_IMAGE_NAME = "rayproject/ray:runtime_env_container_nested"


@pytest.fixture(scope="session")
def podman_docker_cluster():
    start_container_command = [
        "docker",
        "run",
        "-d",
        "--privileged",
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock",
        "-v",
        "/var/lib/containers:/var/lib/containers",
        # For testing environment variables
        "--env",
        "RAY_TEST_ABC=1",
        "--env",
        "TEST_ABC=1",
        IMAGE_NAME,
        "tail",
        "-f",
        "/dev/null",
    ]
    container_id = subprocess.check_output(start_container_command).decode("utf-8")
    container_id = container_id.strip()

    # Get group id that owns the docker socket file. Add user `ray` to
    # group to get necessary permissions for pulling an image from
    # docker's local storage into podman
    docker_group_id = run_in_container(
        [["stat", "-c", "%g", "/var/run/docker.sock"]], container_id
    )[0]
    run_in_container(
        [
            ["id"],
            ["sudo", "groupadd", "-g", docker_group_id, "docker"],
            ["sudo", "usermod", "-aG", "docker", "ray"],
            ["podman", "pull", f"docker-daemon:{IMAGE_NAME}"],
        ],
        container_id,
    )

    # Add custom file to new image tagged `runtime_env_container_nested`,
    # which can be read by Ray actors / Serve deployments to verify the
    # container runtime env plugin. Also add serve application that will
    # be imported by the telemetry test.
    serve_app = """
from ray import serve
@serve.deployment
class Model:
    def __call__(self):
        with open("file.txt") as f:
            return f.read().strip()
app = Model.bind()
"""

    run_in_container(
        [
            ["bash", "-c", "echo helloworldalice >> /tmp/file.txt"],
            ["bash", "-c", f"echo '{serve_app}' >> /tmp/serve_application.py"],
            ["podman", "create", "--name", "tmp_container", IMAGE_NAME],
            ["podman", "cp", "/tmp/file.txt", "tmp_container:/home/ray/file.txt"],
            [
                "podman",
                "cp",
                "/tmp/serve_application.py",
                "tmp_container:/home/ray/serve_application.py",
            ],
            ["podman", "commit", "tmp_container", NESTED_IMAGE_NAME],
        ],
        container_id,
    )

    # For debugging
    run_in_container([["podman", "image", "ls"]], container_id)

    yield container_id

    subprocess.check_call(["docker", "kill", container_id])
