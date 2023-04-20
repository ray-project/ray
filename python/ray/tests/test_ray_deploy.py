import pytest
from pytest_docker_tools import container
import subprocess

from ray.tests.test_gcs_ha_e2e import Container, gcs_network  # noqa: F401


gcs_node = container(
    image="ray_ci:v1",
    name="gcs",
    network="{gcs_network.name}",
    command=["ray", "start", "--gcs", "--block", "--port=9001"],
    wrapper_class=Container,
)


rayelt_1 = container(
    image="ray_ci:v1",
    name="raylet_1",
    network="{gcs_network.name}",
    command=[
        "ray",
        "start",
        "--block",
        "--address=gcs:9001",
        "--api-server",
        "--raylet",
        "--dashboard-port=9002",
        "--dashboard-host=0.0.0.0",
        "--ray-client-server-port=9003",
    ],
    wrapper_class=Container,
    ports={
        "9002/tcp": "9002",
        "9003/tcp": "9003",
    },
)

rayelt_2 = container(
    image="ray_ci:v1",
    name="raylet_2",
    network="{gcs_network.name}",
    command=[
        "ray",
        "start",
        "--block",
        "--address=gcs:9001",
        "--api-server",
        "--raylet",
        "--dashboard-port=9002",
        "--dashboard-host=0.0.0.0",
        "--ray-client-server-port=9003",
    ],
    wrapper_class=Container,
    ports={
        "9002/tcp": "8002",
        "9003/tcp": "8003",
    },
)


@pytest.fixture
def docker_cluster(gcs_node, rayelt_1, rayelt_2):
    yield (gcs_node, rayelt_1, rayelt_2)


def test_ray_deploy_e2e(docker_cluster, tmp_path):
    """In this test we run an e2e test to make sure ray components can be deployed
    independently and still able to serve the traffic.
    node-1:
        GCS
    node-2:
        dashboard + raylet + client server
    node-3:
        dashboard + raylet + client server

    In this test we make sure the client server is available in both node-2 and
    node-3.

    It also tested that the dashboard in node-2 and node-3 works.
    """

    with (tmp_path / "script.py").open("w") as f:
        scripts = """
import ray

@ray.remote
def hello_world():
    return "hello world"

ray.init()
print(ray.get(hello_world.remote()))
"""
        f.write(scripts)

    head, r1, r2 = docker_cluster
    result = head.exec_run(cmd="bash -c 'ps -ef | grep gcs_server | grep -v grep'")
    assert "ray/core/src/ray/gcs/gcs_server" in result.output.decode()

    # Make sure ray client is still working
    import ray

    ray.init("ray://localhost:9003")

    @ray.remote
    def hello():
        return "world"

    assert ray.get(hello.remote()) == "world"
    ray.shutdown()

    # Make sure the ray client on the other server is also working
    ray.init("ray://localhost:8003")
    assert ray.get(hello.remote()) == "world"
    ray.shutdown()

    # Make sure the job submission is working
    import uuid

    submission_id = str(uuid.uuid1())
    subprocess.check_output(
        [
            "ray",
            "job",
            "submit",
            "--working-dir",
            str(tmp_path),
            "--submission-id",
            submission_id,
            "--address=http://localhost:9002",
            "--",
            "python",
            "script.py",
        ]
    )

    output = subprocess.check_output(
        f"ray job logs {submission_id} --address=http://localhost:9002", shell=True
    ).decode()
    assert "world" in output

    # Test it on the other dashboard
    submission_id = str(uuid.uuid1())
    subprocess.check_output(
        [
            "ray",
            "job",
            "submit",
            "--working-dir",
            str(tmp_path),
            "--submission-id",
            submission_id,
            "--address=http://localhost:8002",
            "--",
            "python",
            "script.py",
        ]
    )

    output = subprocess.check_output(
        f"ray job logs {submission_id} --address=http://localhost:8002", shell=True
    ).decode()
    assert "world" in output
