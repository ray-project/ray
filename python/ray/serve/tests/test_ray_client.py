import random
import subprocess

import pytest
import requests

import ray
from ray import serve


@pytest.fixture
def ray_client_instance():
    port = random.randint(60000, 70000)
    subprocess.check_output([
        "ray", "start", "--head", "--num-cpus", "8",
        "--ray-client-server-port", f"{port}"
    ])
    try:
        yield f"localhost:{port}"
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


def test_ray_client(ray_client_instance):
    ray.util.connect(ray_client_instance)
    client = serve.start(detached=True)

    # TODO(edoakes): disconnecting and reconnecting causes the test to
    # spuriously hang.
    # ray.util.disconnect()

    # ray.util.connect(ray_client_instance)
    # client = serve.connect()

    def f(*args):
        return "hello"

    client.create_backend("test1", f)
    client.create_endpoint("test1", backend="test1", route="/hello")
    assert requests.get("http://localhost:8000/hello").text == "hello"
    # TODO(edoakes): the below tests currently hang.
    # assert ray.get(client.get_handle("test1").remote()) == "hello"
    ray.util.disconnect()

    # ray.util.connect(ray_client_instance)
    # client = serve.connect()
    # client.delete_endpoint("test1")
    # client.delete_backend("test1")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
