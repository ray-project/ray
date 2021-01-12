import subprocess

import pytest

import ray
import ray.util.client.server.server as ray_client_server
from ray import serve

@pytest.fixture
def ray_client_instance():
    subprocess.check_output(["ray", "start", "--head", "--ray-client-server-port", "60123"])
    try:
        yield "localhost:60123"
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


def test_ray_client(ray_client_instance):
    import time;time.sleep(5)
    ray.util.connect(ray_client_instance)
    serve.start(detached=True)
    ray.util.disconnect()

    ray.util.connect(ray_client_instance)
    client = serve.connect()

    def f(*args):
        return "hello"

    client.create_backend("test1", f)
    client.create_endpoint("test1", backend="test1")
    assert ray.get(client.get_handle("test1").remote()) == "hello"
    ray.disconnect()

    ray.util.connect(ray_client_instance)
    client = serve.connect()
    client.delete_endpoint("test1")
    client.delete_backend("test1")
