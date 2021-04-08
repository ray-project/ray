import random
import subprocess

import pytest
import requests

import ray
from ray.test_utils import run_string_as_driver
from ray import serve

# https://tools.ietf.org/html/rfc6335#section-6
MIN_DYNAMIC_PORT = 49152
MAX_DYNAMIC_PORT = 65535


@pytest.fixture
def ray_client_instance():
    port = random.randint(MIN_DYNAMIC_PORT, MAX_DYNAMIC_PORT)
    subprocess.check_output([
        "ray", "start", "--head", "--num-cpus", "16",
        "--ray-client-server-port", f"{port}"
    ])
    try:
        yield f"localhost:{port}"
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


def test_ray_client(ray_client_instance):
    ray.util.connect(ray_client_instance)

    start = """
import ray
ray.util.connect("{}")

from ray import serve

serve.start(detached=True)
""".format(ray_client_instance)
    run_string_as_driver(start)

    serve.connect()

    deploy = """
import ray
ray.util.connect("{}")

from ray import serve

def f(*args):
    return "hello"

serve.create_backend("test1", f)
serve.create_endpoint("test1", backend="test1", route="/hello")
""".format(ray_client_instance)
    run_string_as_driver(deploy)

    assert "test1" in serve.list_backends()
    assert "test1" in serve.list_endpoints()
    assert requests.get("http://localhost:8000/hello").text == "hello"

    delete = """
import ray
ray.util.connect("{}")

from ray import serve

serve.delete_endpoint("test1")
serve.delete_backend("test1")
""".format(ray_client_instance)
    run_string_as_driver(delete)

    assert "test1" not in serve.list_backends()
    assert "test1" not in serve.list_endpoints()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
