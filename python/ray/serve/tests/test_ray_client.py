import random
import subprocess
import sys
import time

import httpx
import pytest

import ray
from ray import serve
from ray._private.test_utils import run_string_as_driver
from ray.serve._private.utils import inside_ray_client_context

# https://tools.ietf.org/html/rfc6335#section-6
MIN_DYNAMIC_PORT = 49152
MAX_DYNAMIC_PORT = 65535


@pytest.fixture
def ray_client_instance(scope="module"):
    port = random.randint(MIN_DYNAMIC_PORT, MAX_DYNAMIC_PORT)
    subprocess.check_output(
        [
            "ray",
            "start",
            "--head",
            "--num-cpus",
            "16",
            "--ray-client-server-port",
            f"{port}",
        ]
    )
    try:
        yield f"localhost:{port}"
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


@pytest.fixture
def serve_with_client(ray_client_instance, ray_init_kwargs=None):
    if ray_init_kwargs is None:
        ray_init_kwargs = dict()
    ray.init(
        f"ray://{ray_client_instance}",
        namespace="default_test_namespace",
        **ray_init_kwargs,
    )
    assert inside_ray_client_context()

    yield

    serve.shutdown()
    ray.util.disconnect()


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "win32", reason="Buggy on MacOS"
)
def test_ray_client(ray_client_instance):
    ray.util.connect(ray_client_instance, namespace="default_test_namespace")

    start = """
import ray
ray.util.connect("{}", namespace="default_test_namespace")

from ray import serve

serve.start()
""".format(
        ray_client_instance
    )
    run_string_as_driver(start)

    deploy = """
import ray
ray.util.connect("{}", namespace="default_test_namespace")

from ray import serve

@serve.deployment
def f(*args):
    return "hello"

serve.run(f.bind(), name="test1", route_prefix="/hello")
""".format(
        ray_client_instance
    )
    run_string_as_driver(deploy)

    assert "test1" in serve.status().applications
    assert httpx.get("http://localhost:8000/hello").text == "hello"

    delete = """
import ray
ray.util.connect("{}", namespace="default_test_namespace")

from ray import serve

serve.delete("test1")
""".format(
        ray_client_instance
    )
    run_string_as_driver(delete)

    assert "test1" not in serve.status().applications

    fastapi = """
import ray
ray.util.connect("{}", namespace="default_test_namespace")

from ray import serve
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def hello():
    return "hello"

@serve.deployment
@serve.ingress(app)
class A:
    pass

serve.run(A.bind(), route_prefix="/A")
""".format(
        ray_client_instance
    )
    run_string_as_driver(fastapi)

    assert httpx.get("http://localhost:8000/A", follow_redirects=True).json() == "hello"

    serve.shutdown()
    ray.util.disconnect()


def test_quickstart_class(serve_with_client):
    @serve.deployment
    def hello(request):
        name = request.query_params["name"]
        return f"Hello {name}!"

    serve.run(hello.bind())

    # Query our endpoint over HTTP.
    response = httpx.get("http://127.0.0.1:8000/hello?name=serve").text
    assert response == "Hello serve!"


def test_quickstart_counter(serve_with_client):
    @serve.deployment
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, *args):
            self.count += 1
            return {"count": self.count}

    # Deploy our class.
    handle = serve.run(Counter.bind())
    print("deploy finished")

    # Query our endpoint in two different ways: from HTTP and from Python.
    assert httpx.get("http://127.0.0.1:8000/Counter").json() == {"count": 1}
    print("query 1 finished")
    assert handle.remote().result() == {"count": 2}
    print("query 2 finished")


@pytest.mark.parametrize(
    "serve_with_client",
    [
        {
            "runtime_env": {
                "env_vars": {
                    "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_LOWER_BOUND": "1",
                    "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_UPPER_BOUND": "2",
                }
            }
        }
    ],
    indirect=True,
)
def test_handle_hanging(serve_with_client):
    # With https://github.com/ray-project/ray/issues/20971
    # the following will hang forever.

    @serve.deployment
    def f():
        return 1

    handle = serve.run(f.bind())

    for _ in range(5):
        assert handle.remote().result() == 1
        time.sleep(0.5)

    ray.shutdown()


def test_streaming_handle(serve_with_client):
    """stream=True is not supported, check that there's a good error message."""

    @serve.deployment
    def f():
        return 1

    h = serve.run(f.bind())
    h = h.options(stream=False)
    with pytest.raises(
        RuntimeError,
        match=(
            "Streaming DeploymentHandles are not currently supported when "
            "connected to a remote Ray cluster using Ray Client."
        ),
    ):
        h = h.options(stream=True)

    assert h.remote().result() == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
