import pytest

import ray
from ray import serve
from ray._common.network_utils import build_address
from ray.serve import HTTPOptions
from ray.serve._private.api import _check_http_options
from ray.serve._private.common import RequestProtocol
from ray.serve._private.test_utils import get_application_urls
from ray.serve.config import DeploymentMode
from ray.serve.exceptions import RayServeConfigException


def test_get_application_urls(serve_instance):
    @serve.deployment
    def f():
        return "Hello, world!"

    serve.run(f.bind())
    controller_details = ray.get(serve_instance._controller.get_actor_details.remote())
    node_ip = controller_details.node_ip
    assert get_application_urls(use_localhost=False) == [
        f"http://{build_address(node_ip, 8000)}"
    ]
    assert get_application_urls("gRPC", use_localhost=False) == [
        build_address(node_ip, 9000)
    ]
    assert get_application_urls(RequestProtocol.HTTP, use_localhost=False) == [
        f"http://{build_address(node_ip, 8000)}"
    ]
    assert get_application_urls(RequestProtocol.GRPC, use_localhost=False) == [
        build_address(node_ip, 9000)
    ]


def test_get_application_urls_with_app_name(serve_instance):
    @serve.deployment
    def f():
        return "Hello, world!"

    serve.run(f.bind(), name="app1", route_prefix="/")
    controller_details = ray.get(serve_instance._controller.get_actor_details.remote())
    node_ip = controller_details.node_ip
    assert get_application_urls("HTTP", app_name="app1", use_localhost=False) == [
        f"http://{node_ip}:8000"
    ]
    assert get_application_urls("gRPC", app_name="app1", use_localhost=False) == [
        f"{node_ip}:9000"
    ]


def test_get_application_urls_with_route_prefix(serve_instance):
    @serve.deployment
    def f():
        return "Hello, world!"

    serve.run(f.bind(), name="app1", route_prefix="/app1")
    controller_details = ray.get(serve_instance._controller.get_actor_details.remote())
    node_ip = controller_details.node_ip
    assert get_application_urls("HTTP", app_name="app1", use_localhost=False) == [
        f"http://{node_ip}:8000/app1"
    ]
    assert get_application_urls("gRPC", app_name="app1", use_localhost=False) == [
        f"{node_ip}:9000"
    ]


def test_serve_start(serve_instance):
    serve.start()  # success with default params
    with pytest.raises(
        RayServeConfigException,
        match=r"{'host': {'previous': '0.0.0.0', 'new': '127.0.0.1'}}",
    ):
        serve.start(http_options={"host": "127.0.0.1"})


def test_check_http_options():
    curr = HTTPOptions()
    _check_http_options(curr, None)
    _check_http_options(curr, {})

    curr = HTTPOptions()
    new = HTTPOptions()
    _check_http_options(curr, new)
    _check_http_options({}, {})
    _check_http_options(None, None)

    curr = HTTPOptions(host="127.0.0.1", port=8000)
    new = {"host": "0.0.0.0"}
    with pytest.raises(RayServeConfigException) as exc:
        _check_http_options(curr, new)
    msg = str(exc.value)
    assert "Attempted updates:" in msg
    assert "`{'host': {'previous': '127.0.0.1', 'new': '0.0.0.0'}}`" in msg

    curr = HTTPOptions(host="127.0.0.1", port=8000, root_path="")
    new = {"host": "0.0.0.0", "port": 8001}
    with pytest.raises(RayServeConfigException) as exc:
        _check_http_options(curr, new)
    msg = str(exc.value)
    assert (
        "`{'host': {'previous': '127.0.0.1', 'new': '0.0.0.0'}, "
        "'port': {'previous': 8000, 'new': 8001}}`"
    ) in msg

    curr = HTTPOptions(location=DeploymentMode.HeadOnly)
    new = HTTPOptions(location=DeploymentMode.EveryNode)
    with pytest.raises(RayServeConfigException) as exc:
        _check_http_options(curr, new)
    msg = str(exc.value)
    assert "`{'location': {'previous': 'HeadOnly', 'new': 'EveryNode'}}`" in msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
