import pytest

import ray
from ray import serve
from ray._common.network_utils import build_address
from ray.serve._private.common import RequestProtocol
from ray.serve._private.test_utils import get_application_urls


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
