import pytest

import ray
from ray import serve
from ray._common.network_utils import build_address
from ray.serve._private.common import RequestProtocol
from ray.serve._private.test_utils import get_application_urls
from ray.serve.context import _get_global_client


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


class TestStart:
    def verify_deployment_mode(self, expected: str):
        client = _get_global_client()
        deployment_mode = client.http_config.location
        assert deployment_mode == expected

    @pytest.mark.parametrize(
        "proxy_location,expected_location",
        [
            (
                None,
                "HeadOnly",
            ),  # HeadOnly is the default location value in the `HTTPOptions`
            ("EveryNode", "EveryNode"),
            ("HeadOnly", "HeadOnly"),
            ("Disabled", "NoServer"),
        ],
    )
    def test_deployment_mode_with_http_options(
        self, ray_shutdown, proxy_location, expected_location
    ):
        # http_config.location (DeploymentMode) should be determined by proxy_location
        serve.start(proxy_location=proxy_location, http_options={"host": "0.0.0.0"})
        self.verify_deployment_mode(expected_location)

    @pytest.mark.parametrize(
        "proxy_location,expected_location",
        [
            (None, "EveryNode"),  # default DeploymentMode
            ("EveryNode", "EveryNode"),
            ("HeadOnly", "HeadOnly"),
            ("Disabled", "NoServer"),
        ],
    )
    def test_deployment_mode_without_http_options(
        self, ray_shutdown, proxy_location, expected_location
    ):
        # http_config.location (DeploymentMode) should be determined by proxy_location
        serve.start(proxy_location=proxy_location)
        self.verify_deployment_mode(expected_location)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
