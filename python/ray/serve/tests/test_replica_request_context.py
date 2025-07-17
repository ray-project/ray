import sys

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from ray import serve
from ray.serve._private.test_utils import get_application_url
from ray.serve.context import _get_serve_request_context


def basic_deployment():
    """Create a basic deployment that returns the request context route."""

    @serve.deployment
    class BasicDeployment:
        def __call__(self) -> str:
            return _get_request_context_route()

    return BasicDeployment


def fast_api_deployment():
    """Create a FastAPI deployment with multiple routes."""
    fastapi_app = FastAPI()

    @serve.deployment
    @serve.ingress(fastapi_app)
    class FastAPIDeployment:
        @fastapi_app.get("/fastapi-path")
        def root(self) -> PlainTextResponse:
            return PlainTextResponse(_get_request_context_route())

        @fastapi_app.get("/dynamic/{user_id}")
        def dynamic(self) -> PlainTextResponse:
            return PlainTextResponse(_get_request_context_route())

    return FastAPIDeployment


def _make_http_request(
    path: str, is_prefixed_route: bool = False, expected_status: int = 200
) -> httpx.Response:
    """Make an HTTP request and return the response."""
    if is_prefixed_route:
        url = get_application_url(exclude_route_prefix=True)
    else:
        url = get_application_url()

    full_url = f"{url}{path}"
    return httpx.get(full_url, timeout=30.0)


def _get_request_context_route() -> str:
    """Get the current request route from the serve context."""
    context = _get_serve_request_context()
    return context.route if context else ""


class TestHTTPRoute:
    """Test HTTP route handling in Ray Serve deployments."""

    @pytest.mark.parametrize(
        "deployment_func,route_prefix,test_path,expected_route",
        [
            (basic_deployment, None, "/", "/"),
            (basic_deployment, None, "/subpath", "/"),
            (basic_deployment, "/prefix", "/prefix", "/prefix"),
            (basic_deployment, "/prefix", "/prefix/subpath", "/prefix"),
            (fast_api_deployment, None, "/fastapi-path", "/fastapi-path"),
            (fast_api_deployment, None, "/dynamic/abc123", "/dynamic/{user_id}"),
            (
                fast_api_deployment,
                "/prefix",
                "/prefix/fastapi-path",
                "/prefix/fastapi-path",
            ),
            (
                fast_api_deployment,
                "/prefix",
                "/prefix/dynamic/abc123",
                "/prefix/dynamic/{user_id}",
            ),
        ],
    )
    def test_route_prefix(
        self, serve_instance, deployment_func, route_prefix, test_path, expected_route
    ):
        """Test deployment route prefix handling for both basic and FastAPI deployments."""
        deployment = deployment_func()

        # Deploy with or without route prefix
        if route_prefix:
            serve.run(deployment.bind(), route_prefix=route_prefix)
        else:
            serve.run(deployment.bind())

        # Test the path
        response = _make_http_request(
            test_path, is_prefixed_route=(route_prefix is not None)
        )

        assert response.text == expected_route, (
            f"Expected '{expected_route}', got '{response.text}' "
            f"for path '{test_path}' with route_prefix='{route_prefix}'"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
