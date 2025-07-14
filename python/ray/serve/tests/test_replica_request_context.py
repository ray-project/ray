import sys

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from ray import serve
from ray.serve._private.test_utils import get_application_url
from ray.serve.context import _get_serve_request_context


def create_basic_deployment():
    """Create a basic deployment that returns the request context route."""

    @serve.deployment
    class BasicDeployment:
        def __call__(self) -> str:
            return _get_request_context_route()

    return BasicDeployment


def create_fastapi_deployment():
    """Create a FastAPI deployment with multiple routes."""
    fastapi_app = FastAPI()

    @serve.deployment
    @serve.ingress(fastapi_app)
    class FastAPIDeployment:
        @fastapi_app.get("/fastapi-path")
        def root(self) -> str:
            return PlainTextResponse(_get_request_context_route())

        @fastapi_app.get("/dynamic/{user_id}")
        def dynamic(self) -> str:
            return PlainTextResponse(_get_request_context_route())

    return FastAPIDeployment


def _make_http_request(
    path: str, is_prefixed_route: bool = False, expected_status: int = 200
) -> str:
    """Make an HTTP request and return the response text.

    Args:
        path: Path to request
        is_prefixed_route: Route prefix to use
        expected_status: Expected HTTP status code

    Returns:
        Response text

    Raises:
        AssertionError: If status code doesn't match expected
    """
    if is_prefixed_route:
        url = get_application_url(exclude_route_prefix=True)
    else:
        url = get_application_url()

    full_url = f"{url}{path}"
    response = httpx.get(full_url)

    assert response.status_code == expected_status, (
        f"Expected status {expected_status}, got {response.status_code} "
        f"for URL: {full_url}"
    )

    return response.text


def _get_request_context_route() -> str:
    """Get the current request route from the serve context."""
    return _get_serve_request_context().route


class TestHTTPRoute:
    """Test HTTP route handling in Ray Serve deployments."""

    @staticmethod
    def setup_method(self):
        """Clean up any existing deployments before each test."""
        serve.shutdown()

    @staticmethod
    def teardown_method():
        """Clean up deployments after each test."""
        serve.shutdown()

    @pytest.mark.parametrize(
        "route_prefix,test_path,expected_route",
        [
            (None, "/", "/"),
            (None, "/subpath", "/"),
            ("/prefix", "/prefix", "/prefix"),
            ("/prefix", "/prefix/subpath", "/prefix"),
        ],
    )
    def test_basic_route_prefix(self, route_prefix, test_path, expected_route):
        """Test basic deployment route prefix handling."""
        deployment = create_basic_deployment()

        # Deploy with or without route prefix
        if route_prefix:
            serve.run(deployment.bind(), route_prefix=route_prefix)
        else:
            serve.run(deployment.bind())

        # Test the path
        response_text = _make_http_request(
            test_path, is_prefixed_route=(route_prefix is not None)
        )
        assert response_text == expected_route, (
            f"Expected '{expected_route}', got '{response_text}' "
            f"for path '{test_path}' with route_prefix='{route_prefix}'"
        )

    @pytest.mark.parametrize(
        "route_prefix,test_path,expected_route",
        [
            (None, "/fastapi-path", "/fastapi-path"),
            (None, "/dynamic/abc123", "/dynamic/{user_id}"),
            ("/prefix", "/prefix/fastapi-path", "/prefix/fastapi-path"),
            ("/prefix", "/prefix/dynamic/abc123", "/prefix/dynamic/{user_id}"),
        ],
    )
    def test_fastapi_route(self, route_prefix, test_path, expected_route):
        """Test FastAPI deployment route handling."""
        deployment = create_fastapi_deployment()

        # Deploy with or without route prefix
        if route_prefix:
            serve.run(deployment.bind(), route_prefix=route_prefix)
        else:
            serve.run(deployment.bind())

        # Test the path
        response_text = _make_http_request(
            test_path, is_prefixed_route=(route_prefix is not None)
        )
        assert response_text == expected_route, (
            f"Expected '{expected_route}', got '{response_text}' "
            f"for path '{test_path}' with route_prefix='{route_prefix}'"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
