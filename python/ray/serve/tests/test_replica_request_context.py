import sys

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from ray import serve
from ray.serve._private.test_utils import get_application_url
from ray.serve.context import _get_serve_request_context


def _get_request_context_route() -> str:
    return _get_serve_request_context().route


class TestHTTPRoute:
    def test_basic_route_prefix(self):
        @serve.deployment
        class A:
            def __call__(self) -> str:
                return _get_request_context_route()

        # No route prefix, should return "/" regardless of full route.
        serve.run(A.bind())
        r = httpx.get(f"{get_application_url()}/")
        assert r.status_code == 200
        assert r.text == "/"
        assert httpx.get(f"{get_application_url()}/subpath").text == "/"

        # Configured route prefix should be set.
        serve.run(A.bind(), route_prefix="/prefix")
        base_url = get_application_url(exclude_route_prefix=True)
        assert httpx.get(f"{base_url}/prefix").text == "/prefix"
        assert httpx.get(f"{base_url}/prefix/subpath").text == "/prefix"

    def test_matching_fastapi_route(self):
        fastapi_app = FastAPI()

        @serve.deployment
        @serve.ingress(fastapi_app)
        class A:
            @fastapi_app.get("/fastapi-path")
            def root(self) -> str:
                return PlainTextResponse(_get_request_context_route())

            @fastapi_app.get("/dynamic/{user_id}")
            def dynamic(self) -> str:
                return PlainTextResponse(_get_request_context_route())

        # No route prefix, should return matched fastapi route.
        serve.run(A.bind())
        assert (
            httpx.get(f"{get_application_url()}/fastapi-path").text == "/fastapi-path"
        )
        assert (
            httpx.get(f"{get_application_url()}/dynamic/abc123").text
            == "/dynamic/{user_id}"
        )

        # Configured route prefix, should return matched route prefix + fastapi route.
        serve.run(A.bind(), route_prefix="/prefix")
        base_url = get_application_url(exclude_route_prefix=True)
        assert (
            httpx.get(f"{base_url}/prefix/fastapi-path").text == "/prefix/fastapi-path"
        )
        assert (
            httpx.get(f"{base_url}/prefix/dynamic/abc123").text
            == "/prefix/dynamic/{user_id}"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
