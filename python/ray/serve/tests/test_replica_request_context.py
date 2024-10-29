import sys

import pytest
import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from ray import serve
from ray.serve.context import _serve_request_context


def _get_request_context_route() -> str:
    return _serve_request_context.get().route


class TestHTTPRoute:
    def test_basic_route_prefix(self):
        @serve.deployment
        class A:
            def __call__(self) -> str:
                return _get_request_context_route()

        # No route prefix, should return "/" regardless of full route.
        serve.run(A.bind())
        assert requests.get("http://localhost:8000/").text == "/"
        assert requests.get("http://localhost:8000/subpath").text == "/"

        # Configured route prefix should be set.
        serve.run(A.bind(), route_prefix="/prefix")
        assert requests.get("http://localhost:8000/prefix").text == "/prefix"
        assert requests.get("http://localhost:8000/prefix/subpath").text == "/prefix"

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
            requests.get("http://localhost:8000/fastapi-path").text == "/fastapi-path"
        )
        assert (
            requests.get("http://localhost:8000/dynamic/abc123").text
            == "/dynamic/{user_id}"
        )

        # Configured route prefix, should return matched route prefix + fastapi route.
        serve.run(A.bind(), route_prefix="/prefix")
        assert (
            requests.get("http://localhost:8000/prefix/fastapi-path").text
            == "/prefix/fastapi-path"
        )
        assert (
            requests.get("http://localhost:8000/prefix/dynamic/abc123").text
            == "/prefix/dynamic/{user_id}"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
