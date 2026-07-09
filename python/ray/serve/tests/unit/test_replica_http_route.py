import sys
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI

from ray.serve._private.replica import Replica


def _router_app() -> FastAPI:
    app = FastAPI()

    @app.post("/internal/route")
    def route():
        pass

    return app


@pytest.mark.parametrize(
    ("route_prefix", "method", "path", "expected"),
    [
        # A matched ASGI route resolves to its name regardless of prefix.
        (None, "POST", "/internal/route", "/internal/route"),
        # A real route prefix passes through unchanged for unmatched paths.
        ("/", "GET", "/nope", "/"),
        # The ingress request router has no prefix. Unmatched paths fall back to
        # "" so the route stays a string for metric tags.
        (None, "GET", "/nope", ""),
    ],
)
def test_determine_http_route(route_prefix, method, path, expected):
    fake = MagicMock()
    fake._route_prefix = route_prefix
    fake._user_callable_asgi_app = _router_app()
    scope = {"type": "http", "method": method, "path": path}
    assert Replica._determine_http_route(fake, scope) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
