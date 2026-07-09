import sys
from typing import Dict, Optional
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI

from ray.serve._private.replica import Replica


def _make_fake(
    *, route_prefix: Optional[str], asgi_app: Optional[FastAPI]
) -> MagicMock:
    fake = MagicMock()
    fake._route_prefix = route_prefix
    fake._user_callable_asgi_app = asgi_app
    return fake


def _scope(method: str, path: str) -> Dict[str, str]:
    return {"type": "http", "method": method, "path": path}


def _router_app() -> FastAPI:
    app = FastAPI()

    @app.post("/internal/route")
    def route():
        pass

    @app.get("/health")
    def health():
        pass

    return app


@pytest.mark.parametrize(
    ("route_prefix", "method", "path", "expected"),
    [
        # Matched routes resolve to the ASGI route name regardless of prefix.
        (None, "POST", "/internal/route", "/internal/route"),
        (None, "GET", "/health", "/health"),
        ("/app", "POST", "/internal/route", "/internal/route"),
        # Unmatched paths fall back to the route prefix.
        ("/app", "GET", "/nope", "/app"),
        ("/", "GET", "/nope", "/"),
        # The ingress request router has no route prefix. Unmatched paths must
        # fall back to "" so the route stays a string for metric tags.
        (None, "GET", "/nope", ""),
        (None, "GET", "/metrics", ""),
    ],
)
def test_determine_http_route(route_prefix, method, path, expected):
    fake = _make_fake(route_prefix=route_prefix, asgi_app=_router_app())
    route = Replica._determine_http_route(fake, _scope(method, path))
    assert route == expected


def test_determine_http_route_no_asgi_app():
    # Without an ASGI app the route is the prefix, coerced to "" when None.
    fake = _make_fake(route_prefix=None, asgi_app=None)
    assert Replica._determine_http_route(fake, _scope("GET", "/nope")) == ""


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
