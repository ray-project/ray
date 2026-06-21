import sys

import pytest
from fastapi import APIRouter, FastAPI

from ray.llm._internal.serve.observability.metrics.middleware import (
    _get_route_details,
)


def _build_app() -> FastAPI:
    """FastAPI app with a top-level route and routes added via include_router().

    FastAPI 0.137 wraps the included routes in ``_IncludedRouter`` nodes that have
    no ``path`` attribute, which used to make ``_get_route_details`` raise
    ``AttributeError``.
    """
    app = FastAPI()

    @app.get("/health")
    def health() -> str:
        return "ok"

    router = APIRouter()

    @router.get("/items/{item_id}")
    def get_item(item_id: str) -> str:
        return item_id

    app.include_router(router, prefix="/api")
    return app


def _http_scope(app: FastAPI, method: str, path: str) -> dict:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "app": app,
        "path_params": {},
        "root_path": "",
    }


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        # Routes added via include_router() must resolve to their templated path
        # rather than raising AttributeError on FastAPI >= 0.137 (_IncludedRouter).
        ("GET", "/api/items/123", "/api/items/{item_id}"),
        # A partial match (path matches, method does not) must also be handled.
        ("OPTIONS", "/api/items/123", "/api/items/{item_id}"),
        # Top-level routes keep resolving as before.
        ("GET", "/health", "/health"),
    ],
)
def test_get_route_details_resolves_templated_path(method, path, expected):
    app = _build_app()
    scope = _http_scope(app, method, path)
    assert _get_route_details(scope) == expected


def test_get_route_details_returns_none_when_no_route_matches():
    app = _build_app()
    scope = _http_scope(app, "GET", "/does-not-exist")
    assert _get_route_details(scope) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
