import sys
from types import SimpleNamespace

import pytest
from fastapi import APIRouter, FastAPI
from starlette.routing import Match

from ray.llm._internal.serve.observability.metrics import middleware
from ray.llm._internal.serve.observability.metrics.middleware import (
    _flatten_routes,
    _get_route_details,
)


class _FakeRouteContext:
    """Stand-in for a FastAPI route context: ``matches()`` + templated ``path``."""

    def __init__(self, path: str, match: Match = Match.FULL):
        self.path = path
        self._match = match

    def matches(self, scope: dict):
        return self._match, {}


class _FakeIncludedRouter:
    """Stand-in for ``fastapi.routing._IncludedRouter`` (FastAPI 0.137+).

    Like the real node it exposes ``effective_route_contexts()`` and crucially has
    no ``path`` attribute, so reading ``route.path`` on it raises ``AttributeError``
    (the regression this middleware works around).
    """

    def __init__(self, contexts):
        self._contexts = list(contexts)

    def effective_route_contexts(self):
        return list(self._contexts)


def _http_scope(app, method: str, path: str) -> dict:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "app": app,
        "path_params": {},
        "root_path": "",
    }


# ---------------------------------------------------------------------------
# Version-independent coverage of the FastAPI 0.137+ _IncludedRouter handling.
#
# The LLM test lockfile pins fastapi==0.133.0, where include_router() flattens
# routes directly (no _IncludedRouter), so a real FastAPI app cannot exercise
# the flattening branches in CI. These tests inject the 0.137 route shape and
# pin ``iter_route_contexts`` so every branch of _flatten_routes runs regardless
# of the installed FastAPI version.
# ---------------------------------------------------------------------------


def test_flatten_routes_expands_included_router_via_fallback(monkeypatch):
    # FastAPI 0.137.0/0.137.1: no public helper, fall back to the private
    # effective_route_contexts() on the _IncludedRouter node.
    monkeypatch.setattr(middleware, "iter_route_contexts", None)

    item_ctx = _FakeRouteContext("/api/items/{item_id}")
    included = _FakeIncludedRouter([item_ctx])
    plain = _FakeRouteContext("/health")

    assert list(_flatten_routes([included, plain])) == [item_ctx, plain]


def test_flatten_routes_uses_public_iter_route_contexts_when_available(monkeypatch):
    # FastAPI >= 0.137.2: delegate to the public iter_route_contexts() helper.
    flattened = [object(), object()]
    seen = {}

    def fake_iter_route_contexts(routes):
        seen["routes"] = routes
        yield from flattened

    monkeypatch.setattr(middleware, "iter_route_contexts", fake_iter_route_contexts)

    routes = [object()]
    assert list(_flatten_routes(routes)) == flattened
    assert seen["routes"] is routes


@pytest.mark.parametrize("match", [Match.FULL, Match.PARTIAL])
def test_get_route_details_resolves_included_router_path(monkeypatch, match):
    # An included route (full or partial match) must resolve to its templated
    # path instead of raising AttributeError on the _IncludedRouter node.
    monkeypatch.setattr(middleware, "iter_route_contexts", None)

    included = _FakeIncludedRouter([_FakeRouteContext("/api/items/{item_id}", match)])
    app = SimpleNamespace(routes=[included])

    assert (
        _get_route_details(_http_scope(app, "GET", "/api/items/123"))
        == "/api/items/{item_id}"
    )


def test_get_route_details_returns_none_when_no_route_matches(monkeypatch):
    monkeypatch.setattr(middleware, "iter_route_contexts", None)

    included = _FakeIncludedRouter(
        [_FakeRouteContext("/api/items/{item_id}", Match.NONE)]
    )
    app = SimpleNamespace(routes=[included])

    assert _get_route_details(_http_scope(app, "GET", "/nope")) is None


# ---------------------------------------------------------------------------
# Integration check against a real FastAPI app on the installed version. On
# FastAPI < 0.137 this exercises the plain-route path; on >= 0.137 it also
# covers the real _IncludedRouter flattening end to end.
# ---------------------------------------------------------------------------


def _build_app() -> FastAPI:
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


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        ("GET", "/api/items/123", "/api/items/{item_id}"),
        ("OPTIONS", "/api/items/123", "/api/items/{item_id}"),
        ("GET", "/health", "/health"),
    ],
)
def test_get_route_details_real_app(method, path, expected):
    app = _build_app()
    assert _get_route_details(_http_scope(app, method, path)) == expected


def test_get_route_details_real_app_no_match():
    app = _build_app()
    assert _get_route_details(_http_scope(app, "GET", "/does-not-exist")) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
