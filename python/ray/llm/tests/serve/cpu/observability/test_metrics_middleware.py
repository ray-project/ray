"""Tests for the LLM Serve metrics middleware route resolution."""
import sys

import pytest
from fastapi import APIRouter, FastAPI

from ray.llm._internal.serve.observability.metrics.middleware import (
    _get_route_details,
)


def _scope(
    path: str, method: str = "GET", scope_type: str = "http", root_path: str = ""
):
    return {
        "type": scope_type,
        "method": method,
        "path": path,
        "headers": [],
        "app": None,  # set by caller
        "path_params": {},
        "root_path": root_path,
    }


def test_get_route_details_include_router():
    """Routes added via `include_router` must resolve without crashing (#64245).

    On FastAPI >= 0.137 these routes are nested under an `_IncludedRouter` node
    that has no `.path` attribute; accessing it previously raised
    ``AttributeError: '_IncludedRouter' object has no attribute 'path'``.
    """
    app = FastAPI()

    @app.get("/direct")
    def direct():
        return {}

    router = APIRouter(prefix="/api")

    @router.get("/items/{item_id}")
    def get_item(item_id: str):
        return item_id

    app.include_router(router)

    # Directly decorated route.
    scope = _scope("/direct")
    scope["app"] = app
    assert _get_route_details(scope) == "/direct"

    # Route registered via include_router (the #64245 regression).
    scope = _scope("/api/items/123")
    scope["app"] = app
    assert _get_route_details(scope) == "/api/items/{item_id}"

    # Unmatched path resolves to None (unchanged behavior).
    scope = _scope("/does-not-exist")
    scope["app"] = app
    assert _get_route_details(scope) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
