"""Unit tests for extract_route_patterns function."""
import pytest
from fastapi import FastAPI
from starlette.applications import Starlette
from starlette.routing import Mount, Route

from ray.serve._private.thirdparty.get_asgi_route_name import (
    extract_route_patterns,
)


def has_path(patterns, path):
    """Helper to check if a path exists in patterns list."""
    return any(pattern.path == path for pattern in patterns)


def get_methods_for_path(patterns, path):
    """Helper to get methods for a specific path."""
    for pattern in patterns:
        if pattern.path == path:
            return pattern.methods
    return None


def test_extract_route_patterns_fastapi_simple():
    """Test extracting route patterns from a simple FastAPI app."""
    app = FastAPI()

    @app.get("/")
    def root():
        return {"message": "root"}

    @app.get("/users/{user_id}")
    def get_user(user_id: str):
        return {"user_id": user_id}

    @app.post("/items/{item_id}")
    def create_item(item_id: str):
        return {"item_id": item_id}

    patterns = extract_route_patterns(app)

    # FastAPI automatically adds some default routes
    assert has_path(patterns, "/")
    assert has_path(patterns, "/users/{user_id}")
    assert has_path(patterns, "/items/{item_id}")
    # FastAPI adds OpenAPI routes
    assert has_path(patterns, "/openapi.json")
    assert has_path(patterns, "/docs")


def test_extract_route_patterns_nested_paths():
    """Test extracting nested parameterized routes."""
    app = FastAPI()

    @app.get("/api/v1/users/{user_id}/posts/{post_id}")
    def get_post(user_id: str, post_id: str):
        return {"user_id": user_id, "post_id": post_id}

    @app.get("/api/v1/users/{user_id}/settings")
    def get_settings(user_id: str):
        return {"user_id": user_id}

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/api/v1/users/{user_id}/posts/{post_id}")
    assert has_path(patterns, "/api/v1/users/{user_id}/settings")


def test_extract_route_patterns_with_mounts():
    """Test extracting route patterns from apps with mounted sub-apps."""
    # Create a sub-app
    sub_app = Starlette(
        routes=[
            Route("/health", lambda request: None),
            Route("/status", lambda request: None),
        ]
    )

    # Create main app with mounted sub-app
    app = Starlette(
        routes=[
            Route("/", lambda request: None),
            Mount("/admin", app=sub_app),
        ]
    )

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/")
    assert has_path(patterns, "/admin/health")
    assert has_path(patterns, "/admin/status")


def test_extract_route_patterns_nested_mounts():
    """Test extracting patterns from deeply nested mounts."""
    # Innermost app
    inner_app = Starlette(
        routes=[
            Route("/details", lambda request: None),
        ]
    )

    # Middle app
    middle_app = Starlette(
        routes=[
            Route("/list", lambda request: None),
            Mount("/item", app=inner_app),
        ]
    )

    # Main app
    app = Starlette(
        routes=[
            Route("/", lambda request: None),
            Mount("/api/v1", app=middle_app),
        ]
    )

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/")
    assert has_path(patterns, "/api/v1/list")
    assert has_path(patterns, "/api/v1/item/details")


def test_extract_route_patterns_with_root_path():
    """Test extracting patterns from apps with root_path set."""
    app = FastAPI(root_path="/v1")

    @app.get("/")
    def root():
        return {}

    @app.get("/users")
    def get_users():
        return []

    @app.get("/items/{item_id}")
    def get_item(item_id: str):
        return {"item_id": item_id}

    patterns = extract_route_patterns(app)

    # Root path should be prepended to all routes
    assert has_path(patterns, "/v1/")  # Root route
    assert has_path(patterns, "/v1/users")
    assert has_path(patterns, "/v1/items/{item_id}")


def test_extract_route_patterns_empty_app():
    """Test extracting patterns from an app with no user-defined routes."""
    app = FastAPI()
    # Don't define any routes

    patterns = extract_route_patterns(app)

    # Should still have FastAPI defaults
    assert has_path(patterns, "/openapi.json")
    assert has_path(patterns, "/docs")
    # May or may not have "/" depending on FastAPI version


def test_extract_route_patterns_starlette():
    """Test extracting patterns from a pure Starlette app."""

    async def homepage(request):
        return None

    async def user_detail(request):
        return None

    app = Starlette(
        routes=[
            Route("/", homepage),
            Route("/users/{user_id}", user_detail),
        ]
    )

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/")
    assert has_path(patterns, "/users/{user_id}")
    # Starlette shouldn't have OpenAPI routes
    assert not has_path(patterns, "/openapi.json")


def test_extract_route_patterns_multiple_methods_same_path():
    """Test that methods are grouped when multiple methods use same path."""
    app = FastAPI()

    @app.get("/items/{item_id}")
    def get_item(item_id: str):
        return {"item_id": item_id}

    @app.put("/items/{item_id}")
    def update_item(item_id: str):
        return {"item_id": item_id}

    @app.delete("/items/{item_id}")
    def delete_item(item_id: str):
        return {"item_id": item_id}

    patterns = extract_route_patterns(app)

    # Path should appear only once with all methods grouped
    path_count = sum(1 for pattern in patterns if pattern.path == "/items/{item_id}")
    assert path_count == 1

    # Check that all methods are present
    methods = get_methods_for_path(patterns, "/items/{item_id}")
    assert methods is not None
    assert "GET" in methods
    assert "PUT" in methods
    assert "DELETE" in methods


def test_extract_route_patterns_invalid_app():
    """Test that invalid apps return empty list gracefully."""

    class FakeApp:
        """An app without routes attribute."""

        pass

    fake_app = FakeApp()

    # Should return empty list without raising exception
    patterns = extract_route_patterns(fake_app)
    assert patterns == []


def test_extract_route_patterns_mount_without_routes():
    """Test handling mounts that don't have sub-routes."""
    from starlette.responses import PlainTextResponse

    async def custom_mount(scope, receive, send):
        response = PlainTextResponse("Custom mount")
        await response(scope, receive, send)

    app = Starlette(
        routes=[
            Route("/", lambda request: None),
            Mount("/custom", app=custom_mount),
        ]
    )

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/")
    assert has_path(patterns, "/custom")
    # Custom mount has no method restrictions
    assert get_methods_for_path(patterns, "/custom") is None


def test_extract_route_patterns_sorted_output():
    """Test that output is sorted by path."""
    app = FastAPI()

    @app.get("/zebra")
    def zebra():
        return {}

    @app.get("/apple")
    def apple():
        return {}

    @app.get("/banana")
    def banana():
        return {}

    patterns = extract_route_patterns(app)

    # Extract just the paths
    paths = [pattern.path for pattern in patterns]

    # Find the user-defined routes
    user_routes = [p for p in paths if p in ["/zebra", "/apple", "/banana"]]

    # Should be sorted
    assert user_routes == ["/apple", "/banana", "/zebra"]


def test_extract_route_patterns_special_characters():
    """Test routes with special regex characters."""
    app = FastAPI()

    @app.get("/users/{user_id:path}")
    def get_user_path(user_id: str):
        return {"user_id": user_id}

    @app.get("/items/{item_id:int}")
    def get_item_int(item_id: int):
        return {"item_id": item_id}

    patterns = extract_route_patterns(app)

    # Extract just the paths
    paths = [pattern.path for pattern in patterns]

    # FastAPI converts these to standard patterns
    assert any("user_id" in p for p in paths)
    assert any("item_id" in p for p in paths)


def test_extract_route_patterns_websocket_routes():
    """Test that WebSocket routes are also extracted."""
    app = FastAPI()

    @app.get("/http")
    def http_route():
        return {}

    @app.websocket("/ws")
    async def websocket_route(websocket):
        await websocket.accept()
        await websocket.close()

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/http")
    assert has_path(patterns, "/ws")

    # WebSocket route should have no method restrictions
    assert get_methods_for_path(patterns, "/ws") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
