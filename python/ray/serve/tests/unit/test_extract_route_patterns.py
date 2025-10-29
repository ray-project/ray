"""Unit tests for extract_route_patterns function."""
import pytest
from fastapi import FastAPI
from starlette.applications import Starlette
from starlette.routing import Mount, Route

from ray.serve._private.thirdparty.get_asgi_route_name import extract_route_patterns


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
    assert "/" in patterns
    assert "/users/{user_id}" in patterns
    assert "/items/{item_id}" in patterns
    # FastAPI adds OpenAPI routes
    assert "/openapi.json" in patterns
    assert "/docs" in patterns


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

    assert "/api/v1/users/{user_id}/posts/{post_id}" in patterns
    assert "/api/v1/users/{user_id}/settings" in patterns


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

    assert "/" in patterns
    assert "/admin/health" in patterns
    assert "/admin/status" in patterns


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

    assert "/" in patterns
    assert "/api/v1/list" in patterns
    assert "/api/v1/item/details" in patterns


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
    assert "/v1/" in patterns  # Root route
    assert "/v1/users" in patterns
    assert "/v1/items/{item_id}" in patterns


def test_extract_route_patterns_empty_app():
    """Test extracting patterns from an app with no user-defined routes."""
    app = FastAPI()
    # Don't define any routes

    patterns = extract_route_patterns(app)

    # Should still have FastAPI defaults
    assert "/openapi.json" in patterns
    assert "/docs" in patterns
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

    assert "/" in patterns
    assert "/users/{user_id}" in patterns
    # Starlette shouldn't have OpenAPI routes
    assert "/openapi.json" not in patterns


def test_extract_route_patterns_multiple_methods_same_path():
    """Test that patterns are deduplicated when multiple methods use same path."""
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

    # Should appear only once even though 3 methods use it
    pattern_count = patterns.count("/items/{item_id}")
    assert pattern_count == 1


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

    assert "/" in patterns
    assert "/custom" in patterns


def test_extract_route_patterns_sorted_output():
    """Test that output is sorted alphabetically."""
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

    # Find the user-defined routes
    user_routes = [p for p in patterns if p in ["/zebra", "/apple", "/banana"]]

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

    # FastAPI converts these to standard patterns
    assert any("user_id" in p for p in patterns)
    assert any("item_id" in p for p in patterns)


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

    assert "/http" in patterns
    assert "/ws" in patterns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
