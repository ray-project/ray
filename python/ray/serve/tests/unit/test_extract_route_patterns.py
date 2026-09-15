"""Unit tests for extract_route_patterns function."""
import pytest
from fastapi import APIRouter, FastAPI
from starlette.applications import Starlette
from starlette.routing import Mount, Route

from ray.serve._private.thirdparty.get_asgi_route_name import (
    ASGIRoutePatternMatcher,
    RoutePattern,
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


def test_extract_route_patterns_included_router():
    """Routes registered via `include_router` must be extracted.

    On FastAPI >= 0.137 these routes are nested under an `_IncludedRouter` node
    instead of being flattened into `app.routes` (see #64475).
    """
    app = FastAPI()

    @app.get("/direct")
    def direct():
        return {}

    # Router with its own prefix, included without an include-time prefix.
    router = APIRouter(prefix="/prefix")

    @router.get("/routed/{user_id}")
    def routed():
        return {}

    @router.websocket("/ws")
    async def ws():
        pass

    # Router included with an include-time prefix.
    other = APIRouter()

    @other.post("/create")
    def create():
        return {}

    app.include_router(router)
    app.include_router(other, prefix="/other")

    patterns = extract_route_patterns(app)

    assert has_path(patterns, "/direct")
    assert has_path(patterns, "/prefix/routed/{user_id}")
    assert get_methods_for_path(patterns, "/prefix/routed/{user_id}") == ["GET"]
    assert has_path(patterns, "/other/create")
    assert get_methods_for_path(patterns, "/other/create") == ["POST"]
    # WebSocket routes have no method restrictions.
    assert has_path(patterns, "/prefix/ws")
    assert get_methods_for_path(patterns, "/prefix/ws") is None


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


class TestASGIRoutePatternMatcher:
    """Matching a (method, path) against a fixed set of route patterns.

    Two callers depend on this agreeing with itself: the proxy tags metrics with
    the matched pattern, and the LLM ingress request router decides from it
    whether a request belongs to the application ingress rather than to a model
    deployment. A disagreement between them would route a control request to a
    model, so the matching lives in one place and is tested here.
    """

    def test_exact_path_and_method(self):
        matcher = ASGIRoutePatternMatcher(
            [RoutePattern(methods=["GET"], path="/v1/models")]
        )
        assert matcher.match("GET", "/v1/models") == "/v1/models"
        assert matcher.matches("GET", "/v1/models")

    def test_method_restrictions_are_enforced(self):
        """`GET /foo` and `POST /foo` can belong to different destinations, so a
        path match with the wrong method is not a match."""
        matcher = ASGIRoutePatternMatcher(
            [
                RoutePattern(methods=["GET"], path="/v1/models"),
                RoutePattern(methods=["POST"], path="/admin/pause"),
            ]
        )
        assert matcher.matches("GET", "/v1/models")
        assert not matcher.matches("POST", "/v1/models")
        assert matcher.matches("POST", "/admin/pause")
        assert not matcher.matches("GET", "/admin/pause")

    def test_same_path_with_two_methods(self):
        matcher = ASGIRoutePatternMatcher(
            [
                RoutePattern(methods=["GET"], path="/thing"),
                RoutePattern(methods=["POST"], path="/thing"),
            ]
        )
        assert matcher.match("GET", "/thing") == "/thing"
        assert matcher.match("POST", "/thing") == "/thing"
        assert not matcher.matches("DELETE", "/thing")

    def test_named_parameter_matches_one_segment(self):
        matcher = ASGIRoutePatternMatcher(
            [RoutePattern(methods=["GET"], path="/users/{user_id}")]
        )
        assert matcher.match("GET", "/users/abc") == "/users/{user_id}"
        # A single `{name}` does not span a `/`.
        assert not matcher.matches("GET", "/users/abc/def")

    def test_path_converter_spans_segments(self):
        """`{model:path}` is what makes `/v1/models/{model}` work for model ids
        that contain slashes, e.g. `meta-llama/Llama-3`."""
        matcher = ASGIRoutePatternMatcher(
            [RoutePattern(methods=["GET"], path="/v1/models/{model:path}")]
        )
        assert (
            matcher.match("GET", "/v1/models/meta-llama/Llama-3")
            == "/v1/models/{model:path}"
        )

    def test_trailing_slash_follows_starlette_redirect_behavior(self):
        matcher = ASGIRoutePatternMatcher(
            [RoutePattern(methods=["GET"], path="/v1/models")]
        )
        # Starlette redirects the slashed form to the same route, and
        # get_asgi_route_name mirrors that rather than reporting no match.
        assert matcher.matches("GET", "/v1/models/")

    def test_unmatched_path_returns_none(self):
        matcher = ASGIRoutePatternMatcher(
            [RoutePattern(methods=["GET"], path="/v1/models")]
        )
        assert matcher.match("GET", "/v1/chat/completions") is None
        assert not matcher.matches("GET", "/v1/chat/completions")

    def test_empty_patterns_match_nothing(self):
        matcher = ASGIRoutePatternMatcher([])
        assert matcher.match("GET", "/anything") is None
        assert not matcher.matches("GET", "/")

    def test_none_methods_allows_any_method(self):
        """`methods=None` is what extract_route_patterns reports for WebSocket
        routes and mounted ASGI apps."""
        matcher = ASGIRoutePatternMatcher([RoutePattern(methods=None, path="/mounted")])
        assert matcher.matches("GET", "/mounted")
        assert matcher.matches("POST", "/mounted")

    def test_invalid_pattern_raises_at_construction(self):
        """Construction is where an unusable pattern surfaces, not the first
        request: the proxy catches this to fall back to the route prefix, and
        the LLM router lets it fail initialization."""
        # Starlette tolerates a stray "{", so use inputs it genuinely rejects.
        with pytest.raises(AssertionError):
            ASGIRoutePatternMatcher(
                [RoutePattern(methods=["GET"], path="no-leading-slash")]
            )
        with pytest.raises(AssertionError):
            ASGIRoutePatternMatcher(
                [RoutePattern(methods=["GET"], path="/{x:nosuchconverter}")]
            )

    def test_match_scope_preserves_root_path(self):
        """The proxy passes a real request scope, so fields get_asgi_route_name
        reads must survive -- `root_path` is prepended to the matched name."""
        matcher = ASGIRoutePatternMatcher(
            [RoutePattern(methods=["GET"], path="/v1/models")]
        )
        matched = matcher.match_scope(
            {
                "type": "http",
                "method": "GET",
                "path": "/v1/models",
                "root_path": "/prefix",
            }
        )
        assert matched == "/prefix/v1/models"

    def test_matches_routes_extracted_from_a_real_app(self):
        """End to end with the producer side: whatever extract_route_patterns
        reports for an app is matchable for that app's own requests."""
        app = FastAPI()

        @app.get("/v1/models")
        def list_models():
            return []

        @app.post("/v1/chat/completions")
        def completions():
            return {}

        matcher = ASGIRoutePatternMatcher(extract_route_patterns(app))
        assert matcher.matches("GET", "/v1/models")
        assert matcher.matches("POST", "/v1/chat/completions")
        assert not matcher.matches("POST", "/v1/models")
        assert not matcher.matches("GET", "/v1/embeddings")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
