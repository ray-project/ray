import sys
from unittest.mock import MagicMock

import pytest
from fastapi import APIRouter, FastAPI

from ray.serve._private.replica import Replica
from ray.serve._private.thirdparty.get_asgi_route_name import get_asgi_route_name


def test_single_path():
    app = FastAPI()

    @app.get("/")
    def root():
        pass

    # Match.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/"}) == "/"
    )
    # Mismatched subpath.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/subpath"})
        is None
    )


def test_methods():
    app = FastAPI()

    @app.get("/")
    def root_get():
        pass

    @app.post("/")
    def root_post():
        pass

    # Match GET.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/"}) == "/"
    )
    # Match POST.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/"}) == "/"
    )
    # Missing PUT.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "PUT", "path": "/"}) is None
    )


def test_subpath():
    app = FastAPI()

    @app.get("/subpath")
    def subpath():
        pass

    # Match.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/subpath"})
        == "/subpath"
    )
    # Missing subpath.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/"}) is None
    )


def test_wildcard():
    app = FastAPI()

    @app.get("/{user_id}")
    def dynamic_subpath():
        pass

    # Match.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/abc123"})
        == "/{user_id}"
    )
    # Missing subpath.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/"}) is None
    )


def test_mounted_app():
    app = FastAPI()

    @app.get("/")
    def root():
        pass

    mounted_app = FastAPI()

    @mounted_app.get("/")
    def mounted_root():
        pass

    @mounted_app.get("/subpath")
    def mounted_subpath():
        pass

    @mounted_app.get("/subpath/{user_id}")
    def mounted_dynamic_subpath():
        pass

    app.mount("/mounted", mounted_app)

    # Match base app root.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/"}) == "/"
    )
    # Match mounted app root.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/mounted"})
        == "/mounted"
    )
    # Match mounted app subpath.
    assert (
        get_asgi_route_name(
            app, {"type": "http", "method": "GET", "path": "/mounted/subpath"}
        )
        == "/mounted/subpath"
    )
    # Match mounted app dynamic subpath.
    assert (
        get_asgi_route_name(
            app, {"type": "http", "method": "GET", "path": "/mounted/subpath/abc123"}
        )
        == "/mounted/subpath/{user_id}"
    )
    # Missing mounted app route.
    assert (
        get_asgi_route_name(
            app, {"type": "http", "method": "GET", "path": "/mounted/some-other-path"}
        )
        is None
    )


def test_included_router():
    """Routes registered via `include_router` must resolve their route name.

    On FastAPI >= 0.137 these routes are nested under an `_IncludedRouter` node
    instead of being flattened into `app.routes` (see #64475).
    """
    app = FastAPI()

    @app.get("/direct")
    def direct():
        pass

    # Router with its own prefix, included without an include-time prefix.
    router = APIRouter(prefix="/prefix")

    @router.get("/routed/{user_id}")
    def routed():
        pass

    @router.websocket("/ws")
    async def ws():
        pass

    # Router included with an include-time prefix.
    other = APIRouter()

    @other.post("/create")
    def create():
        pass

    app.include_router(router)
    app.include_router(other, prefix="/other")

    # Directly decorated route still resolves.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/direct"})
        == "/direct"
    )
    # Route from an included router (with dynamic segment).
    assert (
        get_asgi_route_name(
            app, {"type": "http", "method": "GET", "path": "/prefix/routed/abc123"}
        )
        == "/prefix/routed/{user_id}"
    )
    # WebSocket route from an included router.
    assert (
        get_asgi_route_name(app, {"type": "websocket", "path": "/prefix/ws"})
        == "/prefix/ws"
    )
    # Route from a router included with an include-time prefix.
    assert (
        get_asgi_route_name(
            app, {"type": "http", "method": "POST", "path": "/other/create"}
        )
        == "/other/create"
    )
    # Unknown path still returns None.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/nope"})
        is None
    )


def test_root_path():
    app = FastAPI(root_path="/some/root")

    @app.get("/subpath")
    def subpath():
        pass

    assert (
        get_asgi_route_name(
            app,
            {
                "type": "http",
                "method": "GET",
                "path": "/subpath",
                "root_path": "/some/root",
            },
        )
        == "/some/root/subpath"
    )


@pytest.mark.parametrize("redirect_slashes", [False, True])
def test_redirect_slashes(redirect_slashes: bool):
    app = FastAPI(redirect_slashes=redirect_slashes)

    @app.get("/subpath")
    def subpath():
        pass

    # Should always match.
    assert (
        get_asgi_route_name(app, {"type": "http", "method": "GET", "path": "/subpath"})
        == "/subpath"
    )
    # Should match depending on redirect_slashes behavior.
    if redirect_slashes:
        assert (
            get_asgi_route_name(
                app, {"type": "http", "method": "GET", "path": "/subpath/"}
            )
            == "/subpath/"
        )
    else:
        assert (
            get_asgi_route_name(
                app, {"type": "http", "method": "GET", "path": "/subpath/"}
            )
            is None
        )

    @app.get("/other/{user_id}")
    def dynamic_subpath():
        pass

    # Should always match.
    assert (
        get_asgi_route_name(
            app, {"type": "http", "method": "GET", "path": "/other/abc123"}
        )
        == "/other/{user_id}"
    )
    # Should match depending on redirect_slashes behavior.
    if redirect_slashes:
        assert (
            get_asgi_route_name(
                app, {"type": "http", "method": "GET", "path": "/other/abc123/"}
            )
            == "/other/{user_id}/"
        )
    else:
        assert (
            get_asgi_route_name(
                app, {"type": "http", "method": "GET", "path": "/other/abc123/"}
            )
            is None
        )


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
    """`Replica._determine_http_route` wraps `get_asgi_route_name` with a
    route-prefix fallback, coercing a missing prefix to "" so the route stays a
    string."""
    app = FastAPI()

    @app.post("/internal/route")
    def route():
        pass

    fake = MagicMock()
    fake._route_prefix = route_prefix
    fake._user_callable_asgi_app = app
    scope = {"type": "http", "method": method, "path": path}
    assert Replica._determine_http_route(fake, scope) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
