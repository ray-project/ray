import sys

import pytest
from fastapi import FastAPI

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
