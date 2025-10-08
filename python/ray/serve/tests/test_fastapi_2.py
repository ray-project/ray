import httpx
import pytest
from fastapi import (
    FastAPI,
    Request,
)

from ray import serve
from ray.serve._private.test_utils import get_application_url


@pytest.mark.parametrize(
    "app_root_path,serve_root_path",
    [
        ("", ""),  # http://127.0.0.1:8000/hello, request.scope["root_path"] == ""
        (
            "/app_root_path",
            "",
        ),  # http://127.0.0.1:8000/hello, request.scope["root_path"] == "/app_root_path"
        (
            "",
            "/serve_root_path",
        ),  # http://127.0.0.1:8000/serve_root_path/hello, request.scope["root_path"] == "/serve_root_path"
        # ("/app_root_path", "/serve_root_path"), # doesn't work for uvicorn versions before and after `0.26.0`
        (
            "/root_path",
            "/root_path",
        ),  # http://127.0.0.1:8000/root_path/hello, request.scope["root_path"] == "/root_path"
    ],
)
def test_root_path(ray_shutdown, app_root_path, serve_root_path):
    app = FastAPI(root_path=app_root_path)

    @app.get("/hello")
    def func(request: Request):
        return {"hello": request.scope.get("root_path")}

    @serve.deployment
    @serve.ingress(app)
    class App:
        pass

    serve.start(http_options={"root_path": serve_root_path})
    serve.run(App.bind())

    url = get_application_url("HTTP")
    if serve_root_path != "":
        url += serve_root_path
    url += "/hello"
    resp = httpx.get(url)
    scope_root_path = app_root_path or serve_root_path
    assert resp.json() == {"hello": scope_root_path}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
