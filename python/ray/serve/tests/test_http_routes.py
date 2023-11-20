import time

import pytest
import requests
from fastapi import FastAPI, Request
from starlette.responses import RedirectResponse

import ray
from ray import serve
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME


def test_path_validation(serve_instance):
    # Path prefix must start with /.
    with pytest.raises(ValueError):

        @serve.deployment(route_prefix="hello")
        class D1:
            pass

    # Path prefix must not end with / unless it's the root.
    with pytest.raises(ValueError):

        @serve.deployment(route_prefix="/hello/")
        class D2:
            pass

    # Wildcards not allowed with new ingress support.
    with pytest.raises(ValueError):

        @serve.deployment(route_prefix="/{hello}")
        class D3:
            pass

    @serve.deployment(route_prefix="/duplicate")
    class D4:
        pass

    serve.run(D4.bind())


def test_routes_healthz(serve_instance):
    resp = requests.get("http://localhost:8000/-/healthz")
    assert resp.status_code == 200
    assert resp.content == b"success"


def test_routes_endpoint(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self, *args):
            return "D1"

    @serve.deployment
    class D2:
        def __call__(self, *args):
            return "D2"

    serve.run(D1.bind(), name="app1", route_prefix="/D1")
    serve.run(D2.bind(), name="app2", route_prefix="/hello/world")

    routes = requests.get("http://localhost:8000/-/routes").json()

    assert len(routes) == 2, routes

    assert requests.get("http://localhost:8000/D1").text == "D1"
    assert requests.get("http://localhost:8000/D1").status_code == 200
    assert requests.get("http://localhost:8000/hello/world").text == "D2"
    assert requests.get("http://localhost:8000/hello/world").status_code == 200
    assert requests.get("http://localhost:8000/not_exist").status_code == 404
    assert requests.get("http://localhost:8000/").status_code == 404


def test_deployment_without_route(serve_instance):
    @serve.deployment(route_prefix=None)
    class D:
        def __call__(self, *args):
            return "1"

    serve.run(D.bind(), route_prefix=None)
    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 0

    # make sure the deployment is not exposed under the default route
    r = requests.get("http://localhost:8000/")
    assert r.status_code == 404


def test_deployment_options_default_route(serve_instance):
    @serve.deployment
    class D1:
        pass

    serve.run(D1.bind())

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1
    assert "/" in routes, routes
    assert routes["/"] == SERVE_DEFAULT_APP_NAME


def test_path_prefixing_1(serve_instance):
    def check_req(subpath, text=None, status=None):
        r = requests.get(f"http://localhost:8000{subpath}")
        if text is not None:
            assert r.text == text, f"{r.text} != {text}"
        if status is not None:
            assert r.status_code == status, f"{r.status_code} != {status}"

        return r

    @serve.deployment
    class D1:
        def __call__(self, *args):
            return "1"

    serve.run(D1.bind(), route_prefix="/hello", name="app1")
    check_req("/", status=404)
    check_req("/hello", text="1")
    check_req("/hello/", text="1")
    check_req("/hello/a", text="1")

    @serve.deployment
    class D2:
        def __call__(self, *args):
            return "2"

    serve.run(D2.bind(), route_prefix="/", name="app2")
    check_req("/hello/", text="1")
    check_req("/hello/a", text="1")
    check_req("/", text="2")
    check_req("/a", text="2")

    @serve.deployment(route_prefix="/hello/world")
    class D3:
        def __call__(self, *args):
            return "3"

    serve.run(D3.bind(), route_prefix="/hello/world", name="app3")
    check_req("/hello/", text="1")
    check_req("/", text="2")
    check_req("/hello/world/", text="3")

    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class D4:
        @app.get("/")
        def root(self):
            return 4

        @app.get("/{p}")
        def subpath(self, p: str):
            return p

    serve.run(D4.bind(), route_prefix="/hello/world/again", name="app4")
    check_req("/hello/") == "1"
    check_req("/") == "2"
    check_req("/hello/world/") == "3"
    check_req("/hello/world/again/") == "4"
    check_req("/hello/world/again/hi") == '"hi"'


@pytest.mark.parametrize("base_path", ["", "subpath"])
def test_redirect(serve_instance, base_path):
    app = FastAPI()

    route_prefix = f"/{base_path}"

    @serve.deployment()
    @serve.ingress(app)
    class D:
        @app.get("/")
        def root(self):
            return "hello from /"

        @app.get("/redirect")
        def redirect_root(self, request: Request):
            root_path = request.scope.get("root_path")
            if not root_path.endswith("/"):
                root_path += "/"
            return RedirectResponse(url=root_path)

        @app.get("/redirect2")
        def redirect_twice(self, request: Request):
            root_path = request.scope.get("root_path")
            if root_path.endswith("/"):
                root_path = root_path[:-1]
            return RedirectResponse(url=root_path + app.url_path_for("redirect_root"))

    serve.run(D.bind(), route_prefix=route_prefix)

    if route_prefix != "/":
        route_prefix += "/"

    r = requests.get(f"http://localhost:8000{route_prefix}redirect")
    assert r.status_code == 200
    assert len(r.history) == 1
    assert r.json() == "hello from /"

    r = requests.get(f"http://localhost:8000{route_prefix}redirect2")
    assert r.status_code == 200
    assert len(r.history) == 2
    assert r.json() == "hello from /"


def test_default_error_handling(serve_instance):
    @serve.deployment
    def f():
        1 / 0

    serve.run(f.bind())
    r = requests.get("http://localhost:8000/f")
    assert r.status_code == 500
    assert "ZeroDivisionError" in r.text, r.text

    @ray.remote(num_cpus=0)
    def intentional_kill(actor_handle):
        ray.kill(actor_handle, no_restart=False)

    @serve.deployment
    def h():
        ray.get(intentional_kill.remote(ray.get_runtime_context().current_actor))
        time.sleep(100)  # Don't return here to leave time for actor exit.

    serve.run(h.bind())
    r = requests.get("http://localhost:8000/h")
    assert r.status_code == 500


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
