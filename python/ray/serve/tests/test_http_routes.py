import time

import httpx
import pytest
from fastapi import FastAPI, Request
from starlette.responses import RedirectResponse

import ray
from ray import serve
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.test_utils import get_application_url


def test_path_validation(serve_instance):
    @serve.deployment
    class D:
        pass

    # Path prefix must start with /.
    with pytest.raises(ValueError):
        serve.run(D.bind(), route_prefix="hello")

    # Path prefix must not end with / unless it's the root.
    with pytest.raises(ValueError):
        serve.run(D.bind(), route_prefix="/hello/")

    # Wildcards not allowed with new ingress support.
    with pytest.raises(ValueError):
        serve.run(D.bind(), route_prefix="/{hello}")


def test_routes_healthz(serve_instance):
    # Should return 503 until there are any routes populated.
    resp = httpx.get("http://localhost:8000/-/healthz")
    assert resp.status_code == 503
    assert resp.text == "Route table is not populated yet."

    @serve.deployment
    class D1:
        def __call__(self, *args):
            return "hi"

    # D1 not exposed over HTTP so should still return 503.
    serve.run(D1.bind(), route_prefix=None)
    resp = httpx.get("http://localhost:8000/-/healthz")
    assert resp.status_code == 503
    assert resp.text == "Route table is not populated yet."

    # D1 exposed over HTTP, should return 200 OK.
    serve.run(D1.bind(), route_prefix="/")
    resp = httpx.get("http://localhost:8000/-/healthz")
    assert resp.status_code == 200
    assert resp.text == "success"


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

    routes = httpx.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 2, routes

    app1_url = get_application_url(app_name="app1")
    app2_url = get_application_url(app_name="app2")

    assert httpx.get(app1_url).text == "D1"
    assert httpx.get(app1_url).status_code == 200
    assert httpx.get(app2_url).text == "D2"
    assert httpx.get(app2_url).status_code == 200
    assert httpx.get("http://localhost:8000/not_exist").status_code == 404

    app1_url = get_application_url(app_name="app1", exclude_route_prefix=True)
    app2_url = get_application_url(app_name="app2", exclude_route_prefix=True)

    assert httpx.get(f"{app1_url}/").status_code == 404
    assert httpx.get(f"{app2_url}/").status_code == 404


def test_deployment_without_route(serve_instance):
    @serve.deployment
    class D:
        def __call__(self, *args):
            return "1"

    serve.run(D.bind(), route_prefix=None)
    routes = httpx.get("http://localhost:8000/-/routes")
    assert len(routes.json()) == 0

    # make sure the deployment is not exposed under the default route
    r = httpx.get("http://localhost:8000/")
    assert r.status_code == 404


def test_deployment_options_default_route(serve_instance):
    @serve.deployment
    class D1:
        pass

    serve.run(D1.bind())
    url = get_application_url(exclude_route_prefix=True)
    routes = httpx.get(f"{url}/-/routes").json()
    assert len(routes) == 1
    assert "/" in routes, routes
    assert routes["/"] == SERVE_DEFAULT_APP_NAME


def test_path_prefixing_1(serve_instance):
    def check_req(subpath, app_name, text=None, status=None):
        url = get_application_url(app_name=app_name, exclude_route_prefix=True)
        r = httpx.get(f"{url}{subpath}")
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
    check_req("/", "app1", status=404)
    check_req("/hello", "app1", text="1")
    check_req("/hello/", "app1", text="1")
    check_req("/hello/a", "app1", text="1")

    @serve.deployment
    class D2:
        def __call__(self, *args):
            return "2"

    serve.run(D2.bind(), route_prefix="/", name="app2")
    check_req("/hello/", "app1", text="1")
    check_req("/hello/a", "app1", text="1")
    check_req("/", "app2", text="2")
    check_req("/a", "app2", text="2")

    @serve.deployment
    class D3:
        def __call__(self, *args):
            return "3"

    serve.run(D3.bind(), route_prefix="/hello/world", name="app3")
    check_req("/hello/", "app1", text="1")
    check_req("/", "app2", text="2")
    check_req("/hello/world/", "app3", text="3")

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
    check_req("/hello/", "app1") == "1"
    check_req("/", "app2") == "2"
    check_req("/hello/world/", "app3") == "3"
    check_req("/hello/world/again/", "app4") == "4"
    check_req("/hello/world/again/hi", "app4") == '"hi"'


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

    url = get_application_url(exclude_route_prefix=True)
    r = httpx.get(f"{url}{route_prefix}redirect", follow_redirects=True)
    assert r.status_code == 200
    assert len(r.history) == 1
    assert r.json() == "hello from /"

    r = httpx.get(f"{url}{route_prefix}redirect2", follow_redirects=True)
    assert r.status_code == 200
    assert len(r.history) == 2
    assert r.json() == "hello from /"


def test_default_error_handling(serve_instance):
    @serve.deployment
    def f():
        _ = 1 / 0

    serve.run(f.bind())
    url = get_application_url(exclude_route_prefix=True)
    # Error is raised when the request reaches the deployed replica.
    r = httpx.get(f"{url}/f")
    assert r.status_code == 500
    assert r.text == "Internal Server Error"

    @ray.remote(num_cpus=0)
    def intentional_kill(actor_handle):
        ray.kill(actor_handle, no_restart=False)

    @serve.deployment
    def h():
        ray.get(intentional_kill.remote(ray.get_runtime_context().current_actor))
        time.sleep(100)  # Don't return here to leave time for actor exit.

    serve.run(h.bind())
    # Error is raised before the request reaches the deployed replica as the replica does not exist.
    r = httpx.get("http://localhost:8000/h")
    assert r.status_code == 500


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
