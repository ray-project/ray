import time

import pytest
import requests
from ray.serve.drivers import DAGDriver
from fastapi import FastAPI, Request
from starlette.responses import RedirectResponse

import ray
from ray import serve


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

    D4.deploy()

    # Allow duplicate route.
    D4.options(name="test2").deploy()


def test_routes_healthz(serve_instance):
    resp = requests.get("http://localhost:8000/-/healthz")
    assert resp.status_code == 200
    assert resp.content == b"success"


def test_routes_endpoint_legacy(serve_instance):
    @serve.deployment
    class D1:
        pass

    @serve.deployment(route_prefix="/hello/world")
    class D2:
        pass

    D1.deploy()
    D2.deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()

    assert len(routes) == 2, routes
    assert "/D1" in routes, routes
    assert routes["/D1"] == "D1", routes
    assert "/hello/world" in routes, routes
    assert routes["/hello/world"] == "D2", routes

    D1.delete()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1, routes
    assert "/hello/world" in routes, routes
    assert routes["/hello/world"] == "D2", routes

    D2.delete()
    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 0, routes

    app = FastAPI()

    @serve.deployment(route_prefix="/hello")
    @serve.ingress(app)
    class D3:
        pass

    D3.deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1, routes
    assert "/hello" in routes, routes
    assert routes["/hello"] == "D3", routes


def test_routes_endpoint(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self):
            return "D1"

    @serve.deployment
    class D2:
        def __call__(self):
            return "D2"

    dag = DAGDriver.bind({"/D1": D1.bind(), "/hello/world": D2.bind()})
    serve.run(dag)

    routes = requests.get("http://localhost:8000/-/routes").json()

    assert len(routes) == 1, routes
    assert "/" in routes, routes

    assert requests.get("http://localhost:8000/D1").json() == "D1"
    assert requests.get("http://localhost:8000/D1").status_code == 200
    assert requests.get("http://localhost:8000/hello/world").json() == "D2"
    assert requests.get("http://localhost:8000/hello/world").status_code == 200


def test_deployment_without_route(serve_instance):
    @serve.deployment(route_prefix=None)
    class D:
        def __call__(self, *args):
            return "1"

    D.deploy()
    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 0

    # make sure the deployment is not exposed under the default route
    r = requests.get(f"http://localhost:8000/{D.name}")
    assert r.status_code == 404


def test_deployment_options_default_route(serve_instance):
    @serve.deployment(name="1")
    class D1:
        pass

    D1.deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1
    assert "/1" in routes, routes
    assert routes["/1"] == "1"

    D1.options(name="2").deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 2
    assert "/1" in routes, routes
    assert routes["/1"] == "1"
    assert "/2" in routes, routes
    assert routes["/2"] == "2"


def test_path_prefixing(serve_instance):
    def check_req(subpath, text=None, status=None):
        r = requests.get(f"http://localhost:8000{subpath}")
        if text is not None:
            assert r.text == text, f"{r.text} != {text}"
        if status is not None:
            assert r.status_code == status, f"{r.status_code} != {status}"

        return r

    @serve.deployment(route_prefix="/hello")
    class D1:
        def __call__(self, *args):
            return "1"

    D1.deploy()
    check_req("/", status=404)
    check_req("/hello", text="1")
    check_req("/hello/", text="1")
    check_req("/hello/a", text="1")

    @serve.deployment(route_prefix="/")
    class D2:
        def __call__(self, *args):
            return "2"

    D2.deploy()
    check_req("/hello/", text="1")
    check_req("/hello/a", text="1")
    check_req("/", text="2")
    check_req("/a", text="2")

    @serve.deployment(route_prefix="/hello/world")
    class D3:
        def __call__(self, *args):
            return "3"

    D3.deploy()
    check_req("/hello/", text="1")
    check_req("/", text="2")
    check_req("/hello/world/", text="3")

    app = FastAPI()

    @serve.deployment(route_prefix="/hello/world/again")
    @serve.ingress(app)
    class D4:
        @app.get("/")
        def root(self):
            return 4

        @app.get("/{p}")
        def subpath(self, p: str):
            return p

    D4.deploy()
    check_req("/hello/") == "1"
    check_req("/") == "2"
    check_req("/hello/world/") == "3"
    check_req("/hello/world/again/") == "4"
    check_req("/hello/world/again/hi") == '"hi"'


def test_multi_dag_with_wrong_route(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self):
            return "D1"

    @serve.deployment
    class D2:
        def __call__(self):
            return "D2"

    dag = DAGDriver.bind({"/D1": D1.bind(), "/hello/world": D2.bind()})

    serve.run(dag)

    assert requests.get("http://localhost:8000/D1").status_code == 200
    assert requests.get("http://localhost:8000/hello/world").status_code == 200
    assert requests.get("http://localhost:8000/not_exist").status_code == 404
    assert requests.get("http://localhost:8000/").status_code == 404


@pytest.mark.parametrize("base_path", ["", "subpath"])
def test_redirect(serve_instance, base_path):
    app = FastAPI()

    route_prefix = f"/{base_path}"

    @serve.deployment(route_prefix=route_prefix)
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

    D.deploy()

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
    assert "retries" in r.text, r.text


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
