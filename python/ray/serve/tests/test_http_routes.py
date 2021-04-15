from fastapi import FastAPI
import pytest
import requests

from ray import serve
from ray.serve.constants import ALL_HTTP_METHODS


def test_path_validation(serve_instance):
    # Path prefix must start with /.
    with pytest.raises(ValueError):

        @serve.deployment(route_prefix="hello")
        class D1:
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

    # Reject duplicate route.
    with pytest.raises(ValueError):
        D4.options(name="test2").deploy()


def test_routes_endpoint(serve_instance):
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
    assert routes["/D1"] == ["D1", ["GET", "POST"]], routes
    assert "/hello/world" in routes, routes
    assert routes["/hello/world"] == ["D2", ["GET", "POST"]], routes

    D1.delete()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1, routes
    assert "/hello/world" in routes, routes
    assert routes["/hello/world"] == ["D2", ["GET", "POST"]], routes

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
    assert routes["/hello"] == ["D3", ALL_HTTP_METHODS], routes


def test_deployment_options_default_route(serve_instance):
    @serve.deployment(name="1")
    class D1:
        pass

    D1.deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1
    assert "/1" in routes, routes
    assert routes["/1"] == ["1", ["GET", "POST"]]

    D1.options(name="2").deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 2
    assert "/1" in routes, routes
    assert routes["/1"] == ["1", ["GET", "POST"]]
    assert "/2" in routes, routes
    assert routes["/2"] == ["2", ["GET", "POST"]]


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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
