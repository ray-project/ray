from fastapi import FastAPI
import pytest
import requests

from ray import serve
from ray.serve.constants import ALL_HTTP_METHODS


def test_path_validation(serve_instance):
    # Path prefix must start with /.
    with pytest.raises(ValueError):

        @serve.ingress(path_prefix="hello")
        class D1:
            pass

    # Wildcards not allowed with new ingress support.
    with pytest.raises(ValueError):

        @serve.ingress(path_prefix="/{wildcard}")
        class D2:
            pass

    @serve.deployment("test")
    @serve.ingress(path_prefix="/duplicate")
    class D3:
        pass

    D3.deploy()

    # Reject duplicate route.
    with pytest.raises(ValueError):
        D3.options(name="test2").deploy()


def test_routes_endpoint(serve_instance):
    @serve.deployment("D1")
    @serve.ingress
    class D1:
        pass

    @serve.deployment("D2")
    @serve.ingress(path_prefix="/hello/world")
    class D2:
        pass

    D1.deploy()
    D2.deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()

    assert len(routes) == 2
    assert routes["/D1"] == ["D1", ["GET", "POST"]]
    assert routes["/hello/world"] == ["D2", ["GET", "POST"]]

    D1.delete()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1
    assert routes["/hello/world"] == ["D2", ["GET", "POST"]]

    D2.delete()
    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 0

    app = FastAPI()

    @serve.deployment("D3")
    @serve.ingress(app, path_prefix="/hello")
    class D3:
        pass

    D3.deploy()

    routes = requests.get("http://localhost:8000/-/routes").json()
    assert len(routes) == 1
    assert routes["/hello"] == ["D3", ALL_HTTP_METHODS]


def test_path_prefixing(serve_instance):
    def req(subpath):
        return requests.get(f"http://localhost:8000{subpath}").text

    @serve.deployment("D1")
    @serve.ingress(path_prefix="/")
    class D1:
        def __call__(self, *args):
            return "1"

    D1.deploy()
    assert req("/") == "1"
    assert req("/a") != "1"

    @serve.deployment("D2")
    @serve.ingress(path_prefix="/hello")
    class D2:
        def __call__(self, *args):
            return "2"

    D2.deploy()
    assert req("/") == "1"
    assert req("/hello") == "2"

    @serve.deployment("D3")
    @serve.ingress(path_prefix="/hello/world")
    class D3:
        def __call__(self, *args):
            return "3"

    D3.deploy()
    assert req("/") == "1"
    assert req("/hello") == "2"
    assert req("/hello/world") == "3"

    app = FastAPI()

    @serve.deployment("D4")
    @serve.ingress(app, path_prefix="/hello/world/again")
    class D4:
        @app.get("/")
        def root(self):
            return "4"

        @app.get("/{p}")
        def subpath(self, p: str):
            return p

    D4.deploy()
    assert req("/") == "1"
    assert req("/hello") == "2"
    assert req("/hello/world") == "3"
    # TODO(edoakes): these fail with an obscure error message right now.
    # assert req("/hello/world/again") == "4"
    # assert req("/hello/world/again/") == "4"
    assert req("/hello/world/again/hi") == "\"hi\""


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
