from fastapi import FastAPI
import requests
import pytest
import inspect

from ray import serve
from ray.serve.utils import make_fastapi_class_based_view


def test_fastapi_function(serve_instance):
    client = serve_instance
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.ingress(app)
    class FastAPIApp:
        pass

    client.deploy("f", FastAPIApp)

    resp = requests.get(f"http://localhost:8000/f/100")
    assert resp.json() == {"result": 100}

    resp = requests.get(f"http://localhost:8000/f/not-number")
    assert resp.status_code == 422  # Unprocessable Entity
    assert resp.json()["detail"][0]["type"] == "type_error.integer"


def test_ingress_prefix(serve_instance):
    client = serve_instance
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.ingress(app, path_prefix="/api")
    class App:
        pass

    client.deploy("f", App)

    resp = requests.get(f"http://localhost:8000/api/100")
    assert resp.json() == {"result": 100}


def test_class_based_view(serve_instance):
    client = serve_instance
    app = FastAPI()

    @app.get("/other")
    def hello():
        return "hello"

    @serve.ingress(app)
    class A:
        def __init__(self):
            self.val = 1

        @app.get("/calc/{i}")
        def b(self, i: int):
            return i + self.val

        @app.post("/calc/{i}")
        def c(self, i: int):
            return i - self.val

    client.deploy("f", A)
    resp = requests.get(f"http://localhost:8000/f/calc/41")
    assert resp.json() == 42
    resp = requests.post(f"http://localhost:8000/f/calc/41")
    assert resp.json() == 40
    resp = requests.get(f"http://localhost:8000/f/other")
    assert resp.json() == "hello"


def test_make_fastapi_cbv_util():
    app = FastAPI()

    class A:
        @app.get("/{i}")
        def b(self, i: int):
            pass

    # before, "self" is treated as a query params
    assert app.routes[-1].endpoint == A.b
    assert app.routes[-1].dependant.query_params[0].name == "self"
    assert len(app.routes[-1].dependant.dependencies) == 0

    make_fastapi_class_based_view(app, A)

    # after, "self" is treated as a dependency instead of query params
    assert app.routes[-1].endpoint == A.b
    assert len(app.routes[-1].dependant.query_params) == 0
    assert len(app.routes[-1].dependant.dependencies) == 1
    self_dep = app.routes[-1].dependant.dependencies[0]
    assert self_dep.name == "self"
    assert inspect.isfunction(self_dep.call)
    assert "get_current_servable" in str(self_dep.call)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
