from fastapi import FastAPI
import requests
import pytest

from ray import serve


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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
