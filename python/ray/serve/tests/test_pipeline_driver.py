import sys

import pytest
import requests
import starlette.requests
from starlette.testclient import TestClient

from ray.serve.drivers import DAGDriver, SimpleSchemaIngress, load_http_adapter
from ray.serve.http_adapters import json_request
from ray.experimental.dag.input_node import InputNode
from ray import serve
import ray


def my_resolver(a: int):
    return a


def test_loading_check():
    with pytest.raises(ValueError, match="callable"):
        load_http_adapter(["not function"])
    with pytest.raises(ValueError, match="type annotated"):

        def func(a):
            return a

        load_http_adapter(func)

    loaded_my_resolver = load_http_adapter(
        "ray.serve.tests.test_pipeline_driver.my_resolver"
    )
    assert (loaded_my_resolver == my_resolver) or (
        loaded_my_resolver.__code__.co_code == my_resolver.__code__.co_code
    )


def test_unit_schema_injection():
    class Impl(SimpleSchemaIngress):
        async def predict(self, inp):
            return inp

    async def resolver(my_custom_param: int):
        return my_custom_param

    server = Impl(http_adapter=resolver)
    client = TestClient(server.app)

    response = client.post("/")
    assert response.status_code == 422

    response = client.post("/?my_custom_param=1")
    assert response.status_code == 200
    assert response.text == "1"

    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json()["paths"]["/"]["get"]["parameters"][0] == {
        "required": True,
        "schema": {"title": "My Custom Param", "type": "integer"},
        "name": "my_custom_param",
        "in": "query",
    }


@serve.deployment
def echo(inp):
    # FastAPI can't handle this.
    if isinstance(inp, starlette.requests.Request):
        return "starlette!"
    return inp


def test_dag_driver_default(serve_instance):
    with InputNode() as inp:
        dag = echo.bind(inp)

    handle = serve.run(DAGDriver.bind(dag))
    assert ray.get(handle.predict.remote(42)) == 42

    resp = requests.post("http://127.0.0.1:8000/", json={"array": [1]})
    print(resp.text)

    resp.raise_for_status()
    assert resp.json() == "starlette!"


async def resolver(my_custom_param: int):
    return my_custom_param


def test_dag_driver_custom_schema(serve_instance):
    with InputNode() as inp:
        dag = echo.bind(inp)

    handle = serve.run(DAGDriver.bind(dag, http_adapter=resolver))
    assert ray.get(handle.predict.remote(42)) == 42

    resp = requests.get("http://127.0.0.1:8000/?my_custom_param=100")
    print(resp.text)
    resp.raise_for_status()
    assert resp.json() == 100


@serve.deployment
def combine(*args):
    return list(args)


def test_dag_driver_partial_input(serve_instance):
    with InputNode() as inp:
        dag = DAGDriver.bind(
            combine.bind(echo.bind(inp[0]), echo.bind(inp[1]), echo.bind(inp[2])),
            http_adapter=json_request,
        )
    handle = serve.run(dag)
    assert ray.get(handle.predict.remote([1, 2, [3, 4]])) == [1, 2, [3, 4]]
    assert ray.get(handle.predict.remote(1, 2, [3, 4])) == [1, 2, [3, 4]]

    resp = requests.post("http://127.0.0.1:8000/", json=[1, 2, [3, 4]])
    print(resp.text)
    resp.raise_for_status()
    assert resp.json() == [1, 2, [3, 4]]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
