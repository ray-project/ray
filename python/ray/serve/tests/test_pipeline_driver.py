import sys

import pytest
import requests
import starlette.requests
from starlette.testclient import TestClient

from ray.serve.drivers import DAGDriver, SimpleSchemaIngress, load_input_schema
from ray.experimental.dag.input_node import InputNode
from ray import serve
import ray


def my_resolver(a: int):
    return a


def test_loading_check():
    with pytest.raises(ValueError, match="callable"):
        load_input_schema(["not function"])
    with pytest.raises(ValueError, match="type annotated"):

        def func(a):
            return a

        load_input_schema(func)

    loaded_my_resolver = load_input_schema(
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

    server = Impl(input_schema=resolver)
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

    handle = serve.run(DAGDriver.bind(dag, input_schema=resolver))
    assert ray.get(handle.predict.remote(42)) == 42

    resp = requests.get("http://127.0.0.1:8000/?my_custom_param=100")
    print(resp.text)
    resp.raise_for_status()
    assert resp.json() == 100


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
