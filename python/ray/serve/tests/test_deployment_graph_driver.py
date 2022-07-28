import contextlib
import io
import sys
import numpy as np
from pydantic import BaseModel

import pytest
import requests
import starlette.requests
from starlette.testclient import TestClient

from ray.serve.drivers import DAGDriver, SimpleSchemaIngress, _load_http_adapter
from ray.serve.http_adapters import json_request
from ray.serve.dag import InputNode
from ray import serve
import ray
from ray._private.test_utils import wait_for_condition


def my_resolver(a: int):
    return a


def test_loading_check():
    with pytest.raises(ValueError, match="callable"):
        _load_http_adapter(["not function"])
    with pytest.raises(ValueError, match="type annotated"):

        def func(a):
            return a

        _load_http_adapter(func)

    loaded_my_resolver = _load_http_adapter(
        "ray.serve.tests.test_deployment_graph_driver.my_resolver"
    )
    assert (loaded_my_resolver == my_resolver) or (
        loaded_my_resolver.__code__.co_code == my_resolver.__code__.co_code
    )


class EchoIngress(SimpleSchemaIngress):
    async def predict(self, inp):
        return inp


def test_unit_schema_injection():
    async def resolver(my_custom_param: int):
        return my_custom_param

    server = EchoIngress(http_adapter=resolver)
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


class MyType(BaseModel):
    a: int
    b: str


def test_unit_pydantic_class_adapter():

    server = EchoIngress(http_adapter=MyType)
    client = TestClient(server.app)
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json()["paths"]["/"]["get"]["requestBody"] == {
        "content": {
            "application/json": {"schema": {"$ref": "#/components/schemas/MyType"}}
        },
        "required": True,
    }


@serve.deployment
def echo(inp):
    # FastAPI can't handle this.
    if isinstance(inp, starlette.requests.Request):
        return "starlette!"
    return inp


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


def test_multi_dag(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self, input):
            return input

    @serve.deployment
    def D2():
        return "D2"

    dag = DAGDriver.bind(
        {"/my_D1": D1.bind(), "/my_D2": D2.bind()}, http_adapter=json_resolver
    )
    handle = serve.run(dag)

    assert ray.get(handle.predict_with_route.remote("/my_D1", 1)) == 1
    assert ray.get(handle.predict_with_route.remote(route_path="/my_D2")) == "D2"
    assert requests.post("http://127.0.0.1:8000/my_D1", json=1).json() == 1
    assert requests.post("http://127.0.0.1:8000/my_D2").json() == "D2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
