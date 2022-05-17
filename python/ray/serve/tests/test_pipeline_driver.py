import contextlib
import io
import sys
import numpy as np
from pydantic import BaseModel

import pytest
import requests
import starlette.requests
from starlette.testclient import TestClient

from ray.serve.drivers import DAGDriver, SimpleSchemaIngress, load_http_adapter
from ray.serve.http_adapters import json_request
from ray.experimental.dag.input_node import InputNode
from ray import serve
import ray
from ray._private.test_utils import wait_for_condition


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


def test_dag_driver_custom_pydantic_schema(serve_instance):
    with InputNode() as inp:
        dag = echo.bind(inp)

    handle = serve.run(DAGDriver.bind(dag, http_adapter=MyType))
    assert ray.get(handle.predict.remote(MyType(a=1, b="str"))) == MyType(a=1, b="str")

    resp = requests.post("http://127.0.0.1:8000/", json={"a": 1, "b": "str"})
    print(resp.text)
    resp.raise_for_status()
    assert resp.json() == {"a": 1, "b": "str"}


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


@serve.deployment
def return_np_int(_):
    return [np.int64(42)]


def test_driver_np_serializer(serve_instance):
    # https://github.com/ray-project/ray/pull/24215#issuecomment-1115237058
    with InputNode() as inp:
        dag = DAGDriver.bind(return_np_int.bind(inp))
    serve.run(dag)
    assert requests.get("http://127.0.0.1:8000/").json() == [42]


def test_dag_driver_sync_warning(serve_instance):
    with InputNode() as inp:
        dag = echo.bind(inp)

    log_file = io.StringIO()
    with contextlib.redirect_stderr(log_file):

        handle = serve.run(DAGDriver.bind(dag))
        assert ray.get(handle.predict.remote(42)) == 42

        def wait_for_request_success_log():
            lines = log_file.getvalue().splitlines()
            for line in lines:
                if "DAGDriver" in line and "HANDLE predict OK" in line:
                    return True
            return False

        wait_for_condition(wait_for_request_success_log)

        assert (
            "You are retrieving a sync handle inside an asyncio loop."
            not in log_file.getvalue()
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
