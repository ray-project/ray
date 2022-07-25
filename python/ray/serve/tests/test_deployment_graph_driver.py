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
    async def predict(self, inp, route_path=None):
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
    assert response.json()["paths"]["/{path_name}"]["get"]["parameters"][0] == {
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
    assert response.json()["paths"]["/{path_name}"]["get"]["requestBody"] == {
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


def test_multi_dag_wrong_inputs(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self):
            return "D1"

    @serve.deployment
    class D2:
        def __call__(self):
            return "D2"

    with pytest.raises(RuntimeError):
        dag = DAGDriver.bind(
            D1.bind(),
            D2.bind(),
            dags_routes=[
                "/D1",
            ],
        )
        serve.run(dag)

    with pytest.raises(RuntimeError):
        dag = DAGDriver.bind(
            D1.bind(),
            D2.bind(),
            dags_routes=["/D1", "/D2", "/D3"],
        )
        serve.run(dag)

    with pytest.raises(RuntimeError):
        dag = DAGDriver.bind(
            D1.bind(),
            D2.bind(),
            dags_routes=["/D1", "/D1"],
        )
        serve.run(dag)

    with pytest.raises(RuntimeError):
        dag = DAGDriver.bind(
            D1.bind(),
            D2.bind(),
            dags_routes=["/D1", 2],
        )
        serve.run(dag)


def test_multi_dag(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self):
            return "D1"

    @serve.deployment
    def D2():
        return "D2"

    dag = DAGDriver.bind(
        D1.bind(),
        D2.bind(),
        dags_routes=["/my_D1", "/my_D2"],
    )
    handle = serve.run(dag)

    assert ray.get(handle.predict.remote(route_path="/my_D1")) == "D1"
    assert ray.get(handle.predict.remote(route_path="/my_D2")) == "D2"
    assert requests.post("http://127.0.0.1:8000/my_D1").json() == "D1"
    assert requests.post("http://127.0.0.1:8000/my_D2").json() == "D2"


def test_multi_dag_with_reconfigure(serve_instance):
    @serve.deployment
    class D1:
        def __call__(self):
            return "D1"

    @serve.deployment
    def D2():
        return "D2"

    dag = DAGDriver.options(user_config={"DAG_ROUTES": ["/my_D11", "/my_D2"]}).bind(
        D1.bind(),
        D2.bind(),
        dags_routes=["/my_D1", "/my_D2"],
    )
    handle = serve.run(dag)

    assert ray.get(handle.predict.remote(route_path="/my_D11")) == "D1"
    assert ray.get(handle.predict.remote(route_path="/my_D2")) == "D2"
    assert requests.post("http://127.0.0.1:8000/my_D11").json() == "D1"
    assert requests.post("http://127.0.0.1:8000/my_D2").json() == "D2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
