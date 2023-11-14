import sys

import pytest
import requests
import starlette.requests

from ray import serve
from ray.serve.dag import InputNode
from ray.serve.drivers import DAGDriver
from ray.serve.drivers_utils import load_http_adapter


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
        "ray.serve.tests.test_deployment_graph_driver.my_resolver"
    )
    assert (loaded_my_resolver == my_resolver) or (
        loaded_my_resolver.__code__.co_code == my_resolver.__code__.co_code
    )


@serve.deployment
def echo(inp):
    # FastAPI can't handle this.
    if isinstance(inp, starlette.requests.Request):
        return "starlette!"
    return inp


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


def test_multi_dag(serve_instance):
    """Test multi dags within dag deployment"""

    @serve.deployment
    class D1:
        def forward(self, *args):
            return "D1"

    @serve.deployment
    class D2:
        def forward(self, *args):
            return "D2"

    @serve.deployment
    class D3:
        def __call__(self, *args):
            return "D3"

    @serve.deployment
    def D4(*args):
        return "D4"

    d1 = D1.bind()
    d2 = D2.bind()
    d3 = D3.bind()
    d4 = D4.bind()
    dag = DAGDriver.bind(
        {
            "/my_D1": d1.forward.bind(),
            "/my_D2": d2.forward.bind(),
            "/my_D3": d3,
            "/my_D4": d4,
        }
    )
    handle = serve.run(dag)

    for i in range(1, 5):
        assert handle.predict_with_route.remote(f"/my_D{i}").result() == f"D{i}"
        assert requests.post(f"http://127.0.0.1:8000/my_D{i}", json=1).json() == f"D{i}"
        assert requests.get(f"http://127.0.0.1:8000/my_D{i}", json=1).json() == f"D{i}"


def test_multi_dag_with_inputs(serve_instance):
    @serve.deployment
    class D1:
        def forward(self, input):
            return input

    @serve.deployment
    class D2:
        def forward(self, input1, input2):
            return input1 + input2

    @serve.deployment
    def D3(input):
        return input

    d1 = D1.bind()
    d2 = D2.bind()

    with InputNode() as dag_input:
        dag = DAGDriver.bind(
            {
                "/my_D1": d1.forward.bind(dag_input),
                "/my_D2": d2.forward.bind(dag_input[0], dag_input[1]),
                "/my_D3": D3.bind(dag_input),
            },
            http_adapter=json_resolver,
        )
        handle = serve.run(dag)

    assert handle.predict_with_route.remote("/my_D1", 1).result() == 1
    assert handle.predict_with_route.remote("/my_D2", 10, 2).result() == 12
    assert handle.predict_with_route.remote("/my_D3", 100).result() == 100
    assert requests.post("http://127.0.0.1:8000/my_D1", json=1).json() == 1
    assert requests.post("http://127.0.0.1:8000/my_D2", json=[1, 2]).json() == 3
    assert requests.post("http://127.0.0.1:8000/my_D3", json=100).json() == 100
    assert requests.get("http://127.0.0.1:8000/my_D1", json=1).json() == 1
    assert requests.get("http://127.0.0.1:8000/my_D2", json=[1, 2]).json() == 3
    assert requests.get("http://127.0.0.1:8000/my_D3", json=100).json() == 100


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
