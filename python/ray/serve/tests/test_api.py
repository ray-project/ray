import asyncio
import os

import requests
import pytest
import starlette.responses

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.application import Application


@serve.deployment()
def sync_d():
    return "sync!"


@serve.deployment()
async def async_d():
    return "async!"


@serve.deployment
class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1
        return {"count": self.count}


@serve.deployment
class AsyncCounter:
    async def __init__(self):
        await asyncio.sleep(0.01)
        self.count = 0

    async def __call__(self):
        self.count += 1
        await asyncio.sleep(0.01)
        return {"count": self.count}


def test_e2e(serve_instance):
    @serve.deployment(name="api")
    def function(starlette_request):
        return {"method": starlette_request.method}

    function.deploy()

    resp = requests.get("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "GET"

    resp = requests.post("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "POST"


def test_starlette_response(serve_instance):
    @serve.deployment(name="basic")
    def basic(_):
        return starlette.responses.Response("Hello, world!", media_type="text/plain")

    basic.deploy()
    assert requests.get("http://127.0.0.1:8000/basic").text == "Hello, world!"

    @serve.deployment(name="html")
    def html(_):
        return starlette.responses.HTMLResponse(
            "<html><body><h1>Hello, world!</h1></body></html>"
        )

    html.deploy()
    assert (
        requests.get("http://127.0.0.1:8000/html").text
        == "<html><body><h1>Hello, world!</h1></body></html>"
    )

    @serve.deployment(name="plain_text")
    def plain_text(_):
        return starlette.responses.PlainTextResponse("Hello, world!")

    plain_text.deploy()
    assert requests.get("http://127.0.0.1:8000/plain_text").text == "Hello, world!"

    @serve.deployment(name="json")
    def json(_):
        return starlette.responses.JSONResponse({"hello": "world"})

    json.deploy()
    assert requests.get("http://127.0.0.1:8000/json").json()["hello"] == "world"

    @serve.deployment(name="redirect")
    def redirect(_):
        return starlette.responses.RedirectResponse(url="http://127.0.0.1:8000/basic")

    redirect.deploy()
    assert requests.get("http://127.0.0.1:8000/redirect").text == "Hello, world!"

    @serve.deployment(name="streaming")
    def streaming(_):
        async def slow_numbers():
            for number in range(1, 4):
                yield str(number)
                await asyncio.sleep(0.01)

        return starlette.responses.StreamingResponse(
            slow_numbers(), media_type="text/plain", status_code=418
        )

    streaming.deploy()
    resp = requests.get("http://127.0.0.1:8000/streaming")
    assert resp.text == "123"
    assert resp.status_code == 418


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_function_no_params(serve_instance, use_async):
    serve.start()

    if use_async:
        expected_output = "async!"
        deployment_cls = async_d
    else:
        expected_output = "sync!"
        deployment_cls = sync_d
    deployment_cls.deploy()

    assert (
        requests.get(f"http://localhost:8000/{deployment_cls.name}").text
        == expected_output
    )
    assert ray.get(deployment_cls.get_handle().remote()) == expected_output


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_function_no_params_call_with_param(serve_instance, use_async):
    serve.start()

    if use_async:
        expected_output = "async!"
        deployment_cls = async_d
    else:
        expected_output = "sync!"
        deployment_cls = sync_d
    deployment_cls.deploy()

    assert (
        requests.get(f"http://localhost:8000/{deployment_cls.name}").text
        == expected_output
    )
    with pytest.raises(
        TypeError, match=r"\(\) takes 0 positional arguments but 1 was given"
    ):
        assert ray.get(deployment_cls.get_handle().remote(1)) == expected_output

    with pytest.raises(TypeError, match=r"\(\) got an unexpected keyword argument"):
        assert ray.get(deployment_cls.get_handle().remote(key=1)) == expected_output


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_class_no_params(serve_instance, use_async):
    serve.start()
    if use_async:
        deployment_cls = AsyncCounter
    else:
        deployment_cls = Counter
    deployment_cls.deploy()

    assert requests.get(f"http://127.0.0.1:8000/{deployment_cls.name}").json() == {
        "count": 1
    }
    assert requests.get(f"http://127.0.0.1:8000/{deployment_cls.name}").json() == {
        "count": 2
    }
    assert ray.get(deployment_cls.get_handle().remote()) == {"count": 3}


def test_user_config(serve_instance):
    @serve.deployment("counter", num_replicas=2, user_config={"count": 123, "b": 2})
    class Counter:
        def __init__(self):
            self.count = 10

        def __call__(self, *args):
            return self.count, os.getpid()

        def reconfigure(self, config):
            self.count = config["count"]

    Counter.deploy()
    handle = Counter.get_handle()

    def check(val, num_replicas):
        pids_seen = set()
        for i in range(100):
            result = ray.get(handle.remote())
            if str(result[0]) != val:
                return False
            pids_seen.add(result[1])
        return len(pids_seen) == num_replicas

    wait_for_condition(lambda: check("123", 2))

    Counter = Counter.options(num_replicas=3)
    Counter.deploy()
    wait_for_condition(lambda: check("123", 3))

    Counter = Counter.options(user_config={"count": 456})
    Counter.deploy()
    wait_for_condition(lambda: check("456", 3))


def test_reject_duplicate_route(serve_instance):
    @serve.deployment(name="A", route_prefix="/api")
    class A:
        pass

    A.deploy()
    with pytest.raises(ValueError):
        A.options(name="B").deploy()


def test_scaling_replicas(serve_instance):
    @serve.deployment(name="counter", num_replicas=2)
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, _):
            self.count += 1
            return self.count

    Counter.deploy()

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/counter").json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    Counter.options(num_replicas=1).deploy()

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/counter").json()
        counter_result.append(resp)
    # Give some time for a replica to spin down. But majority of the request
    # should be served by the only remaining replica.
    assert max(counter_result) - min(counter_result) > 6


def test_delete_deployment(serve_instance):
    @serve.deployment(name="delete")
    def function(_):
        return "hello"

    function.deploy()

    assert requests.get("http://127.0.0.1:8000/delete").text == "hello"

    function.delete()

    @serve.deployment(name="delete")
    def function2(_):
        return "olleh"

    function2.deploy()

    wait_for_condition(
        lambda: requests.get("http://127.0.0.1:8000/delete").text == "olleh", timeout=6
    )


@pytest.mark.parametrize("blocking", [False, True])
def test_delete_deployment_group(serve_instance, blocking):
    @serve.deployment(num_replicas=1)
    def f(*args):
        return "got f"

    @serve.deployment(num_replicas=2)
    def g(*args):
        return "got g"

    # Check redeploying after deletion
    for _ in range(2):
        f.deploy()
        g.deploy()

        wait_for_condition(
            lambda: requests.get("http://127.0.0.1:8000/f").text == "got f", timeout=5
        )
        wait_for_condition(
            lambda: requests.get("http://127.0.0.1:8000/g").text == "got g", timeout=5
        )

        # Check idempotence
        for _ in range(2):

            serve_instance.delete_deployments(["f", "g"], blocking=blocking)

            wait_for_condition(
                lambda: requests.get("http://127.0.0.1:8000/f").status_code == 404,
                timeout=5,
            )
            wait_for_condition(
                lambda: requests.get("http://127.0.0.1:8000/g").status_code == 404,
                timeout=5,
            )


def test_starlette_request(serve_instance):
    @serve.deployment(name="api")
    async def echo_body(starlette_request):
        data = await starlette_request.body()
        return data

    echo_body.deploy()

    # Long string to test serialization of multiple messages.
    UVICORN_HIGH_WATER_MARK = 65536  # max bytes in one message
    long_string = "x" * 10 * UVICORN_HIGH_WATER_MARK

    resp = requests.post("http://127.0.0.1:8000/api", data=long_string).text
    assert resp == long_string


def test_start_idempotent(serve_instance):
    @serve.deployment(name="start")
    def func(*args):
        pass

    func.deploy()

    assert "start" in serve.list_deployments()
    serve.start(detached=True)
    serve.start()
    serve.start(detached=True)
    serve.start()
    assert "start" in serve.list_deployments()


def test_shutdown_destructor(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment
    class A:
        def __del__(self):
            signal.send.remote()

    A.deploy()
    A.delete()
    ray.get(signal.wait.remote(), timeout=10)

    # If the destructor errored, it should be logged but also cleaned up.
    @serve.deployment
    class B:
        def __del__(self):
            raise RuntimeError("Opps")

    B.deploy()
    B.delete()


def test_run_get_ingress_app(serve_instance):
    """Check that serve.run() with an app returns the ingress."""

    @serve.deployment(route_prefix=None)
    def f():
        return "got f"

    @serve.deployment(route_prefix="/g")
    def g():
        return "got g"

    app = Application([f, g])
    ingress_handle = serve.run(app)

    assert ray.get(ingress_handle.remote()) == "got g"
    serve_instance.delete_deployments(["f", "g"])

    no_ingress_app = Application([f.options(route_prefix="/f"), g])
    ingress_handle = serve.run(no_ingress_app)
    assert ingress_handle is None


def test_run_get_ingress_node(serve_instance):
    """Check that serve.run() with a node returns the ingress."""

    @serve.deployment
    class Driver:
        def __init__(self, dag):
            self.dag = dag

        async def __call__(self, *args):
            return await self.dag.remote()

    @serve.deployment
    class f:
        def __call__(self, *args):
            return "got f"

    dag = Driver.bind(f.bind())
    ingress_handle = serve.run(dag)

    assert ray.get(ingress_handle.remote()) == "got f"


def test_run_delete_old_deployments(serve_instance):
    """Check that serve.run() can remove all old deployments"""

    @serve.deployment(name="f", route_prefix="/test1")
    def f():
        return "got f"

    @serve.deployment(name="g", route_prefix="/test2")
    def g():
        return "got g"

    ingress_handle = serve.run(f.bind())
    assert ray.get(ingress_handle.remote()) == "got f"

    ingress_handle = serve.run(g.bind())
    assert ray.get(ingress_handle.remote()) == "got g"

    assert "g" in serve.list_deployments()
    assert "f" not in serve.list_deployments()


class TestSetOptions:
    def test_set_options_basic(self):
        @serve.deployment(
            num_replicas=4,
            max_concurrent_queries=3,
            prev_version="abcd",
            ray_actor_options={"num_cpus": 2},
            _health_check_timeout_s=17,
        )
        def f():
            pass

        f.set_options(
            num_replicas=9,
            prev_version="abcd",
            version="efgh",
            ray_actor_options={"num_gpus": 3},
        )

        assert f.num_replicas == 9
        assert f.max_concurrent_queries == 3
        assert f.prev_version == "abcd"
        assert f.version == "efgh"
        assert f.ray_actor_options == {"num_gpus": 3}
        assert f._config.health_check_timeout_s == 17

    def test_set_options_validation(self):
        @serve.deployment
        def f():
            pass

        with pytest.raises(TypeError):
            f.set_options(init_args=-4)

        with pytest.raises(ValueError):
            f.set_options(max_concurrent_queries=-4)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
