import asyncio
import os
from typing import Optional

from fastapi import FastAPI
import requests
from pydantic import BaseModel, ValidationError
import pytest
import starlette.responses

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.built_application import BuiltApplication
from ray.serve.deployment import Application
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.drivers import DAGDriver
from ray.serve.exceptions import RayServeException
from ray.serve._private.api import call_app_builder_with_args_if_necessary
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)


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

    serve.run(function.bind())

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
    handle = serve.run(deployment_cls.bind())

    assert (
        requests.get(f"http://localhost:8000/{deployment_cls.name}").text
        == expected_output
    )
    assert ray.get(handle.remote()) == expected_output


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_function_no_params_call_with_param(serve_instance, use_async):
    serve.start()

    if use_async:
        expected_output = "async!"
        deployment_cls = async_d
    else:
        expected_output = "sync!"
        deployment_cls = sync_d
    handle = serve.run(deployment_cls.bind())

    assert (
        requests.get(f"http://localhost:8000/{deployment_cls.name}").text
        == expected_output
    )
    with pytest.raises(
        TypeError, match=r"\(\) takes 0 positional arguments but 1 was given"
    ):
        assert ray.get(handle.remote(1)) == expected_output

    with pytest.raises(TypeError, match=r"\(\) got an unexpected keyword argument"):
        assert ray.get(handle.remote(key=1)) == expected_output


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_class_no_params(serve_instance, use_async):
    serve.start()
    if use_async:
        deployment_cls = AsyncCounter
    else:
        deployment_cls = Counter
    handle = serve.run(deployment_cls.bind())

    assert requests.get(f"http://127.0.0.1:8000/{deployment_cls.name}").json() == {
        "count": 1
    }
    assert requests.get(f"http://127.0.0.1:8000/{deployment_cls.name}").json() == {
        "count": 2
    }
    assert ray.get(handle.remote()) == {"count": 3}


def test_user_config(serve_instance):
    @serve.deployment("counter", num_replicas=2, user_config={"count": 123, "b": 2})
    class Counter:
        def __init__(self):
            self.count = 10

        def __call__(self, *args):
            return self.count, os.getpid()

        def reconfigure(self, config):
            self.count = config["count"]

    handle = serve.run(Counter.bind())

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
    serve.run(Counter.bind())
    wait_for_condition(lambda: check("123", 3))

    Counter = Counter.options(user_config={"count": 456})
    serve.run(Counter.bind())
    wait_for_condition(lambda: check("456", 3))


def test_user_config_empty(serve_instance):
    @serve.deployment(user_config={})
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, *args):
            return self.count

        def reconfigure(self, config):
            self.count += 1

    handle = serve.run(Counter.bind())
    assert ray.get(handle.remote()) == 1


def test_scaling_replicas(serve_instance):
    @serve.deployment(name="counter", num_replicas=2)
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, _):
            self.count += 1
            return self.count

    serve.run(Counter.bind())

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/counter").json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    serve.run(Counter.options(num_replicas=1).bind())

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

            wait_for_condition(
                lambda: len(serve_instance.list_deployments()) == 0,
                timeout=5,
            )


def test_starlette_request(serve_instance):
    @serve.deployment(name="api")
    async def echo_body(starlette_request):
        data = await starlette_request.body()
        return data

    serve.run(echo_body.bind())

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

    @serve.deployment(route_prefix="/g")
    def g():
        return "got g"

    app = BuiltApplication([g])
    ingress_handle = serve.run(app)

    assert ray.get(ingress_handle.remote()) == "got g"
    serve_instance.delete_deployments(["g"])

    no_ingress_app = BuiltApplication([g.options(route_prefix=None)])
    ingress_handle = serve.run(no_ingress_app)
    assert ingress_handle is None


def test_run_get_ingress_node(serve_instance):
    """Check that serve.run() with a node returns the ingress."""

    @serve.deployment
    class Driver:
        def __init__(self, dag: RayServeDAGHandle):
            self.dag = dag

        async def __call__(self, *args):
            return await (await self.dag.remote())

    @serve.deployment
    class f:
        def __call__(self, *args):
            return "got f"

    dag = Driver.bind(f.bind())
    ingress_handle = serve.run(dag)

    assert ray.get(ingress_handle.remote()) == "got f"


class TestSetOptions:
    def test_set_options_basic(self):
        @serve.deployment(
            num_replicas=4,
            max_concurrent_queries=3,
            ray_actor_options={"num_cpus": 2},
            health_check_timeout_s=17,
        )
        def f():
            pass

        f.set_options(
            num_replicas=9,
            version="efgh",
            ray_actor_options={"num_gpus": 3},
        )

        assert f.num_replicas == 9
        assert f.max_concurrent_queries == 3
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


def test_deploy_application(serve_instance):
    """Test deploy multiple applications"""

    @serve.deployment
    def f():
        return "got f"

    @serve.deployment
    def g():
        return "got g"

    @serve.deployment(route_prefix="/my_prefix")
    def h():
        return "got h"

    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    app = FastAPI()

    @serve.deployment(route_prefix="/hello")
    @serve.ingress(app)
    class MyFastAPIDeployment:
        @app.get("/")
        def root(self):
            return "Hello, world!"

    # Test function deployment with app name
    f_handle = serve.run(f.bind(), name="app_f")
    assert ray.get(f_handle.remote()) == "got f"
    assert requests.get("http://127.0.0.1:8000/").text == "got f"

    # Test function deployment with app name and route_prefix
    g_handle = serve.run(g.bind(), name="app_g", route_prefix="/app_g")
    assert ray.get(g_handle.remote()) == "got g"
    assert requests.get("http://127.0.0.1:8000/app_g").text == "got g"

    # Test function deployment with app name and route_prefix set in deployment
    # decorator
    h_handle = serve.run(h.bind(), name="app_h")
    assert ray.get(h_handle.remote()) == "got h"
    assert requests.get("http://127.0.0.1:8000/my_prefix").text == "got h"

    # Test deployment graph
    graph_handle = serve.run(
        DAGDriver.bind(Model1.bind()), name="graph", route_prefix="/my_graph"
    )
    assert ray.get(graph_handle.predict.remote()) == "got model1"
    assert requests.get("http://127.0.0.1:8000/my_graph").text == '"got model1"'

    # Test FastAPI
    serve.run(MyFastAPIDeployment.bind(), name="FastAPI")
    assert requests.get("http://127.0.0.1:8000/hello").text == '"Hello, world!"'


def test_delete_application(serve_instance):
    """Test delete single application"""

    @serve.deployment
    def f():
        return "got f"

    @serve.deployment
    def g():
        return "got g"

    f_handle = serve.run(f.bind(), name="app_f")
    g_handle = serve.run(g.bind(), name="app_g", route_prefix="/app_g")
    assert ray.get(f_handle.remote()) == "got f"
    assert requests.get("http://127.0.0.1:8000/").text == "got f"

    serve.delete("app_f")
    assert "Path '/' not found" in requests.get("http://127.0.0.1:8000/").text

    # delete again, no exception & crash expected.
    serve.delete("app_f")

    # make sure no affect to app_g
    assert ray.get(g_handle.remote()) == "got g"
    assert requests.get("http://127.0.0.1:8000/app_g").text == "got g"


def test_deployment_name_with_app_name(serve_instance):
    """Test replica name with app name as prefix"""

    controller = serve_instance._controller

    @serve.deployment
    def g():
        return "got g"

    serve.run(g.bind())
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert (
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}g"
        in deployment_info
    )

    @serve.deployment
    def f():
        return "got f"

    serve.run(f.bind(), route_prefix="/f", name="app1")
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert "app1_f" in deployment_info


def test_deploy_application_with_same_name(serve_instance):
    """Test deploying two applications with the same name."""

    controller = serve_instance._controller

    @serve.deployment
    class Model:
        def __call__(self):
            return "got model"

    handle = serve.run(Model.bind(), name="app")
    assert ray.get(handle.remote()) == "got model"
    assert requests.get("http://127.0.0.1:8000/").text == "got model"
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert "app_Model" in deployment_info

    # After deploying a new app with the same name, no Model replicas should be running
    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    handle = serve.run(Model1.bind(), name="app")
    assert ray.get(handle.remote()) == "got model1"
    assert requests.get("http://127.0.0.1:8000/").text == "got model1"
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert "app_Model1" in deployment_info
    assert "app_Model" not in deployment_info or deployment_info["app_Model"] == []

    # Redeploy with same app to update route prefix
    handle = serve.run(Model1.bind(), name="app", route_prefix="/my_app")
    assert requests.get("http://127.0.0.1:8000/my_app").text == "got model1"
    assert requests.get("http://127.0.0.1:8000/").status_code == 404


def test_deploy_application_with_route_prefix_conflict(serve_instance):
    """Test route_prefix conflicts with different apps."""

    @serve.deployment
    class Model:
        def __call__(self):
            return "got model"

    handle = serve.run(Model.bind(), name="app")
    assert ray.get(handle.remote()) == "got model"
    assert requests.get("http://127.0.0.1:8000/").text == "got model"

    # Second app with the same route_prefix fails to be deployed
    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    with pytest.raises(RayServeException):
        handle = serve.run(Model1.bind(), name="app1")

    # Update the route prefix
    handle = serve.run(Model1.bind(), name="app1", route_prefix="/model1")
    assert ray.get(handle.remote()) == "got model1"
    assert requests.get("http://127.0.0.1:8000/model1").text == "got model1"

    # The "app" application should still work properly
    assert requests.get("http://127.0.0.1:8000/").text == "got model"


@pytest.mark.parametrize(
    "ingress_route,app_route",
    [
        ("/hello", "/"),
        ("/hello", "/override"),
        ("/", "/override"),
        (None, "/override"),
        ("/hello", None),
        (None, None),
    ],
)
def test_application_route_prefix_override(serve_instance, ingress_route, app_route):
    """
    Set route prefix in serve.run to a non-None value, check it overrides correctly.
    """

    @serve.deployment
    def f():
        return "hello"

    node = f.options(route_prefix=ingress_route).bind()
    serve.run(node, route_prefix=app_route)
    if app_route is None:
        routes = requests.get("http://localhost:8000/-/routes").json()
        assert len(routes) == 0
    else:
        assert requests.get(f"http://localhost:8000{app_route}").text == "hello"


@pytest.mark.parametrize("ingress_route", ["/hello", "/"])
def test_application_route_prefix_override1(serve_instance, ingress_route):
    """
    Don't set route prefix in serve.run, check it always uses the ingress deployment
    route.
    """

    @serve.deployment
    def f():
        return "hello"

    node = f.options(route_prefix=ingress_route).bind()
    serve.run(node)
    if ingress_route is None:
        routes = requests.get("http://localhost:8000/-/routes").json()
        assert len(routes) == 0
    else:
        assert requests.get(f"http://localhost:8000{ingress_route}").text == "hello"


def test_invalid_driver_deployment_class():
    """Test invalid driver deployment class"""

    @serve.deployment(is_driver_deployment=True)
    def f():
        pass

    with pytest.raises(ValueError):
        f.options(num_replicas=2)
    with pytest.raises(ValueError):
        f.options(autoscaling_config={"min_replicas": "1"})


class TestAppBuilder:
    @serve.deployment
    class A:
        pass

    @serve.deployment
    def f():
        pass

    class TypedArgs(BaseModel):
        message: str
        num_replicas: Optional[int]

    def test_prebuilt_app(self):
        a = self.A.bind()
        assert call_app_builder_with_args_if_necessary(a, {}) == a

        f = self.f.bind()
        assert call_app_builder_with_args_if_necessary(f, {}) == f

        with pytest.raises(
            ValueError,
            match="Arguments can only be passed to an application builder function",
        ):
            call_app_builder_with_args_if_necessary(f, {"key": "val"})

    def test_invalid_builder(self):
        class ThisShouldBeAFunction:
            pass

        with pytest.raises(
            TypeError,
            match=(
                "Expected a built Serve application "
                "or an application builder function"
            ),
        ):
            call_app_builder_with_args_if_necessary(ThisShouldBeAFunction, {})

    def test_invalid_signature(self):
        def builder_with_two_args(args1, args2):
            return self.f.bind()

        with pytest.raises(
            TypeError,
            match="Application builder functions should take exactly one parameter",
        ):
            call_app_builder_with_args_if_necessary(builder_with_two_args, {})

    def test_builder_returns_bad_type(self):
        def return_none(args):
            self.f.bind()

        with pytest.raises(
            TypeError,
            match="Application builder functions must return a",
        ):
            call_app_builder_with_args_if_necessary(return_none, {})

        def return_unbound_deployment(args):
            return self.f

        with pytest.raises(
            TypeError,
            match="Application builder functions must return a",
        ):
            call_app_builder_with_args_if_necessary(return_unbound_deployment, {})

    def test_basic_no_args(self):
        def build_function(args):
            return self.A.bind()

        assert isinstance(
            call_app_builder_with_args_if_necessary(build_function, {}), Application
        )

        def build_class(args):
            return self.f.bind()

        assert isinstance(
            call_app_builder_with_args_if_necessary(build_class, {}), Application
        )

    def test_args_dict(self):
        args_dict = {"message": "hiya", "num_replicas": "3"}

        def build(args):
            assert len(args) == 2
            assert args["message"] == "hiya"
            assert args["num_replicas"] == "3"
            return self.A.options(num_replicas=int(args["num_replicas"])).bind(
                args["message"]
            )

        app = call_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

    def test_args_typed(self):
        args_dict = {"message": "hiya", "num_replicas": "3"}

        def build(args: self.TypedArgs):
            assert isinstance(args, self.TypedArgs)
            assert args.message == "hiya"
            assert args.num_replicas == 3
            return self.A.options(num_replicas=args.num_replicas).bind(args.message)

        app = call_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

        # Sanity check that pydantic validation works.

        # 1) Check that validation permits a missing optional field.
        def check_missing_optional(args: self.TypedArgs):
            assert args.message == "hiya"
            assert args.num_replicas is None
            return self.A.bind()

        app = call_app_builder_with_args_if_necessary(
            check_missing_optional, {"message": "hiya"}
        )
        assert isinstance(app, Application)

        # 2) Check that validation rejects a missing required field.
        def check_missing_required(args: self.TypedArgs):
            assert False, "Shouldn't get here because validation failed."

        with pytest.raises(ValidationError, match="field required"):
            call_app_builder_with_args_if_necessary(
                check_missing_required, {"num_replicas": "10"}
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
