import asyncio
import os
import sys
from typing import Dict, Optional

import pytest
import requests
import starlette.responses
from fastapi import FastAPI

import ray
from ray import serve
from ray._private.pydantic_compat import BaseModel, ValidationError
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.api import call_app_builder_with_args_if_necessary
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.deployment import Application
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle, RayServeHandle


@pytest.fixture
def serve_and_ray_shutdown():
    yield
    serve.shutdown()
    ray.shutdown()


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


def test_starlette_response_basic(serve_instance):
    @serve.deployment
    def basic():
        return starlette.responses.Response("Hello, world!", media_type="text/plain")

    serve.run(basic.bind())
    assert requests.get("http://127.0.0.1:8000/").text == "Hello, world!"


def test_starlette_response_html(serve_instance):
    @serve.deployment
    def html():
        return starlette.responses.HTMLResponse(
            "<html><body><h1>Hello, world!</h1></body></html>"
        )

    serve.run(html.bind())
    assert (
        requests.get("http://127.0.0.1:8000/").text
        == "<html><body><h1>Hello, world!</h1></body></html>"
    )


def test_starlette_response_plain_text(serve_instance):
    @serve.deployment
    def plain_text():
        return starlette.responses.PlainTextResponse("Hello, world!")

    serve.run(plain_text.bind())
    assert requests.get("http://127.0.0.1:8000/").text == "Hello, world!"


def test_starlette_response_json(serve_instance):
    @serve.deployment
    def json():
        return starlette.responses.JSONResponse({"hello": "world"})

    serve.run(json.bind())
    assert requests.get("http://127.0.0.1:8000/json").json()["hello"] == "world"


def test_starlette_response_redirect(serve_instance):
    @serve.deployment
    def basic():
        return starlette.responses.Response("Hello, world!", media_type="text/plain")

    @serve.deployment(name="redirect")
    def redirect():
        return starlette.responses.RedirectResponse(url="http://127.0.0.1:8000/")

    serve.run(basic.bind(), name="app1", route_prefix="/")
    serve.run(redirect.bind(), name="app2", route_prefix="/redirect")
    assert requests.get("http://127.0.0.1:8000/redirect").text == "Hello, world!"


def test_starlette_response_streaming(serve_instance):
    @serve.deployment
    def streaming():
        async def slow_numbers():
            for number in range(1, 4):
                yield str(number)
                await asyncio.sleep(0.01)

        return starlette.responses.StreamingResponse(
            slow_numbers(), media_type="text/plain", status_code=418
        )

    serve.run(streaming.bind())
    resp = requests.get("http://127.0.0.1:8000/")
    assert resp.text == "123"
    assert resp.status_code == 418


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_function_no_params(serve_instance, use_async):
    if use_async:
        expected_output = "async!"
        deployment_cls = async_d
    else:
        expected_output = "sync!"
        deployment_cls = sync_d
    handle = serve.run(deployment_cls.bind()).options(
        use_new_handle_api=True,
    )

    assert (
        requests.get(f"http://localhost:8000/{deployment_cls.name}").text
        == expected_output
    )
    assert handle.remote().result() == expected_output


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_function_no_params_call_with_param(serve_instance, use_async):
    if use_async:
        expected_output = "async!"
        deployment_cls = async_d
    else:
        expected_output = "sync!"
        deployment_cls = sync_d

    handle = serve.run(deployment_cls.bind()).options(
        use_new_handle_api=True,
    )

    assert (
        requests.get(f"http://localhost:8000/{deployment_cls.name}").text
        == expected_output
    )
    with pytest.raises(
        TypeError, match=r"\(\) takes 0 positional arguments but 1 was given"
    ):
        handle.remote(1).result()

    with pytest.raises(TypeError, match=r"\(\) got an unexpected keyword argument"):
        handle.remote(key=1).result()


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_class_no_params(serve_instance, use_async):
    if use_async:
        deployment_cls = AsyncCounter
    else:
        deployment_cls = Counter

    handle = serve.run(deployment_cls.bind()).options(
        use_new_handle_api=True,
    )

    assert requests.get(f"http://127.0.0.1:8000/{deployment_cls.name}").json() == {
        "count": 1
    }
    assert requests.get(f"http://127.0.0.1:8000/{deployment_cls.name}").json() == {
        "count": 2
    }
    assert handle.remote().result() == {"count": 3}


def test_user_config(serve_instance):
    @serve.deployment("counter", num_replicas=2, user_config={"count": 123, "b": 2})
    class Counter:
        def __init__(self):
            self.count = 10

        def __call__(self, *args):
            return self.count, os.getpid()

        def reconfigure(self, config):
            self.count = config["count"]

    handle = serve.run(Counter.bind()).options(
        use_new_handle_api=True,
    )

    def check(val, num_replicas):
        pids_seen = set()
        for i in range(100):
            result = handle.remote().result()
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

    handle = serve.run(Counter.bind()).options(
        use_new_handle_api=True,
    )
    assert handle.remote().result() == 1


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


def test_shutdown_destructor(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment
    class A:
        def __del__(self):
            signal.send.remote()

    serve.run(A.bind(), name="A")
    serve.delete("A")
    ray.get(signal.wait.remote(), timeout=10)


def test_run_get_ingress_node(serve_instance):
    """Check that serve.run() with a node returns the ingress."""

    @serve.deployment
    class Driver:
        def __init__(self, handle):
            self._h = handle.options(use_new_handle_api=True)

        async def __call__(self, *args):
            return await self._h.remote()

    @serve.deployment
    class f:
        def __call__(self, *args):
            return "got f"

    handle = serve.run(Driver.bind(f.bind())).options(
        use_new_handle_api=True,
    )
    assert handle.remote().result() == "got f"


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
        assert f.ray_actor_options == {"num_cpus": 1, "num_gpus": 3}
        assert f._deployment_config.health_check_timeout_s == 17

    def test_set_options_validation(self):
        @serve.deployment
        def f():
            pass

        with pytest.raises(TypeError):
            f.set_options(init_args=-4)

        with pytest.raises(ValueError):
            f.set_options(max_concurrent_queries=-4)


def test_deploy_application_basic(serve_instance):
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
        def __call__(self, *args):
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
    assert f_handle.remote().result() == "got f"
    assert requests.get("http://127.0.0.1:8000/").text == "got f"

    # Test function deployment with app name and route_prefix
    g_handle = serve.run(g.bind(), name="app_g", route_prefix="/app_g")
    assert g_handle.remote().result() == "got g"
    assert requests.get("http://127.0.0.1:8000/app_g").text == "got g"

    # Test function deployment with app name and route_prefix set in deployment
    # decorator
    h_handle = serve.run(h.bind(), name="app_h")
    assert h_handle.remote().result() == "got h"
    assert requests.get("http://127.0.0.1:8000/my_prefix").text == "got h"

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

    f_handle = serve.run(f.bind(), name="app_f").options(
        use_new_handle_api=True,
    )
    g_handle = serve.run(g.bind(), name="app_g", route_prefix="/app_g").options(
        use_new_handle_api=True,
    )
    assert f_handle.remote().result() == "got f"
    assert requests.get("http://127.0.0.1:8000/").text == "got f"

    serve.delete("app_f")
    assert "Path '/' not found" in requests.get("http://127.0.0.1:8000/").text

    # delete again, no exception & crash expected.
    serve.delete("app_f")

    # make sure no affect to app_g
    assert g_handle.remote().result() == "got g"
    assert requests.get("http://127.0.0.1:8000/app_g").text == "got g"


def test_deployment_name_with_app_name(serve_instance):
    """Test replica name with app name as prefix"""

    controller = serve_instance._controller

    @serve.deployment
    def g():
        return "got g"

    serve.run(g.bind())
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID("g", SERVE_DEFAULT_APP_NAME) in deployment_info

    @serve.deployment
    def f():
        return "got f"

    serve.run(f.bind(), route_prefix="/f", name="app1")
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID("f", "app1") in deployment_info


def test_deploy_application_with_same_name(serve_instance):
    """Test deploying two applications with the same name."""

    controller = serve_instance._controller

    @serve.deployment
    class Model:
        def __call__(self):
            return "got model"

    handle = serve.run(Model.bind(), name="app").options(
        use_new_handle_api=True,
    )
    assert handle.remote().result() == "got model"
    assert requests.get("http://127.0.0.1:8000/").text == "got model"
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID("Model", "app") in deployment_info

    # After deploying a new app with the same name, no Model replicas should be running
    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    handle = serve.run(Model1.bind(), name="app").options(
        use_new_handle_api=True,
    )
    assert handle.remote().result() == "got model1"
    assert requests.get("http://127.0.0.1:8000/").text == "got model1"
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID("Model1", "app") in deployment_info
    assert (
        DeploymentID("Model", "app") not in deployment_info
        or deployment_info[DeploymentID("Model", "app")] == []
    )

    # Redeploy with same app to update route prefix
    serve.run(Model1.bind(), name="app", route_prefix="/my_app")
    assert requests.get("http://127.0.0.1:8000/my_app").text == "got model1"
    assert requests.get("http://127.0.0.1:8000/").status_code == 404


def test_deploy_application_with_route_prefix_conflict(serve_instance):
    """Test route_prefix conflicts with different apps."""

    @serve.deployment
    class Model:
        def __call__(self):
            return "got model"

    handle = serve.run(Model.bind(), name="app").options(
        use_new_handle_api=True,
    )
    assert handle.remote().result() == "got model"
    assert requests.get("http://127.0.0.1:8000/").text == "got model"

    # Second app with the same route_prefix fails to be deployed
    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    with pytest.raises(RayServeException):
        serve.run(Model1.bind(), name="app1")

    # Update the route prefix
    handle = serve.run(Model1.bind(), name="app1", route_prefix="/model1").options(
        use_new_handle_api=True,
    )
    assert handle.remote().result() == "got model1"
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

        def build(args):
            """Builder with no type hint."""

            return self.A.options(num_replicas=args["num_replicas"]).bind(
                args["message"]
            )

        app = call_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

        def build(args: Dict[str, str]):
            """Builder with vanilla type hint."""

            return self.A.options(num_replicas=args["num_replicas"]).bind(
                args["message"]
            )

        app = call_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

        class ForwardRef:
            def build(args: "ForwardRef"):
                """Builder with forward reference as type hint."""

                return self.A.options(num_replicas=args["num_replicas"]).bind(
                    args["message"]
                )

        app = call_app_builder_with_args_if_necessary(ForwardRef.build, args_dict)
        assert isinstance(app, Application)

        def build(args: self.TypedArgs):
            """Builder with Pydantic model type hint."""

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

    @pytest.mark.parametrize("use_v1_patch", [True, False])
    def test_pydantic_version_compatibility(self, use_v1_patch: bool):
        """Check compatibility with different pydantic versions."""

        if use_v1_patch:
            try:
                # Only runs if installed pydantic version is >=2.5.0
                from pydantic.v1 import BaseModel
            except ImportError:
                return
        else:
            from pydantic import BaseModel

        cat_dict = {"color": "orange", "age": 10}

        class Cat(BaseModel):
            color: str
            age: int

        def build(args: Cat):
            """Builder with Pydantic model type hint."""

            assert isinstance(args, Cat), f"args type: {type(args)}"
            assert args.color == cat_dict["color"]
            assert args.age == cat_dict["age"]
            return self.A.bind(f"My {args.color} cat is {args.age} years old.")

        app = call_app_builder_with_args_if_necessary(build, cat_dict)
        assert isinstance(app, Application)


def test_no_slash_route_prefix(serve_instance):
    """Test serve run with no slash route_prefix.

    This test ensure when serve runs with no prefix slash in route_prefix, it will throw
    good error message.
    """

    @serve.deployment
    def f():
        pass

    with pytest.raises(
        ValueError, match=r"The route_prefix must start with a forward slash \('/'\)"
    ):
        serve.run(f.bind(), route_prefix="no_slash")


def test_status_basic(serve_instance):
    # Before Serve is started, serve.status() should have an empty list of applications
    assert len(serve.status().applications) == 0

    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    class A:
        def __call__(self, val: int):
            return val + 1

    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    def f():
        return "hello world"

    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    class MyDriver:
        def __init__(self, handle):
            self._h = handle.options(use_new_handle_api=True)

        async def __call__(self):
            return await self._h.remote()

    handle_1 = serve.run(A.bind(), name="plus", route_prefix="/a").options(
        use_new_handle_api=True,
    )
    handle_2 = serve.run(
        MyDriver.bind(f.bind()), name="hello", route_prefix="/b"
    ).options(
        use_new_handle_api=True,
    )

    assert handle_1.remote(8).result() == 9
    assert handle_2.remote().result() == "hello world"

    app_status = serve.status().applications
    assert len(app_status) == 2
    assert set(app_status["plus"].deployments.keys()) == {"A"}
    assert set(app_status["hello"].deployments.keys()) == {"MyDriver", "f"}
    for d in app_status["plus"].deployments.values():
        assert d.status == "HEALTHY" and d.replica_states == {"RUNNING": 1}
    for d in app_status["plus"].deployments.values():
        assert d.status == "HEALTHY" and d.replica_states == {"RUNNING": 1}

    proxy_status = serve.status().proxies
    assert all(p == "HEALTHY" for p in proxy_status.values())


def test_status_constructor_error(serve_instance):
    """Deploys Serve deployment that errors out in constructor, checks that the
    traceback is surfaced in serve.status().
    """

    @serve.deployment
    class A:
        def __init__(self):
            1 / 0

    serve.run(A.bind(), _blocking=False)

    def check_for_failed_deployment():
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        error_substr = "ZeroDivisionError: division by zero"
        return (
            default_app.status == "DEPLOY_FAILED"
            and error_substr in default_app.deployments["A"].message
        )

    wait_for_condition(check_for_failed_deployment)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env support experimental on windows"
)
def test_status_package_unavailable_in_controller(serve_instance):
    """Test that exceptions raised from packages that are installed on deployment actors
    but not on controller is serialized and surfaced properly in serve.status().
    """

    @serve.deployment
    class MyDeployment:
        def __init__(self):
            import pymysql
            from sqlalchemy import create_engine

            pymysql.install_as_MySQLdb()

            create_engine("mysql://some_wrong_url:3306").connect()

    ray_actor_options = {"runtime_env": {"pip": ["PyMySQL", "sqlalchemy==1.3.19"]}}
    serve.run(
        MyDeployment.options(ray_actor_options=ray_actor_options).bind(),
        _blocking=False,
    )

    def check_for_failed_deployment():
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return (
            default_app.status == "DEPLOY_FAILED"
            and "some_wrong_url" in default_app.deployments["MyDeployment"].message
        )

    wait_for_condition(check_for_failed_deployment, timeout=15)


def test_get_app_handle_basic(serve_instance):
    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    class M:
        def __call__(self, val: int):
            return val + 1

    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    def f():
        return "hello world"

    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    class MyDriver:
        def __init__(self, handle):
            self._h = handle.options(use_new_handle_api=True)

        async def __call__(self):
            return await self._h.remote()

    serve.run(M.bind(), name="A", route_prefix="/a")
    serve.run(MyDriver.bind(f.bind()), name="B", route_prefix="/b")

    handle = serve.get_app_handle("A")
    assert handle.remote(8).result() == 9

    handle = serve.get_app_handle("B")
    assert handle.remote().result() == "hello world"


def test_get_app_handle_dne(serve_instance):
    """Test getting app handle to an app that doesn't exist."""

    with pytest.raises(RayServeException) as e:
        serve.get_app_handle("random")

    assert "Application 'random' does not exist" in str(e.value)


def test_get_app_handle_within_deployment_async(serve_instance):
    @serve.deployment()
    class a:
        def __init__(self, handle):
            self.handle = handle

        def __call__(self, val: int):
            return val + 2

    @serve.deployment()
    class b:
        def __call__(self, val: int):
            return val

    @serve.deployment
    async def f(val):
        handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
        result = await handle.remote(val)
        return f"The answer is {result}"

    serve.run(a.bind(b.bind()), route_prefix="/math")
    serve.run(f.bind(), name="call")

    handle = serve.get_app_handle("call")
    assert handle.remote(7).result() == "The answer is 9"


def test_get_deployment_handle_basic(serve_instance):
    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    def f():
        return "hello world"

    @serve.deployment(ray_actor_options={"num_cpus": 0.1})
    class MyDriver:
        def __init__(self, handle):
            self._h = handle.options(use_new_handle_api=True)

        async def __call__(self):
            return f"{await self._h.remote()}!!"

    serve.run(MyDriver.bind(f.bind()))

    handle = serve.get_deployment_handle("f", SERVE_DEFAULT_APP_NAME)
    assert isinstance(handle, DeploymentHandle)
    assert handle.remote().result() == "hello world"

    app_handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    assert isinstance(app_handle, DeploymentHandle)
    assert app_handle.remote().result() == "hello world!!"


def test_deployment_handle_nested_in_obj(serve_instance):
    """Test binding a handle within a custom object."""

    class HandleWrapper:
        def __init__(self, handle: RayServeHandle):
            self._handle = handle

        def get(self) -> DeploymentHandle:
            return self._handle.options(use_new_handle_api=True)

    @serve.deployment
    def f() -> str:
        return "hi"

    @serve.deployment
    class MyDriver:
        def __init__(self, handle_wrapper: HandleWrapper):
            self.handle_wrapper = handle_wrapper

        async def __call__(self) -> str:
            return await self.handle_wrapper.get().remote()

    handle_wrapper = HandleWrapper(f.bind())
    h = serve.run(MyDriver.bind(handle_wrapper)).options(use_new_handle_api=True)
    assert h.remote().result() == "hi"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
