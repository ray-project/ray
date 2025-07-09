import asyncio
import os
import sys
from typing import Dict, List, Optional

import httpx
import pytest
import starlette.responses
from fastapi import FastAPI

import ray
from ray import serve
from ray._common.pydantic_compat import BaseModel, ValidationError
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.api import call_user_app_builder_with_args_if_necessary
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    DEFAULT_MAX_ONGOING_REQUESTS,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    RequestRouter,
)
from ray.serve._private.test_utils import get_application_url
from ray.serve.deployment import Application
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle


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


class FakeRequestRouter(RequestRouter):
    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        return [candidate_replicas]


@serve.deployment(request_router_class=FakeRequestRouter)
class AppWithCustomRequestRouter:
    def __call__(self) -> str:
        return "Hello, world!"


def test_e2e(serve_instance):
    @serve.deployment(name="api")
    def function(starlette_request):
        return {"method": starlette_request.method}

    serve.run(function.bind())
    url = f"{get_application_url()}/api"
    resp = httpx.get(url).json()["method"]
    assert resp == "GET"

    resp = httpx.post(url).json()["method"]
    assert resp == "POST"


def test_starlette_response_basic(serve_instance):
    @serve.deployment
    def basic():
        return starlette.responses.Response("Hello, world!", media_type="text/plain")

    serve.run(basic.bind())
    url = f"{get_application_url()}/"
    assert httpx.get(url).text == "Hello, world!"


def test_starlette_response_html(serve_instance):
    @serve.deployment
    def html():
        return starlette.responses.HTMLResponse(
            "<html><body><h1>Hello, world!</h1></body></html>"
        )

    serve.run(html.bind())
    url = f"{get_application_url()}/"
    assert httpx.get(url).text == "<html><body><h1>Hello, world!</h1></body></html>"


def test_starlette_response_plain_text(serve_instance):
    @serve.deployment
    def plain_text():
        return starlette.responses.PlainTextResponse("Hello, world!")

    serve.run(plain_text.bind())
    url = f"{get_application_url()}/"
    assert httpx.get(url).text == "Hello, world!"


def test_starlette_response_json(serve_instance):
    @serve.deployment
    def json():
        return starlette.responses.JSONResponse({"hello": "world"})

    serve.run(json.bind())
    url = f"{get_application_url()}/json"
    assert httpx.get(url).json()["hello"] == "world"


def test_starlette_response_redirect(serve_instance):
    @serve.deployment
    def basic():
        return starlette.responses.Response("Hello, world!", media_type="text/plain")

    @serve.deployment(name="redirect")
    def redirect():
        url = get_application_url("HTTP", app_name="app1")
        return starlette.responses.RedirectResponse(url=url)

    serve.run(basic.bind(), name="app1", route_prefix="/")
    serve.run(redirect.bind(), name="app2", route_prefix="/redirect")
    url = f"{get_application_url(app_name='app2')}"
    assert httpx.get(url, follow_redirects=True).text == "Hello, world!"


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
    url = f"{get_application_url()}/"
    resp = httpx.get(url)
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
    handle = serve.run(deployment_cls.bind())
    url = f"{get_application_url()}/{deployment_cls.name}"
    assert httpx.get(url).text == expected_output
    assert handle.remote().result() == expected_output


@pytest.mark.parametrize("use_async", [False, True])
def test_deploy_function_no_params_call_with_param(serve_instance, use_async):
    if use_async:
        expected_output = "async!"
        deployment_cls = async_d
    else:
        expected_output = "sync!"
        deployment_cls = sync_d

    handle = serve.run(deployment_cls.bind())
    url = f"{get_application_url()}/{deployment_cls.name}"
    assert httpx.get(url).text == expected_output
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

    handle = serve.run(deployment_cls.bind())

    url = f"{get_application_url()}/{deployment_cls.name}"
    assert httpx.get(url).json() == {"count": 1}
    assert httpx.get(url).json() == {"count": 2}
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

    handle = serve.run(Counter.bind())

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

    handle = serve.run(Counter.bind())
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
        url = f"{get_application_url()}/counter"
        resp = httpx.get(url).json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    serve.run(Counter.options(num_replicas=1).bind())

    counter_result = []
    for _ in range(10):
        url = f"{get_application_url()}/counter"
        resp = httpx.get(url).json()
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

    url = f"{get_application_url()}/api"
    resp = httpx.post(url, data=long_string).text
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
            self._h = handle

        async def __call__(self, *args):
            return await self._h.remote()

    @serve.deployment
    class f:
        def __call__(self, *args):
            return "got f"

    handle = serve.run(Driver.bind(f.bind()))
    assert handle.remote().result() == "got f"


def test_deploy_application_basic(serve_instance):
    """Test deploy multiple applications"""

    @serve.deployment
    def f():
        return "got f"

    @serve.deployment
    def g():
        return "got g"

    @serve.deployment
    def h():
        return "got h"

    @serve.deployment
    class Model1:
        def __call__(self, *args):
            return "got model1"

    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class MyFastAPIDeployment:
        @app.get("/")
        def root(self):
            return "Hello, world!"

    # Test function deployment with app name
    f_handle = serve.run(f.bind(), name="app_f")
    assert f_handle.remote().result() == "got f"
    url = f"{get_application_url(app_name='app_f')}/"
    assert httpx.get(url).text == "got f"

    # Test function deployment with app name and route_prefix
    g_handle = serve.run(g.bind(), name="app_g", route_prefix="/app_g")
    assert g_handle.remote().result() == "got g"
    url = f"{get_application_url(app_name='app_g')}"
    assert httpx.get(url).text == "got g"

    # Test function deployment with app name and route_prefix set in deployment
    # decorator
    h_handle = serve.run(h.bind(), name="app_h", route_prefix="/my_prefix")
    assert h_handle.remote().result() == "got h"
    url = f"{get_application_url(app_name='app_h')}"
    assert httpx.get(url).text == "got h"

    # Test FastAPI
    serve.run(MyFastAPIDeployment.bind(), name="FastAPI", route_prefix="/hello")
    url = f"{get_application_url(app_name='FastAPI')}"
    assert httpx.get(url, follow_redirects=True).text == '"Hello, world!"'


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
    assert f_handle.remote().result() == "got f"
    url = get_application_url("HTTP", app_name="app_f")
    assert httpx.get(url).text == "got f"

    serve.delete("app_f")
    url = "http://localhost:8000/app_f"
    assert "Path '/app_f' not found" in httpx.get(url).text

    # delete again, no exception & crash expected.
    serve.delete("app_f")

    # make sure no affect to app_g
    assert g_handle.remote().result() == "got g"
    url = get_application_url("HTTP", app_name="app_g")
    assert httpx.get(url).text == "got g"


@pytest.mark.asyncio
async def test_delete_while_initializing(serve_instance):
    """Test that __del__ runs when a replica terminates while initializing."""

    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def incr(self):
            self.count += 1

        def get_count(self) -> int:
            return self.count

    signal = SignalActor.remote()
    counter = Counter.remote()

    @serve.deployment(graceful_shutdown_timeout_s=0.01)
    class HangingStart:
        async def __init__(
            self, signal: ray.actor.ActorHandle, counter: ray.actor.ActorHandle
        ):
            self.signal = signal
            self.counter = counter
            await signal.send.remote()
            print("HangingStart set the EventHolder.")
            await asyncio.sleep(10000)

        async def __del__(self):
            print("Running __del__")
            await self.counter.incr.remote()

    serve._run(HangingStart.bind(signal, counter), _blocking=False)

    print("Waiting for the deployment to start initialization.")
    await signal.wait.remote()

    print("Calling serve.delete().")
    serve.delete(name=SERVE_DEFAULT_APP_NAME)

    # Ensure that __del__ ran once, even though the deployment terminated
    # during initialization.
    assert (await counter.get_count.remote()) == 1


def test_deployment_name_with_app_name(serve_instance):
    """Test replica name with app name as prefix"""

    controller = serve_instance._controller

    @serve.deployment
    def g():
        return "got g"

    serve.run(g.bind())
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID(name="g") in deployment_info

    @serve.deployment
    def f():
        return "got f"

    serve.run(f.bind(), route_prefix="/f", name="app1")
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID(name="f", app_name="app1") in deployment_info


def test_deploy_application_with_same_name(serve_instance):
    """Test deploying two applications with the same name."""

    controller = serve_instance._controller

    @serve.deployment
    class Model:
        def __call__(self):
            return "got model"

    handle = serve.run(Model.bind(), name="app")
    assert handle.remote().result() == "got model"
    url = get_application_url("HTTP", app_name="app")
    assert httpx.get(url).text == "got model"
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID(name="Model", app_name="app") in deployment_info

    # After deploying a new app with the same name, no Model replicas should be running
    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    handle = serve.run(Model1.bind(), name="app")
    assert handle.remote().result() == "got model1"
    url = get_application_url("HTTP", app_name="app")
    assert httpx.get(url).text == "got model1"
    deployment_info = ray.get(controller._all_running_replicas.remote())
    assert DeploymentID(name="Model1", app_name="app") in deployment_info
    assert (
        DeploymentID(name="Model", app_name="app") not in deployment_info
        or deployment_info[DeploymentID(name="Model", app_name="app")] == []
    )

    # Redeploy with same app to update route prefix
    serve.run(Model1.bind(), name="app", route_prefix="/my_app")
    url_new = get_application_url("HTTP", app_name="app")
    # Reread the url to get the correct port value
    old_url_route_prefix = "/"
    url = (
        get_application_url("HTTP", app_name="app", exclude_route_prefix=True)
    ) + old_url_route_prefix

    assert httpx.get(url_new).text == "got model1"
    assert httpx.get(url).status_code == 404


def test_deploy_application_with_route_prefix_conflict(serve_instance):
    """Test route_prefix conflicts with different apps."""

    @serve.deployment
    class Model:
        def __call__(self):
            return "got model"

    handle = serve.run(Model.bind(), name="app")
    assert handle.remote().result() == "got model"
    url = get_application_url("HTTP", app_name="app")
    assert httpx.get(url).text == "got model"

    # Second app with the same route_prefix fails to be deployed
    @serve.deployment
    class Model1:
        def __call__(self):
            return "got model1"

    with pytest.raises(RayServeException):
        serve.run(Model1.bind(), name="app1")

    # Update the route prefix
    handle = serve.run(Model1.bind(), name="app1", route_prefix="/model1")
    assert handle.remote().result() == "got model1"
    url_new = get_application_url("HTTP", app_name="app1")
    assert httpx.get(url_new).text == "got model1"

    # The "app" application should still work properly
    assert httpx.get(url).text == "got model"


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
        assert call_user_app_builder_with_args_if_necessary(a, {}) == a

        f = self.f.bind()
        assert call_user_app_builder_with_args_if_necessary(f, {}) == f

        with pytest.raises(
            ValueError,
            match="Arguments can only be passed to an application builder function",
        ):
            call_user_app_builder_with_args_if_necessary(f, {"key": "val"})

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
            call_user_app_builder_with_args_if_necessary(ThisShouldBeAFunction, {})

    def test_invalid_signature(self):
        def builder_with_two_args(args1, args2):
            return self.f.bind()

        with pytest.raises(
            TypeError,
            match="Application builder functions should take exactly one parameter",
        ):
            call_user_app_builder_with_args_if_necessary(builder_with_two_args, {})

    def test_builder_returns_bad_type(self):
        def return_none(args):
            self.f.bind()

        with pytest.raises(
            TypeError,
            match="Application builder functions must return a",
        ):
            call_user_app_builder_with_args_if_necessary(return_none, {})

        def return_unbound_deployment(args):
            return self.f

        with pytest.raises(
            TypeError,
            match="Application builder functions must return a",
        ):
            call_user_app_builder_with_args_if_necessary(return_unbound_deployment, {})

    def test_basic_no_args(self):
        def build_function(args):
            return self.A.bind()

        assert isinstance(
            call_user_app_builder_with_args_if_necessary(build_function, {}),
            Application,
        )

        def build_class(args):
            return self.f.bind()

        assert isinstance(
            call_user_app_builder_with_args_if_necessary(build_class, {}), Application
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

        app = call_user_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

    def test_args_typed(self):
        args_dict = {"message": "hiya", "num_replicas": "3"}

        def build(args):
            """Builder with no type hint."""

            return self.A.options(num_replicas=args["num_replicas"]).bind(
                args["message"]
            )

        app = call_user_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

        def build(args: Dict[str, str]):
            """Builder with vanilla type hint."""

            return self.A.options(num_replicas=args["num_replicas"]).bind(
                args["message"]
            )

        app = call_user_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

        class ForwardRef:
            def build(args: "ForwardRef"):
                """Builder with forward reference as type hint."""

                return self.A.options(num_replicas=args["num_replicas"]).bind(
                    args["message"]
                )

        app = call_user_app_builder_with_args_if_necessary(ForwardRef.build, args_dict)
        assert isinstance(app, Application)

        def build(args: self.TypedArgs):
            """Builder with Pydantic model type hint."""

            assert isinstance(args, self.TypedArgs)
            assert args.message == "hiya"
            assert args.num_replicas == 3
            return self.A.options(num_replicas=args.num_replicas).bind(args.message)

        app = call_user_app_builder_with_args_if_necessary(build, args_dict)
        assert isinstance(app, Application)

        # Sanity check that pydantic validation works.

        # 1) Check that validation permits a missing optional field.
        def check_missing_optional(args: self.TypedArgs):
            assert args.message == "hiya"
            assert args.num_replicas is None
            return self.A.bind()

        app = call_user_app_builder_with_args_if_necessary(
            check_missing_optional, {"message": "hiya"}
        )
        assert isinstance(app, Application)

        # 2) Check that validation rejects a missing required field.
        def check_missing_required(args: self.TypedArgs):
            assert False, "Shouldn't get here because validation failed."

        with pytest.raises(ValidationError, match="field required"):
            call_user_app_builder_with_args_if_necessary(
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

        app = call_user_app_builder_with_args_if_necessary(build, cat_dict)
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
        ValueError,
        match=(
            r"Invalid route_prefix 'no_slash', "
            r"must start with a forward slash \('/'\)"
        ),
    ):
        serve.run(f.bind(), route_prefix="no_slash")


def test_mutually_exclusive_max_replicas_per_node_and_placement_group_bundles():
    with pytest.raises(
        ValueError,
        match=(
            "Setting max_replicas_per_node is not allowed when "
            "placement_group_bundles is provided."
        ),
    ):

        @serve.deployment(max_replicas_per_node=3, placement_group_bundles=[{"CPU": 1}])
        def f():
            pass

    with pytest.raises(
        ValueError,
        match=(
            "Setting max_replicas_per_node is not allowed when "
            "placement_group_bundles is provided."
        ),
    ):

        @serve.deployment
        def g():
            pass

        g.options(max_replicas_per_node=3, placement_group_bundles=[{"CPU": 1}])


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
            self._h = handle

        async def __call__(self):
            return await self._h.remote()

    handle_1 = serve.run(A.bind(), name="plus", route_prefix="/a")
    handle_2 = serve.run(MyDriver.bind(f.bind()), name="hello", route_prefix="/b")

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
            _ = 1 / 0

    serve._run(A.bind(), _blocking=False)

    def check_for_failed_app():
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        error_substr = "ZeroDivisionError: division by zero"
        assert (
            default_app.status == "DEPLOY_FAILED"
            and error_substr in default_app.deployments["A"].message
        )
        assert default_app.deployments["A"].status == "DEPLOY_FAILED"
        return True

    wait_for_condition(check_for_failed_app)

    # Instead of hanging forever, a request to the application should
    # return a 503 error to reflect the failed deployment state.
    # The timeout is there to prevent the test from hanging and blocking
    # the test suite if it does fail.
    url = get_application_url("HTTP")
    r = httpx.post(url, timeout=10)
    assert r.status_code == 503 and "unavailable" in r.text

    @serve.deployment
    class A:
        def __init__(self):
            pass

    serve._run(A.bind(), _blocking=False)

    def check_for_running_app():
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert default_app.status == "RUNNING"
        assert default_app.deployments["A"].status == "HEALTHY"
        return True

    wait_for_condition(check_for_running_app)


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
    serve._run(
        MyDeployment.options(ray_actor_options=ray_actor_options).bind(),
        _blocking=False,
    )

    def check_for_failed_deployment():
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert default_app.status == "DEPLOY_FAILED"
        assert "some_wrong_url" in default_app.deployments["MyDeployment"].message
        return True

    wait_for_condition(check_for_failed_deployment, timeout=60)


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
            self._h = handle

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
            self._h = handle

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
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle

        def get(self) -> DeploymentHandle:
            return self._handle

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
    h = serve.run(MyDriver.bind(handle_wrapper))
    assert h.remote().result() == "hi"


def test_max_ongoing_requests_none(serve_instance):
    """We should not allow setting `max_ongoing_requests` to None."""

    def get_max_ongoing_requests():
        details = serve_instance.get_serve_details()
        return details["applications"]["default"]["deployments"]["A"][
            "deployment_config"
        ]["max_ongoing_requests"]

    class A:
        pass

    with pytest.raises(ValueError):
        serve.deployment(max_ongoing_requests=None)(A).bind()
    with pytest.raises(ValueError):
        serve.deployment(A).options(max_ongoing_requests=None).bind()

    serve.run(serve.deployment(A).bind())
    assert get_max_ongoing_requests() == DEFAULT_MAX_ONGOING_REQUESTS

    serve.run(
        serve.deployment(max_ongoing_requests=8, graceful_shutdown_timeout_s=2)(
            A
        ).bind()
    )
    assert get_max_ongoing_requests() == 8

    serve.run(serve.deployment(A).options(max_ongoing_requests=12).bind())
    assert get_max_ongoing_requests() == 12


def test_deploy_app_with_custom_request_router(serve_instance):
    """Test deploying an app with a custom request router configured in the
    deployment decorator."""

    handle = serve.run(AppWithCustomRequestRouter.bind())
    assert handle.remote().result() == "Hello, world!"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
