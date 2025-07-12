import inspect
import tempfile
import time
from typing import Any, List, Optional

import httpx
import pytest
import starlette.responses
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Cookie,
    Depends,
    FastAPI,
    Header,
    Query,
    Request,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.applications import Starlette
from starlette.routing import Route

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.exceptions import GetTimeoutError
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.http_util import make_fastapi_class_based_view
from ray.serve._private.test_utils import get_application_url
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle


def test_fastapi_function(serve_instance):
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.deployment
    @serve.ingress(app)
    class FastAPIApp:
        pass

    serve.run(FastAPIApp.bind())

    url = get_application_url("HTTP")

    resp = httpx.get(f"{url}/100")
    assert resp.json() == {"result": 100}

    resp = httpx.get(f"{url}/not-number")
    assert resp.status_code == 422  # Unprocessable Entity
    # Pydantic 1.X returns `type_error.integer`, 2.X returns `int_parsing`.
    assert resp.json()["detail"][0]["type"] in {"type_error.integer", "int_parsing"}


def test_ingress_prefix(serve_instance):
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.deployment
    @serve.ingress(app)
    class App:
        pass

    serve.run(App.bind(), route_prefix="/api")

    url = get_application_url("HTTP")
    resp = httpx.get(f"{url}/100")
    assert resp.json() == {"result": 100}


def test_class_based_view(serve_instance):
    app = FastAPI()

    @app.get("/other")
    def hello():
        return "hello"

    @serve.deployment
    @serve.ingress(app)
    class A:
        def __init__(self):
            self.val = 1

        @app.get("/calc/{i}")
        def b(self, i: int):
            return i + self.val

        @app.post("/calc/{i}")
        def c(self, i: int):
            return i - self.val

        def other(self, msg: str):
            return msg

    serve.run(A.bind())

    # Test HTTP calls.
    url = get_application_url("HTTP")
    resp = httpx.get(f"{url}/calc/41")
    assert resp.json() == 42
    resp = httpx.post(f"{url}/calc/41")
    assert resp.json() == 40
    resp = httpx.get(f"{url}/other")
    assert resp.json() == "hello"

    # Test handle calls.
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    assert handle.b.remote(41).result() == 42
    assert handle.c.remote(41).result() == 40
    assert handle.other.remote("world").result() == "world"


@pytest.mark.parametrize("websocket", [False, True])
def test_make_fastapi_class_based_view(websocket: bool):
    app = FastAPI()

    if websocket:

        class A:
            @app.get("/{i}")
            def b(self, i: int):
                pass

    else:

        class A:
            @app.websocket("/{i}")
            def b(self, i: int):
                pass

    # before, "self" is treated as a query params
    assert app.routes[-1].endpoint == A.b
    assert app.routes[-1].dependant.query_params[0].name == "self"
    assert len(app.routes[-1].dependant.dependencies) == 0

    make_fastapi_class_based_view(app, A)

    # after, "self" is treated as a dependency instead of query params
    assert app.routes[-1].endpoint == A.b
    assert len(app.routes[-1].dependant.query_params) == 0
    assert len(app.routes[-1].dependant.dependencies) == 1
    self_dep = app.routes[-1].dependant.dependencies[0]
    assert self_dep.name == "self"
    assert inspect.isfunction(self_dep.call)
    assert "get_current_servable" in str(self_dep.call)


class Nested(BaseModel):
    val: int


class BodyType(BaseModel):
    name: str
    price: float = Field(None, gt=1.0, description="High price!")
    nests: Nested


class RespModel(BaseModel):
    ok: bool
    vals: List[Any]
    file_path: str


def test_fastapi_features(serve_instance):
    app = FastAPI(openapi_url="/my_api.json")

    @app.on_event("startup")
    def inject_state():
        app.state.state_one = "app.state"

    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response

    async def yield_db():
        yield "db"

    async def common_parameters(q: Optional[str] = None):
        return {"q": q}

    @app.exception_handler(ValueError)
    async def custom_handler(_: Request, exc: ValueError):
        return JSONResponse(
            status_code=500, content={"custom_error": "true", "message": str(exc)}
        )

    def run_background(background_tasks: BackgroundTasks):
        _, path = tempfile.mkstemp()

        def write_to_file(p):
            with open(p, "w") as f:
                f.write("hello")

        background_tasks.add_task(write_to_file, path)
        return path

    app.add_middleware(CORSMiddleware, allow_origins="*")

    @app.get("/{path_arg}", response_model=RespModel, status_code=201)
    async def func(
        path_arg: str,
        query_arg: str,
        body_val: BodyType,
        backgrounds_tasks: BackgroundTasks,
        do_error: bool = False,
        query_arg_valid: Optional[str] = Query(None, min_length=3),
        cookie_arg: Optional[str] = Cookie(None),
        user_agent: Optional[str] = Header(None),
        commons: dict = Depends(common_parameters),
        db=Depends(yield_db),
    ):
        if do_error:
            raise ValueError("bad input")

        path = run_background(backgrounds_tasks)

        return RespModel(
            ok=True,
            vals=[
                path_arg,
                query_arg,
                body_val.price,
                body_val.nests.val,
                do_error,
                query_arg_valid,
                cookie_arg,
                user_agent.split("/")[0],  # returns python-requests
                commons,
                db,
                app.state.state_one,
            ],
            file_path=path,
        )

    router = APIRouter(prefix="/prefix")

    @router.get("/subpath")
    def router_path():
        return "ok"

    app.include_router(router)

    @serve.deployment
    @serve.ingress(app)
    class Worker:
        pass

    serve.run(Worker.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(f"{url}/")
    assert resp.status_code == 404
    assert "x-process-time" in resp.headers

    resp = httpx.get(f"{url}/my_api.json")
    assert resp.status_code == 200
    assert resp.json()  # it returns a well-formed json.

    resp = httpx.get(f"{url}/docs")
    assert resp.status_code == 200
    assert "<!DOCTYPE html>" in resp.text

    resp = httpx.get(f"{url}/redoc")
    assert resp.status_code == 200
    assert "<!DOCTYPE html>" in resp.text

    resp = httpx.get(f"{url}/path_arg")
    assert resp.status_code == 422  # Malformed input

    # Including a body in a GET request is against HTTP/1.1
    # spec (RFC 7231) and is discouraged, even though some
    # servers/libraries may accept it.
    resp = httpx.request(
        "GET",
        f"{url}/path_arg",
        json={"name": "serve", "price": 12, "nests": {"val": 1}},
        params={
            "query_arg": "query_arg",
            "query_arg_valid": "at-least-three-chars",
            "q": "common_arg",
        },
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["ok"]
    assert resp.json()["vals"] == [
        "path_arg",
        "query_arg",
        12.0,
        1,
        False,
        "at-least-three-chars",
        None,
        "python-httpx",
        {"q": "common_arg"},
        "db",
        "app.state",
    ]
    assert open(resp.json()["file_path"]).read() == "hello"

    resp = httpx.request(
        "GET",
        f"{url}/path_arg",
        json={"name": "serve", "price": 12, "nests": {"val": 1}},
        params={
            "query_arg": "query_arg",
            "query_arg_valid": "at-least-three-chars",
            "q": "common_arg",
            "do_error": "true",
        },
    )
    assert resp.status_code == 500
    assert resp.json()["custom_error"] == "true"

    resp = httpx.get(f"{url}/prefix/subpath")
    assert resp.status_code == 200

    resp = httpx.get(
        f"{url}/docs",
        headers={
            "Access-Control-Request-Method": "GET",
            "Origin": "https://googlebot.com",
        },
    )
    assert resp.headers["access-control-allow-origin"] == "*", resp.headers


def test_fast_api_mounted_app(serve_instance):
    app = FastAPI()
    subapp = FastAPI()

    @subapp.get("/hi")
    def hi():
        return "world"

    app.mount("/mounted", subapp)

    @serve.deployment
    @serve.ingress(app)
    class A:
        pass

    serve.run(A.bind(), route_prefix="/api")

    url = get_application_url("HTTP")
    assert httpx.get(f"{url}/mounted/hi").json() == "world"


def test_fastapi_init_lifespan_should_not_shutdown(serve_instance):
    app = FastAPI()

    @app.on_event("shutdown")
    async def shutdown():
        _ = 1 / 0

    @serve.deployment
    @serve.ingress(app)
    class A:
        def f(self):
            return 1

    handle = serve.run(A.bind())
    # Without a proper fix, the actor won't be initialized correctly.
    # Because it will crash on each startup.
    assert handle.f.remote().result() == 1


def test_fastapi_lifespan_startup_failure_crashes_actor(serve_instance):
    async def lifespan(app):
        raise Exception("crash")

        yield

    app = FastAPI(lifespan=lifespan)

    @serve.deployment
    @serve.ingress(app)
    class A:
        pass

    with pytest.raises(RuntimeError):
        serve.run(A.bind())


def test_fastapi_duplicate_routes(serve_instance):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class App1:
        @app.get("/")
        def func_v1(self):
            return "first"

    @serve.deployment
    @serve.ingress(app)
    class App2:
        @app.get("/")
        def func_v2(self):
            return "second"

    @app.get("/ignored")
    def ignored():
        pass

    serve.run(App1.bind(), name="app1", route_prefix="/api/v1")
    serve.run(App2.bind(), name="app2", route_prefix="/api/v2")
    app1_url = get_application_url("HTTP", app_name="app1")
    app2_url = get_application_url("HTTP", app_name="app2")

    resp = httpx.get(app1_url, follow_redirects=True)
    assert resp.json() == "first"

    resp = httpx.get(app2_url, follow_redirects=True)
    assert resp.json() == "second"

    for version in [app1_url, app2_url]:
        resp = httpx.get(f"{version}/ignored")
        assert resp.status_code == 404


def test_asgi_compatible(serve_instance):
    async def homepage(_):
        return starlette.responses.JSONResponse({"hello": "world"})

    app = Starlette(routes=[Route("/", homepage)])

    @serve.deployment
    @serve.ingress(app)
    class MyApp:
        pass

    serve.run(MyApp.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == {"hello": "world"}


@pytest.mark.parametrize(
    "input_route_prefix,expected_route_prefix",
    [("/", "/"), ("/subpath", "/subpath/")],
)
def test_doc_generation(serve_instance, input_route_prefix, expected_route_prefix):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class App:
        @app.get("/")
        def func1(self, arg: str):
            return "hello"

    serve.run(App.bind(), route_prefix=input_route_prefix)

    url = get_application_url("HTTP")
    assert expected_route_prefix.rstrip("/") in url
    r = httpx.get(f"{url}/openapi.json")
    assert r.status_code == 200
    assert len(r.json()["paths"]) == 1
    assert "/" in r.json()["paths"]
    assert len(r.json()["paths"]["/"]) == 1
    assert "get" in r.json()["paths"]["/"]

    r = httpx.get(f"{url}/docs")
    assert r.status_code == 200

    @serve.deployment
    @serve.ingress(app)
    class App:
        @app.get("/")
        def func1(self, arg: str):
            return "hello"

        @app.post("/hello")
        def func2(self, arg: int):
            return "hello"

    serve.run(App.bind(), route_prefix=input_route_prefix)

    url = get_application_url("HTTP")
    assert expected_route_prefix.rstrip("/") in url
    r = httpx.get(f"{url}/openapi.json")
    assert r.status_code == 200
    assert len(r.json()["paths"]) == 2
    assert "/" in r.json()["paths"]
    assert len(r.json()["paths"]["/"]) == 1
    assert "get" in r.json()["paths"]["/"]
    assert "/hello" in r.json()["paths"]
    assert len(r.json()["paths"]["/hello"]) == 1
    assert "post" in r.json()["paths"]["/hello"]

    r = httpx.get(f"{url}/docs")
    assert r.status_code == 200


def test_fastapi_multiple_headers(serve_instance):
    # https://fastapi.tiangolo.com/advanced/response-cookies/
    app = FastAPI()

    @app.get("/")
    def func(resp: Response):
        resp.set_cookie(key="a", value="b")
        resp.set_cookie(key="c", value="d")
        return "hello"

    @serve.deployment
    @serve.ingress(app)
    class FastAPIApp:
        pass

    serve.run(FastAPIApp.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert dict(resp.cookies) == {"a": "b", "c": "d"}


class TestModel(BaseModel):
    a: str
    # https://github.com/ray-project/ray/issues/16757
    b: List[str]


def test_fastapi_nested_field_in_response_model(serve_instance):

    app = FastAPI()

    @app.get("/", response_model=TestModel)
    def test_endpoint():
        test_model = TestModel(a="a", b=["b"])
        return test_model

    @serve.deployment
    @serve.ingress(app)
    class TestDeployment:
        # https://github.com/ray-project/ray/issues/17363
        @app.get("/inner", response_model=TestModel)
        def test_endpoint_2(self):
            test_model = TestModel(a="a", b=["b"])
            return test_model

        # https://github.com/ray-project/ray/issues/24710
        @app.get("/inner2", response_model=List[TestModel])
        def test_endpoint_3(self):
            test_model = TestModel(a="a", b=["b"])
            return [test_model]

    serve.run(TestDeployment.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == {"a": "a", "b": ["b"]}

    resp = httpx.get(f"{url}/inner")
    assert resp.json() == {"a": "a", "b": ["b"]}

    resp = httpx.get(f"{url}/inner2")
    assert resp.json() == [{"a": "a", "b": ["b"]}]


def test_fastapiwrapper_constructor_before_startup_hooks(serve_instance):
    """
    Tests that the class constructor is called before the startup hooks
    are run in FastAPIWrapper. SignalActor event is set from a startup hook
    and is awaited in the class constructor. If the class constructor is run
    before the startup hooks, the SignalActor event will time out while waiting
    and the test will pass.
    """
    app = FastAPI()
    signal = SignalActor.remote()

    @app.on_event("startup")
    def startup_event():
        ray.get(signal.send.remote())

    @serve.deployment
    @serve.ingress(app)
    class TestDeployment:
        def __init__(self):
            self.test_passed = False
            try:
                ray.get(signal.wait.remote(), timeout=0.1)
                self.test_passed = False
            except GetTimeoutError:
                self.test_passed = True

        @app.get("/")
        def root(self):
            return self.test_passed

    serve.run(TestDeployment.bind())
    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json()


def test_fastapi_shutdown_hook(serve_instance):
    # https://github.com/ray-project/ray/issues/18349
    shutdown_signal = SignalActor.remote()
    del_signal = SignalActor.remote()

    app = FastAPI()

    @app.on_event("shutdown")
    def call_signal():
        shutdown_signal.send.remote()

    @serve.deployment
    @serve.ingress(app)
    class A:
        def __del__(self):
            del_signal.send.remote()

    serve.run(A.bind())
    serve.delete(SERVE_DEFAULT_APP_NAME)
    ray.get(shutdown_signal.wait.remote(), timeout=20)
    ray.get(del_signal.wait.remote(), timeout=20)


def test_fastapi_shutdown_hook_async(serve_instance):
    # https://github.com/ray-project/ray/issues/41261
    shutdown_signal = SignalActor.remote()
    del_signal = SignalActor.remote()

    app = FastAPI()

    @app.on_event("shutdown")
    def call_signal():
        shutdown_signal.send.remote()

    @serve.deployment
    @serve.ingress(app)
    class A:
        async def __del__(self):
            del_signal.send.remote()

    serve.run(A.bind())
    serve.delete(SERVE_DEFAULT_APP_NAME)
    ray.get(shutdown_signal.wait.remote(), timeout=20)
    ray.get(del_signal.wait.remote(), timeout=20)


def test_fastapi_method_redefinition(serve_instance):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class A:
        @app.get("/")
        def method(self):
            return "hi get"

        @app.post("/")  # noqa: F811 method redefinition
        def method(self):  # noqa: F811 method redefinition
            return "hi post"

    serve.run(A.bind(), route_prefix="/a")
    url = get_application_url("HTTP")
    assert httpx.get(f"{url}/").json() == "hi get"
    assert httpx.post(f"{url}/").json() == "hi post"


def test_fastapi_same_app_multiple_deployments(serve_instance):
    # https://github.com/ray-project/ray/issues/21264
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class CounterDeployment1:
        @app.get("/incr")
        def incr(self):
            return "incr"

        @app.get("/decr")
        def decr(self):
            return "decr"

    @serve.deployment
    @serve.ingress(app)
    class CounterDeployment2:
        @app.get("/incr2")
        def incr2(self):
            return "incr2"

        @app.get("/decr2")
        def decr2(self):
            return "decr2"

    serve.run(CounterDeployment1.bind(), name="app1", route_prefix="/app1")
    serve.run(CounterDeployment2.bind(), name="app2", route_prefix="/app2")

    app1_url = get_application_url("HTTP", app_name="app1")
    app2_url = get_application_url("HTTP", app_name="app2")

    should_work = [
        (app1_url, "/incr", "incr"),
        (app1_url, "/decr", "decr"),
        (app2_url, "/incr2", "incr2"),
        (app2_url, "/decr2", "decr2"),
    ]
    for url, path, resp in should_work:
        assert httpx.get(f"{url}{path}").json() == resp, (path, resp)

    should_404 = [
        (app1_url, "/incr2", 404),
        (app1_url, "/decr2", 404),
        (app2_url, "/incr", 404),
        (app2_url, "/decr", 404),
    ]
    for url, path, status_code in should_404:
        assert httpx.get(f"{url}{path}").status_code == status_code, (path, status_code)


@pytest.mark.parametrize("two_fastapi", [True, False])
@pytest.mark.parametrize("docs_url", ["/docs", None])
def test_two_fastapi_in_one_application(
    serve_instance: ServeControllerClient, two_fastapi, docs_url
):
    """
    Check that a deployment graph that would normally work, will not deploy
    successfully if there are two FastAPI deployments.
    """
    app1 = FastAPI(docs_url=docs_url)
    app2 = FastAPI(docs_url=docs_url)

    class SubModel:
        def add(self, a: int):
            return a + 1

    @serve.deployment
    @serve.ingress(app1)
    class Model:
        def __init__(self, submodel: DeploymentHandle):
            self.submodel = submodel

        @app1.get("/{a}")
        async def func(self, a: int):
            return await self.submodel.add.remote(a)

    if two_fastapi:
        SubModel = serve.deployment(serve.ingress(app2)(SubModel))
        with pytest.raises(RayServeException) as e:
            handle = serve.run(Model.bind(SubModel.bind()), name="app1")
        assert "FastAPI" in str(e.value)
    else:
        handle = serve.run(Model.bind(serve.deployment(SubModel).bind()), name="app1")
        assert handle.func.remote(5).result() == 6


@pytest.mark.parametrize(
    "is_fastapi,docs_path",
    [
        (False, None),  # Not integrated with FastAPI
        (True, "/docs"),  # Don't specify docs_url, use default
        (True, "/documentation"),  # Override default docs url
    ],
)
def test_fastapi_docs_path(
    serve_instance: ServeControllerClient, is_fastapi, docs_path
):
    # If not the default docs_url, override it.
    if docs_path != "/docs":
        app = FastAPI(docs_url=docs_path)
    else:
        app = FastAPI()

    class Model:
        @app.get("/{a}")
        def func(a: int):
            return {"result": a}

    if is_fastapi:
        Model = serve.ingress(app)(Model)

    serve.run(serve.deployment(Model).bind(), name="app1")
    wait_for_condition(
        lambda: ray.get(serve_instance._controller.get_docs_path.remote("app1"))
        == docs_path
    )


def fastapi_builder():
    app = FastAPI(docs_url="/custom-docs")

    @app.get("/")
    def f1():
        return "hello"

    router = APIRouter()

    @router.get("/f2")
    def f2():
        return "hello f2"

    @router.get("/error")
    def error():
        raise ValueError("some error")

    app.include_router(router)

    # add a middleware
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Custom-Middleware"] = "fake-middleware"
        return response

    # custom exception handler
    @app.exception_handler(ValueError)
    async def custom_exception_handler(request: Request, exc: ValueError):
        return JSONResponse(status_code=500, content={"error": "fake-error"})

    return app


def test_ingress_with_fastapi_routes_outside_deployment(serve_instance):
    app = fastapi_builder()

    @serve.deployment
    @serve.ingress(app)
    class ASGIIngress:
        @app.get("/class_route")
        def class_route(self):
            return "hello class route"

    serve.run(ASGIIngress.bind())
    url = get_application_url("HTTP")
    assert httpx.get(url).json() == "hello"
    assert httpx.get(f"{url}/f2").json() == "hello f2"
    assert httpx.get(f"{url}/class_route").json() == "hello class route"
    assert httpx.get(f"{url}/error").status_code == 500
    assert httpx.get(f"{url}/error").json() == {"error": "fake-error"}

    # get the docs path from the controller
    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path == "/custom-docs"


def test_ingress_with_fastapi_with_no_deployment_class(serve_instance):
    app = fastapi_builder()

    ingress_deployment = serve.deployment(serve.ingress(app)())
    assert ingress_deployment.name == "ASGIIngressDeployment"
    serve.run(ingress_deployment.bind())
    url = get_application_url("HTTP")
    assert httpx.get(url).json() == "hello"
    assert httpx.get(f"{url}/f2").json() == "hello f2"
    assert httpx.get(f"{url}/error").status_code == 500
    assert httpx.get(f"{url}/error").json() == {"error": "fake-error"}

    # get the docs path from the controller
    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path == "/custom-docs"


def test_ingress_with_fastapi_builder_function(serve_instance):
    ingress_deployment = serve.deployment(serve.ingress(fastapi_builder)())
    serve.run(ingress_deployment.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == "hello"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/f2")
    assert resp.json() == "hello f2"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/error")
    assert resp.status_code == 500
    assert resp.json() == {"error": "fake-error"}

    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path == "/custom-docs"


def test_ingress_with_fastapi_builder_with_deployment_class(serve_instance):
    @serve.deployment
    @serve.ingress(fastapi_builder)
    class ASGIIngress:
        def __init__(self):
            pass

    serve.run(ASGIIngress.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == "hello"

    resp = httpx.get(f"{url}/f2")
    assert resp.json() == "hello f2"

    resp = httpx.get(f"{url}/error")
    assert resp.status_code == 500
    assert resp.json() == {"error": "fake-error"}

    # get the docs path from the controller
    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path == "/custom-docs"


def test_ingress_with_fastapi_with_native_deployment(serve_instance):
    app = fastapi_builder()

    class ASGIIngress:
        def __call__(self):
            pass

    with pytest.raises(ValueError) as e:
        serve.ingress(app)(ASGIIngress)
    assert "Classes passed to @serve.ingress may not have __call__ method." in str(
        e.value
    )


def sub_deployment():
    @serve.deployment
    class SubModel:
        def __call__(self, a: int):
            return a + 1

    return SubModel.options(name="sub_deployment")


def fastapi_builder_with_sub_deployment():
    app = fastapi_builder()

    def get_sub_deployment_handle():
        return serve.get_deployment_handle(sub_deployment().name, "default")

    class Data(BaseModel):
        a: int

    @app.get("/sub_deployment", response_model=Data)
    async def f(
        request: Request, handle: DeploymentHandle = Depends(get_sub_deployment_handle)
    ):
        a = int(request.query_params.get("a", 1))
        result = await handle.remote(a)
        return Data(a=result)

    return app


def test_deployment_composition_with_builder_function(serve_instance):
    @serve.deployment
    @serve.ingress(fastapi_builder_with_sub_deployment)
    class ASGIIngress:
        def __init__(self, sub_deployment: DeploymentHandle):
            self.sub_deployment = sub_deployment

    serve.run(ASGIIngress.bind(sub_deployment().bind()))

    url = get_application_url("HTTP")
    resp = httpx.get(f"{url}/sub_deployment?a=2")
    assert resp.json() == {"a": 3}


def test_deployment_composition_with_builder_function_without_decorator(serve_instance):
    app = serve.deployment(serve.ingress(fastapi_builder_with_sub_deployment)())

    # the default ingress deployment returned from serve.ingress accepts args and kwargs
    # and passes them to the deployment constructor
    serve.run(app.bind(sub_deployment().bind()))

    url = get_application_url("HTTP")
    resp = httpx.get(f"{url}/sub_deployment?a=2")
    assert resp.json() == {"a": 3}


def starlette_builder():
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import JSONResponse
    from starlette.routing import Route, Router

    # Define route handlers
    async def homepage(request):
        return JSONResponse("hello")

    async def f2(request):
        return JSONResponse("hello f2")

    async def error(request):
        raise ValueError("some error")

    # Create a router for additional routes
    router = Router(
        [
            Route("/f2", f2),
            Route("/error", error),
        ]
    )

    # Create a middleware for adding headers
    class CustomHeaderMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            response = await call_next(request)
            response.headers["X-Custom-Middleware"] = "fake-middleware"
            return response

    # Custom exception handler for ValueError
    def handle_value_error(request, exc):
        return JSONResponse(status_code=500, content={"error": "fake-error"})

    exception_handlers = {ValueError: handle_value_error}

    # Configure routes for the main app
    routes = [
        Route("/", homepage),
    ]

    # Create the Starlette app with middleware and exception handlers
    app = Starlette(
        routes=routes,
        middleware=[Middleware(CustomHeaderMiddleware)],
        exception_handlers=exception_handlers,
    )

    # Mount the router to the main app
    app.mount("/", router)

    return app


def test_ingress_with_starlette_app_with_no_deployment_class(serve_instance):
    ingress_deployment = serve.deployment(serve.ingress(starlette_builder())())
    serve.run(ingress_deployment.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == "hello"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/f2")
    assert resp.json() == "hello f2"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/error")
    assert resp.status_code == 500
    assert resp.json() == {"error": "fake-error"}

    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path is None


def test_ingress_with_starlette_builder_with_no_deployment_class(serve_instance):
    ingress_deployment = serve.deployment(serve.ingress(starlette_builder)())
    serve.run(ingress_deployment.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == "hello"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/f2")
    assert resp.json() == "hello f2"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/error")
    assert resp.status_code == 500
    assert resp.json() == {"error": "fake-error"}

    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path is None


def test_ingress_with_starlette_builder_with_deployment_class(serve_instance):
    @serve.deployment
    @serve.ingress(starlette_builder)
    class ASGIIngress:
        def __init__(self):
            pass

    serve.run(ASGIIngress.bind())

    url = get_application_url("HTTP")
    resp = httpx.get(url)
    assert resp.json() == "hello"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/f2")
    assert resp.json() == "hello f2"
    assert resp.headers["X-Custom-Middleware"] == "fake-middleware"

    resp = httpx.get(f"{url}/error")
    assert resp.status_code == 500
    assert resp.json() == {"error": "fake-error"}

    docs_path = ray.get(serve_instance._controller.get_docs_path.remote("default"))
    assert docs_path is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
