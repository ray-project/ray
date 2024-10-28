import inspect
import tempfile
import time
from typing import Any, List, Optional

import pytest
import requests
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
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.exceptions import GetTimeoutError
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.http_util import make_fastapi_class_based_view
from ray.serve._private.utils import DEFAULT
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

    resp = requests.get("http://localhost:8000/100")
    assert resp.json() == {"result": 100}

    resp = requests.get("http://localhost:8000/not-number")
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

    resp = requests.get("http://localhost:8000/api/100")
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
    resp = requests.get("http://localhost:8000/calc/41")
    assert resp.json() == 42
    resp = requests.post("http://localhost:8000/calc/41")
    assert resp.json() == 40
    resp = requests.get("http://localhost:8000/other")
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

    url = "http://localhost:8000"
    resp = requests.get(f"{url}/")
    assert resp.status_code == 404
    assert "x-process-time" in resp.headers

    resp = requests.get(f"{url}/my_api.json")
    assert resp.status_code == 200
    assert resp.json()  # it returns a well-formed json.

    resp = requests.get(f"{url}/docs")
    assert resp.status_code == 200
    assert "<!DOCTYPE html>" in resp.text

    resp = requests.get(f"{url}/redoc")
    assert resp.status_code == 200
    assert "<!DOCTYPE html>" in resp.text

    resp = requests.get(f"{url}/path_arg")
    assert resp.status_code == 422  # Malformed input

    resp = requests.get(
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
        "python-requests",
        {"q": "common_arg"},
        "db",
        "app.state",
    ]
    assert open(resp.json()["file_path"]).read() == "hello"

    resp = requests.get(
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

    resp = requests.get(f"{url}/prefix/subpath")
    assert resp.status_code == 200

    resp = requests.get(
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

    assert requests.get("http://localhost:8000/api/mounted/hi").json() == "world"


def test_fastapi_init_lifespan_should_not_shutdown(serve_instance):
    app = FastAPI()

    @app.on_event("shutdown")
    async def shutdown():
        1 / 0

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

    resp = requests.get("http://localhost:8000/api/v1")
    assert resp.json() == "first"

    resp = requests.get("http://localhost:8000/api/v2")
    assert resp.json() == "second"

    for version in ["v1", "v2"]:
        resp = requests.get(f"http://localhost:8000/api/{version}/ignored")
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

    resp = requests.get("http://localhost:8000/")
    assert resp.json() == {"hello": "world"}


@pytest.mark.parametrize(
    "input_route_prefix,expected_route_prefix",
    [(DEFAULT.VALUE, "/"), ("/", "/"), ("/subpath", "/subpath/")],
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

    r = requests.get(f"http://localhost:8000{expected_route_prefix}openapi.json")
    assert r.status_code == 200
    assert len(r.json()["paths"]) == 1
    assert "/" in r.json()["paths"]
    assert len(r.json()["paths"]["/"]) == 1
    assert "get" in r.json()["paths"]["/"]

    r = requests.get(f"http://localhost:8000{expected_route_prefix}docs")
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

    r = requests.get(f"http://localhost:8000{expected_route_prefix}openapi.json")
    assert r.status_code == 200
    assert len(r.json()["paths"]) == 2
    assert "/" in r.json()["paths"]
    assert len(r.json()["paths"]["/"]) == 1
    assert "get" in r.json()["paths"]["/"]
    assert "/hello" in r.json()["paths"]
    assert len(r.json()["paths"]["/hello"]) == 1
    assert "post" in r.json()["paths"]["/hello"]

    r = requests.get(f"http://localhost:8000{expected_route_prefix}docs")
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

    resp = requests.get("http://localhost:8000/")
    assert resp.cookies.get_dict() == {"a": "b", "c": "d"}


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

    resp = requests.get("http://localhost:8000/")
    assert resp.json() == {"a": "a", "b": ["b"]}

    resp = requests.get("http://localhost:8000/inner")
    assert resp.json() == {"a": "a", "b": ["b"]}

    resp = requests.get("http://localhost:8000/inner2")
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
    resp = requests.get("http://localhost:8000/")
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
    assert requests.get("http://localhost:8000/a/").json() == "hi get"
    assert requests.post("http://localhost:8000/a/").json() == "hi post"


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

    should_work = [
        ("/app1/incr", "incr"),
        ("/app1/decr", "decr"),
        ("/app2/incr2", "incr2"),
        ("/app2/decr2", "decr2"),
    ]
    for path, resp in should_work:
        assert requests.get("http://localhost:8000" + path).json() == resp, (path, resp)

    should_404 = [
        "/app2/incr",
        "/app2/decr",
        "/app1/incr2",
        "/app1/decr2",
    ]
    for path in should_404:
        assert requests.get("http://localhost:8000" + path).status_code == 404, path


@pytest.mark.parametrize("two_fastapi", [True, False])
def test_two_fastapi_in_one_application(
    serve_instance: ServeControllerClient, two_fastapi
):
    """
    Check that a deployment graph that would normally work, will not deploy
    successfully if there are two FastAPI deployments.
    """
    app1 = FastAPI()
    app2 = FastAPI()

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
