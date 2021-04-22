import time
from typing import Any, List, Optional
import tempfile

import pytest
import inspect
import requests
from fastapi import (Cookie, Depends, FastAPI, Header, Query, Request,
                     APIRouter, BackgroundTasks)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.applications import Starlette
import starlette.responses
from starlette.routing import Route

import ray
from ray import serve
from ray.serve.http_util import make_fastapi_class_based_view


def test_fastapi_function(serve_instance):
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.deployment(name="f")
    @serve.ingress(app)
    class FastAPIApp:
        pass

    FastAPIApp.deploy()

    resp = requests.get("http://localhost:8000/f/100")
    assert resp.json() == {"result": 100}

    resp = requests.get("http://localhost:8000/f/not-number")
    assert resp.status_code == 422  # Unprocessable Entity
    assert resp.json()["detail"][0]["type"] == "type_error.integer"


def test_ingress_prefix(serve_instance):
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.deployment(route_prefix="/api")
    @serve.ingress(app)
    class App:
        pass

    App.deploy()

    resp = requests.get("http://localhost:8000/api/100")
    assert resp.json() == {"result": 100}


def test_class_based_view(serve_instance):
    app = FastAPI()

    @app.get("/other")
    def hello():
        return "hello"

    @serve.deployment(name="f")
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

    A.deploy()

    # Test HTTP calls.
    resp = requests.get("http://localhost:8000/f/calc/41")
    assert resp.json() == 42
    resp = requests.post("http://localhost:8000/f/calc/41")
    assert resp.json() == 40
    resp = requests.get("http://localhost:8000/f/other")
    assert resp.json() == "hello"

    # Test handle calls.
    handle = A.get_handle()
    assert ray.get(handle.b.remote(41)) == 42
    assert ray.get(handle.c.remote(41)) == 40
    assert ray.get(handle.other.remote("world")) == "world"


def test_make_fastapi_cbv_util():
    app = FastAPI()

    class A:
        @app.get("/{i}")
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

    async def yield_db():
        yield "db"

    async def common_parameters(q: Optional[str] = None):
        return {"q": q}

    @app.exception_handler(ValueError)
    async def custom_handler(_: Request, exc: ValueError):
        return JSONResponse(
            status_code=500,
            content={
                "custom_error": "true",
                "message": str(exc)
            })

    def run_background(background_tasks: BackgroundTasks):
        path = tempfile.mktemp()

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

    @serve.deployment(name="fastapi")
    @serve.ingress(app)
    class Worker:
        pass

    Worker.deploy()

    url = "http://localhost:8000/fastapi"
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
        json={
            "name": "serve",
            "price": 12,
            "nests": {
                "val": 1
            }
        },
        params={
            "query_arg": "query_arg",
            "query_arg_valid": "at-least-three-chars",
            "q": "common_arg",
        })
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
        {
            "q": "common_arg"
        },
        "db",
        "app.state",
    ]
    assert open(resp.json()["file_path"]).read() == "hello"

    resp = requests.get(
        f"{url}/path_arg",
        json={
            "name": "serve",
            "price": 12,
            "nests": {
                "val": 1
            }
        },
        params={
            "query_arg": "query_arg",
            "query_arg_valid": "at-least-three-chars",
            "q": "common_arg",
            "do_error": "true"
        })
    assert resp.status_code == 500
    assert resp.json()["custom_error"] == "true"

    resp = requests.get(f"{url}/prefix/subpath")
    assert resp.status_code == 200

    resp = requests.get(
        f"{url}/docs",
        headers={
            "Access-Control-Request-Method": "GET",
            "Origin": "https://googlebot.com"
        })
    assert resp.headers["access-control-allow-origin"] == "*", resp.headers


def test_fastapi_duplicate_routes(serve_instance):
    app = FastAPI()

    @serve.deployment(route_prefix="/api/v1")
    @serve.ingress(app)
    class App1:
        @app.get("/")
        def func_v1(self):
            return "first"

    @serve.deployment(route_prefix="/api/v2")
    @serve.ingress(app)
    class App2:
        @app.get("/")
        def func_v2(self):
            return "second"

    @app.get("/ignored")
    def ignored():
        pass

    App1.deploy()
    App2.deploy()

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

    MyApp.deploy()

    resp = requests.get("http://localhost:8000/MyApp/")
    assert resp.json() == {"hello": "world"}


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
