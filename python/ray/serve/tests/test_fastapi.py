import time
from typing import Any, List, Optional
import tempfile

import pytest
import inspect
import requests
from fastapi import Cookie, Depends, FastAPI, Header, Query, Request, APIRouter, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ray import serve
from ray.serve.utils import make_fastapi_class_based_view


def test_fastapi_function(serve_instance):
    client = serve_instance
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.ingress(app)
    class FastAPIApp:
        pass

    client.deploy("f", FastAPIApp)

    resp = requests.get(f"http://localhost:8000/f/100")
    assert resp.json() == {"result": 100}

    resp = requests.get(f"http://localhost:8000/f/not-number")
    assert resp.status_code == 422  # Unprocessable Entity
    assert resp.json()["detail"][0]["type"] == "type_error.integer"


def test_ingress_prefix(serve_instance):
    client = serve_instance
    app = FastAPI()

    @app.get("/{a}")
    def func(a: int):
        return {"result": a}

    @serve.ingress(app, path_prefix="/api")
    class App:
        pass

    client.deploy("f", App)

    resp = requests.get(f"http://localhost:8000/api/100")
    assert resp.json() == {"result": 100}


def test_class_based_view(serve_instance):
    client = serve_instance
    app = FastAPI()

    @app.get("/other")
    def hello():
        return "hello"

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

    client.deploy("f", A)
    resp = requests.get(f"http://localhost:8000/f/calc/41")
    assert resp.json() == 42
    resp = requests.post(f"http://localhost:8000/f/calc/41")
    assert resp.json() == 40
    resp = requests.get(f"http://localhost:8000/f/other")
    assert resp.json() == "hello"


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
    client = serve_instance
    app = FastAPI(openapi_url="/my_api.json")

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
        nests: List[Nested]

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

    app.add_middleware(CORSMiddleware)

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
                body_val.nests[0].val,
                do_error,
                query_arg_valid,
                cookie_arg,
                user_agent,
                commons,
                db,
            ],
            file_path=path,
        )

    router = APIRouter(prefix="/prefix")

    @router.get("/subpath")
    def router_path():
        return "ok"

    app.include_router(router)

    @serve.deployment("fastapi")
    @serve.ingress(app)
    class Worker:
        pass

    Worker.deploy()

    url = "http://localhost:8000/fastapi"
    resp = requests.get(f"{url}")
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
    assert resp.status_code == 522  # Malformed input

    resp = requests.get(
        f"{url}/path_arg",
        json={
            "name": "serve",
            "price": 12,
            "nests": [{
                "val": 1
            }]
        },
        params={
            "query_arg": "query_arg",
            "query_arg_valid": "at-least-three-chars",
            "q": "common_arg",
        })
    assert resp.json()["ok"]
    assert resp.json()["vals"] == [
        "path_arg",
        "query_arg",
        12.0,
        1,
        False,
        "at-least-three-chars",
        None,
        "python-requests/2.24.0",
        {
            "q": "common_arg"
        },
        "db",
    ]
    assert open(resp.json()["file_path"]).read() == "hello"

    resp = requests.get(
        f"{url}/path_arg",
        json={
            "name": "serve",
            "price": 12,
            "nests": [{
                "val": 1
            }]
        },
        params={
            "query_arg": "query_arg",
            "query_arg_valid": "at-least-three-chars",
            "q": "common_arg",
            "do_error": "true"
        })
    assert resp.status_code == 500
    assert resp.json()["custom_error"] == "true"

    resp = requests.get(
        f"{url}/docs",
        headers={
            "Access-Control-Request-Method": "GET",
            "Origin": "https://googlebot.com"
        })
    assert resp.headers["access-control-allow-origin"] == "*"

    resp = requests.get(f"{url}/prefix/subpath")
    assert resp.status_code == 200


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
