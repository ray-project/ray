from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest
from fastapi import FastAPI
from pydantic import BaseModel
from ray.serve.utils import register_custom_serializers

import ray


@pytest.fixture(scope="session")
def start_ray():
    ray.init(ignore_reinit_error=True)
    register_custom_serializers()


def test_serialize_cls(start_ray):
    class User(BaseModel):
        name: str

    ray.get(ray.put(User))


def test_serialize_instance(start_ray):
    class User(BaseModel):
        name: str

    ray.get(ray.put(User(name="a")))


def test_serialize_imported_cls(start_ray):
    from pydantic_module import User

    ray.get(ray.put(User))


def test_serialize_imported_instance(start_ray):
    from pydantic_module import user

    ray.get(ray.put(user))


def test_serialize_app_no_route(start_ray):
    app = FastAPI()
    ray.get(ray.put(app))


def test_serialize_app_no_validation(start_ray):
    app = FastAPI()

    @app.get("/")
    def hello() -> str:
        return "hi"

    ray.get(ray.put(app))


def test_serialize_app_primitive_type(start_ray):
    app = FastAPI()

    @app.get("/")
    def hello(v: str) -> str:
        return "hi"

    ray.get(ray.put(app))


def test_serialize_app_pydantic_type_imported(start_ray):
    from pydantic_module import User

    app = FastAPI()

    @app.get("/")
    def hello(v: str, u: User) -> str:
        return "hi"

    ray.get(ray.put(app))


def test_serialize_app_pydantic_type_inline(start_ray):
    class User(BaseModel):
        name: str

    app = FastAPI()

    @app.get("/")
    def hello(v: str, u: User) -> str:
        return "hi"

    ray.get(ray.put(app))


def test_serialize_app_imported(start_ray):
    from pydantic_module import app
    ray.get(ray.put(app))


def test_serialize_app_pydantic_type_closure_ref(start_ray):
    class User(BaseModel):
        name: str

    def make():
        app = FastAPI()

        @app.get("/")
        def hello(v: str, u: User) -> str:
            return "hi"

        return app

    ray.get(ray.put(make))


def test_serialize_app_pydantic_type_closure_ref_import(start_ray):
    from pydantic_module import User

    def make():
        app = FastAPI()

        @app.get("/")
        def hello(v: str, u: User) -> str:
            return "hi"

        return app

    ray.get(ray.put(make))


def test_serialize_app_pydantic_type_closure(start_ray):
    def make():
        class User(BaseModel):
            name: str

        app = FastAPI()

        @app.get("/")
        def hello(v: str, u: User) -> str:
            return "hi"

        return app

    ray.get(ray.put(make))


def test_serialize_app_imported_closure(start_ray):
    from pydantic_module import closure
    ray.get(ray.put(closure))


def test_serialize_serve_dataclass(start_ray):
    @dataclass
    class BackendMetadata:
        accepts_batches: bool = False
        is_blocking: bool = True
        autoscaling_config: Optional[Dict[str, Any]] = None

    class BackendConfig(BaseModel):
        internal_metadata: BackendMetadata = BackendMetadata()

    ray.get(ray.put(BackendConfig()))

    @ray.remote
    def consume(f):
        pass

    ray.get(consume.remote(BackendConfig()))


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
