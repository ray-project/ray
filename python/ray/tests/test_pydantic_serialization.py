from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional, Type, Tuple
import os
import sys
from packaging import version

import pytest
from fastapi import FastAPI
import pydantic

try:
    # Testing with Pydantic 2
    from pydantic import BaseModel as BaseModelV2
    from pydantic.v1 import BaseModel as BaseModelV1

    from pydantic import ValidationError as ValidationErrorV2
    from pydantic.v1 import ValidationError as ValidationErrorV1

    BASE_MODELS = [BaseModelV1, BaseModelV2]
    BASE_MODEL_AND_ERRORS = [
        (BaseModelV1, ValidationErrorV1),
        (BaseModelV2, ValidationErrorV2),
    ]
except ImportError:
    # Testing with Pydantic 1
    from pydantic import BaseModel as BaseModelV1
    from pydantic import ValidationError as ValidationErrorV1

    BaseModelV2 = None
    BASE_MODELS = [BaseModelV1]
    BASE_MODEL_AND_ERRORS = [(BaseModelV1, ValidationErrorV1)]

import ray

from ray.tests.pydantic_module import User, app, user, closure


@pytest.fixture(scope="session")
def start_ray():
    ray.init(ignore_reinit_error=True)


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_cls(start_ray, BaseModel: Type):
    class User(BaseModel):
        name: str

    ray.get(ray.put(User))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_instance(start_ray, BaseModel: Type):
    class User(BaseModel):
        name: str

    ray.get(ray.put(User(name="a")))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_imported_cls(start_ray, BaseModel: Type):
    ray.get(ray.put(User))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_imported_instance(start_ray, BaseModel: Type):
    ray.get(ray.put(user))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_no_route(start_ray, BaseModel: Type):
    app = FastAPI()
    ray.get(ray.put(app))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_no_validation(start_ray, BaseModel: Type):
    app = FastAPI()

    @app.get("/")
    def hello() -> str:
        return "hi"

    ray.get(ray.put(app))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_primitive_type(start_ray, BaseModel: Type):
    app = FastAPI()

    @app.get("/")
    def hello(v: str) -> str:
        return "hi"

    ray.get(ray.put(app))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_pydantic_type_imported(start_ray, BaseModel: Type):
    app = FastAPI()

    @app.get("/")
    def hello(v: str, u: User) -> str:
        return "hi"

    ray.get(ray.put(app))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_pydantic_type_inline(start_ray, BaseModel: Type):
    class User(BaseModel):
        name: str

    app = FastAPI()

    @app.get("/")
    def hello(v: str, u: User) -> str:
        return "hi"

    ray.get(ray.put(app))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_imported(start_ray, BaseModel: Type):
    ray.get(ray.put(app))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_pydantic_type_closure_ref(start_ray, BaseModel: Type):
    class User(BaseModel):
        name: str

    def make():
        app = FastAPI()

        @app.get("/")
        def hello(v: str, u: User) -> str:
            return "hi"

        return app

    ray.get(ray.put(make))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_pydantic_type_closure_ref_import(start_ray, BaseModel: Type):
    def make():
        app = FastAPI()

        @app.get("/")
        def hello(v: str, u: User) -> str:
            return "hi"

        return app

    ray.get(ray.put(make))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_pydantic_type_closure(start_ray, BaseModel: Type):
    def make():
        class User(BaseModel):
            name: str

        app = FastAPI()

        @app.get("/")
        def hello(v: str, u: User) -> str:
            return "hi"

        return app

    ray.get(ray.put(make))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_app_imported_closure(start_ray, BaseModel: Type):
    ray.get(ray.put(closure))


# TODO: Serializing a Serve dataclass doesn't work in Pydantic 1.10 – 2.0.
@pytest.mark.parametrize("BaseModel", [BaseModelV2] if BaseModelV2 else [])
def test_serialize_serve_dataclass(start_ray, BaseModel: Type):
    @dataclass
    class BackendMetadata:
        is_blocking: bool = True
        autoscaling_config: Optional[Dict[str, Any]] = None

    class BackendConfig(BaseModel):
        internal_metadata: BackendMetadata = BackendMetadata()

    ray.get(ray.put(BackendConfig()))

    @ray.remote
    def consume(f):
        pass

    ray.get(consume.remote(BackendConfig()))


@pytest.mark.parametrize("BaseModel", BASE_MODELS)
def test_serialize_nested_field(start_ray, BaseModel: Type):
    class B(BaseModel):
        v: List[int]

    # this shouldn't error
    B(v=[1])

    @ray.remote
    def func():
        # this shouldn't error
        return B(v=[1])

    ray.get(func.remote())


@pytest.mark.skipif(
    version.parse(pydantic.__version__) < version.parse("2.6.0"),
    reason="pydantic version < 2.6.0 has a bug that ValidationError "
    "is not picklable: See "
    "https://github.com/pydantic/pydantic-core/pull/1119",
)
@pytest.mark.parametrize("base_model_and_error", BASE_MODEL_AND_ERRORS)
def test_validation_error(
    start_ray, propagate_logs, caplog, base_model_and_error: Tuple[Type, Type]
):
    BaseModel, ValidationError = base_model_and_error

    class B(BaseModel):
        s: str

    # This should error.
    with pytest.raises(ValidationError):
        B(s=None)

    @ray.remote
    def func():
        # This should also error. The problem is that Pydantic v2 ValidationError is
        # marked @final so we can't subclass it. This means Ray can't raise an exception
        # that can be caught as both `RayTaskError` and `pydantic.ValidationError`. So
        # we issue a warning and just raise it as `RayTaskError`. The user needs to use
        # `e.cause` to get the ValidationError.
        class B(BaseModel):
            s: str

        return B(v=None)

    with caplog.at_level(logging.WARNING, logger="ray.exceptions"):
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(func.remote())

    if BaseModel == BaseModelV1:
        # Pydantic v1 validation errors can be subclassed.
        assert isinstance(exc_info.value, ray.exceptions.RayTaskError)
        assert isinstance(exc_info.value, ValidationError)
    else:
        # Pydantic v2 validation errors are final, can't be subclassed.
        assert (
            "This exception is raised as RayTaskError only. You can use "
            "`ray_task_error.cause` to access the user exception."
        ) in caplog.text
        assert isinstance(exc_info.value, ray.exceptions.RayTaskError)
        assert isinstance(exc_info.value.cause, ValidationError)

    caplog.clear()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
