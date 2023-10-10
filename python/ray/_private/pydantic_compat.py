# flake8: noqa
import weakref
from pkg_resources import packaging
from typing import Any

# Pydantic is a dependency of `ray["default"]` but not the minimal installation,
# so handle the case where it isn't installed.
try:
    import pydantic
except ImportError:
    pydantic = None


class CloudpickleableSchemaSerializer:
    def __init__(self, schema, core_config):
        self._schema = schema
        self._core_config = core_config

        from pydantic_core import SchemaSerializer

        self._schema_serializer = SchemaSerializer(schema, core_config)

    def __reduce__(self):
        return CloudpickleableSchemaSerializer, (self._schema, self._core_config)

    def __getattr__(self, attr: str):
        return getattr(self._schema_serializer, attr)


class WeakRefWrapper:
    def __init__(self, obj: Any):
        if obj is None:
            self._wr = None
        else:
            self._wr = weakref.ref(obj)

    def __reduce__(self):
        return WeakRefWrapper, (self(),)

    def __call__(self) -> Any:
        if self._wr is None:
            return None
        else:
            return self._wr()


def monkeypatch_pydantic_2_for_cloudpickle():
    """XXX"""
    pydantic._internal._model_construction.SchemaSerializer = (
        CloudpickleableSchemaSerializer
    )
    pydantic._internal._dataclasses.SchemaSerializer = CloudpickleableSchemaSerializer
    pydantic.type_adapter.SchemaSerializer = CloudpickleableSchemaSerializer
    pydantic._internal._model_construction._PydanticWeakRef = WeakRefWrapper


if pydantic is None:
    BaseModel = None
    Extra = None
    Field = None
    NonNegativeFloat = None
    NonNegativeInt = None
    PositiveFloat = None
    PositiveInt = None
    ValidationError = None
    root_validator = None
    validator = None
    ModelMetaclass = None
elif packaging.version.parse(pydantic.__version__) > packaging.version.parse("2.0"):
    # TODO(edoakes): compare this against the version that has the fixes.
    monkeypatch_pydantic_2_for_cloudpickle()
    from pydantic.v1 import (
        BaseModel,
        Extra,
        Field,
        NonNegativeFloat,
        NonNegativeInt,
        PositiveFloat,
        PositiveInt,
        ValidationError,
        root_validator,
        validator,
    )

    # TODO: we shouldn't depend on this path in pydantic>=2.0 as it's not public.
    from pydantic._internal._model_construction import ModelMetaclass
elif pydantic is not None:
    from pydantic import (
        BaseModel,
        Extra,
        Field,
        NonNegativeFloat,
        NonNegativeInt,
        PositiveFloat,
        PositiveInt,
        ValidationError,
        root_validator,
        validator,
    )
    from pydantic.main import ModelMetaclass
