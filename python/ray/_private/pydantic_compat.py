# flake8: noqa
import weakref
from pkg_resources import packaging
from typing import Any

# Pydantic is a dependency of `ray["default"]` but not the minimal installation,
# so handle the case where it isn't installed.
try:
    import pydantic

    PYDANTIC_INSTALLED = True
except ImportError:
    pydantic = None
    PYDANTIC_INSTALLED = False


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
    """Patches non-serializable types introduced in Pydantic 2.0.

    See https://github.com/pydantic/pydantic/issues/6763 for details.

    This is a temporary workaround and will only work if Ray is imported *before*
    Pydantic models are defined.
    """
    pydantic._internal._model_construction.SchemaSerializer = (
        CloudpickleableSchemaSerializer
    )
    pydantic._internal._dataclasses.SchemaSerializer = CloudpickleableSchemaSerializer
    pydantic.type_adapter.SchemaSerializer = CloudpickleableSchemaSerializer
    pydantic._internal._model_construction._PydanticWeakRef = WeakRefWrapper


if not PYDANTIC_INSTALLED:
    IS_PYDANTIC_2 = False
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
# In pydantic <1.9.0, __version__ attribute is missing, issue ref:
# https://github.com/pydantic/pydantic/issues/2572, so we need to check
# the existence prior to comparison.
elif hasattr(pydantic, "__version__") and packaging.version.parse(
    pydantic.__version__
) < packaging.version.parse("2.0"):
    IS_PYDANTIC_2 = False
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
else:
    IS_PYDANTIC_2 = True
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
