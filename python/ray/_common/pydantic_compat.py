# ruff: noqa
import packaging.version

# Pydantic is a dependency of `ray["default"]` but not the minimal installation,
# so handle the case where it isn't installed.
try:
    import pydantic

    PYDANTIC_INSTALLED = True
except ImportError:
    pydantic = None
    PYDANTIC_INSTALLED = False


if not PYDANTIC_INSTALLED:
    IS_PYDANTIC_2 = False
    BaseModel = None
    Extra = None
    Field = None
    NonNegativeFloat = None
    NonNegativeInt = None
    PositiveFloat = None
    PositiveInt = None
    PrivateAttr = None
    StrictInt = None
    ValidationError = None
    root_validator = None
    validator = None
    is_subclass_of_base_model = lambda obj: False
elif not hasattr(pydantic, "__version__") or packaging.version.parse(
    pydantic.__version__
) < packaging.version.parse("2.0"):
    raise ImportError(
        "Pydantic v1 is no longer supported in Ray. "
        "Please upgrade to `pydantic>=2`."
    )
else:
    IS_PYDANTIC_2 = True
    from pydantic import (
        BaseModel,
        Extra,
        Field,
        NonNegativeFloat,
        NonNegativeInt,
        PositiveFloat,
        PositiveInt,
        PrivateAttr,
        StrictInt,
        ValidationError,
        root_validator,
        validator,
    )

    def is_subclass_of_base_model(obj):
        return issubclass(obj, BaseModel)


def register_pydantic_serializers(serialization_context):
    if not PYDANTIC_INSTALLED:
        return

    # FastAPI still uses Pydantic's v1 ModelField compatibility type internally.
    from pydantic.v1.fields import ModelField

    # Pydantic's Cython validators are not serializable.
    # https://github.com/cloudpipe/cloudpickle/issues/408
    serialization_context._register_cloudpickle_serializer(
        ModelField,
        custom_serializer=lambda o: {
            "name": o.name,
            # outer_type_ is the original type for ModelFields,
            # while type_ can be updated later with the nested type
            # like int for List[int].
            "type_": o.outer_type_,
            "class_validators": o.class_validators,
            "model_config": o.model_config,
            "default": o.default,
            "default_factory": o.default_factory,
            "required": o.required,
            "alias": o.alias,
            "field_info": o.field_info,
        },
        custom_deserializer=lambda kwargs: ModelField(**kwargs),
    )
