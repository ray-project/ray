# flake8: noqa
from pkg_resources import packaging

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
    ValidationError = None
    root_validator = None
    validator = None
    is_subclass_of_base_model = lambda obj: False
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

    def is_subclass_of_base_model(obj):
        return issubclass(obj, BaseModel)

else:
    IS_PYDANTIC_2 = True
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

    def is_subclass_of_base_model(obj):
        from pydantic import BaseModel as BaseModelV2
        from pydantic.v1 import BaseModel as BaseModelV1

        return issubclass(obj, BaseModelV1) or issubclass(obj, BaseModelV2)


def register_pydantic_serializers(serialization_context):
    if not PYDANTIC_INSTALLED:
        return

    if IS_PYDANTIC_2:
        # TODO(edoakes): compare against the version that has the fixes.
        from pydantic.v1.fields import ModelField
    else:
        from pydantic.fields import ModelField

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
