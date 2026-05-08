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

    def is_subclass_of_base_model(obj):
        return False

elif not hasattr(pydantic, "__version__") or packaging.version.parse(
    pydantic.__version__
) < packaging.version.parse("2.0"):
    raise ImportError(
        "Pydantic v1 is no longer supported in Ray. " "Please upgrade to `pydantic>=2`."
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


def _iter_model_field_types():
    model_field_types = []

    try:
        from pydantic.fields import ModelField as model_field_type
    except ImportError:
        pass
    else:
        model_field_types.append(model_field_type)

    try:
        from pydantic.v1.fields import ModelField as compat_model_field_type
    except ImportError:
        pass
    else:
        if compat_model_field_type not in model_field_types:
            model_field_types.append(compat_model_field_type)

    return model_field_types


def register_pydantic_serializers(serialization_context):
    if not PYDANTIC_INSTALLED:
        return

    # Pydantic's Cython validators are not serializable.
    # https://github.com/cloudpipe/cloudpickle/issues/408
    #
    # FastAPI can still surface Pydantic's v1 compatibility ModelField under
    # Pydantic v2, so we need to register serializers for both types until that
    # compatibility path is no longer used upstream.
    for model_field_type in _iter_model_field_types():
        serialization_context._register_cloudpickle_serializer(
            model_field_type,
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
            custom_deserializer=(
                lambda kwargs, model_field_type=model_field_type: model_field_type(
                    **kwargs
                )
            ),
        )
