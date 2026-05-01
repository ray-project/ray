"""
This module is intended for implementing internal serializers for some
site packages.
"""

import sys

from ray.util.annotations import DeveloperAPI


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


@DeveloperAPI
def register_pydantic_serializers(serialization_context):
    try:
        import pydantic  # noqa: F401
    except ImportError:
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


@DeveloperAPI
def register_starlette_serializer(serialization_context):
    try:
        import starlette.datastructures
    except ImportError:
        return

    # Starlette's app.state object is not serializable
    # because it overrides __getattr__
    serialization_context._register_cloudpickle_serializer(
        starlette.datastructures.State,
        custom_serializer=lambda s: s._state,
        custom_deserializer=lambda s: starlette.datastructures.State(s),
    )


@DeveloperAPI
def apply(serialization_context):
    register_pydantic_serializers(serialization_context)
    register_starlette_serializer(serialization_context)

    if sys.platform != "win32":
        from ray._private.arrow_serialization import (
            _register_custom_datasets_serializers,
        )

        _register_custom_datasets_serializers(serialization_context)
