"""This module is intended for implementing internal serializers for some
site packages.
"""

import sys

import packaging.version

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def register_pydantic_serializers(serialization_context):
    try:
        import pydantic
    except ImportError:
        # Pydantic is not part of the minimal Ray installation.
        return

    if not hasattr(pydantic, "__version__") or packaging.version.parse(
        pydantic.__version__
    ) < packaging.version.parse("2.0"):
        raise ImportError(
            "Pydantic v1 is no longer supported in Ray. Please upgrade to "
            "`pydantic>=2`."
        )

    try:
        from pydantic.v1.fields import ModelField
    except ImportError:
        return

    # Pydantic's Cython validators are not serializable.
    # https://github.com/cloudpipe/cloudpickle/issues/408
    #
    # Pydantic 2 still ships this compatibility ModelField, so keep its serializer
    # for code that uses `pydantic.v1` models.
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
