"""
This module is intended for implementing internal serializers for some
site packages.
"""

import sys

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def register_pydantic_serializer(serialization_context):
    try:
        from pydantic import fields
    except ImportError:
        fields = None

    try:
        from pydantic.v1 import fields as pydantic_v1_fields
    except ImportError:
        pydantic_v1_fields = None

    if hasattr(fields, "ModelField"):
        ModelField = fields.ModelField
    elif pydantic_v1_fields:
        ModelField = pydantic_v1_fields.ModelField
    else:
        ModelField = None

    if ModelField is not None:
        # In Pydantic 2.x, ModelField has been removed so this serialization
        # strategy no longer works. We keep this code around to allow support
        # for users with pydantic 1.x installed or users using pydantic.v1
        # import within the pydantic 2.x package.
        # TODO(aguo): Figure out how to enable cloudpickle serialization for
        # pydantic 2.x

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


@DeveloperAPI
def register_fastapi_serializer(serialization_context):
    """Register FastAPI serializer that's compatible with Pydantic 2.x."""

    try:
        # Check if FastAPI is installed on the cluster
        import fastapi  # noqa: F401
    except ImportError:
        return

    try:
        import pydantic

        if pydantic.__version__.startswith("1."):
            # Only register serializer for Pydantic 2.x.
            return
    except ImportError:
        return

    from fastapi.routing import APIRoute

    serialization_context._register_cloudpickle_serializer(
        APIRoute,
        custom_serializer=lambda o: {
            "path": o.path,
            "endpoint": o.endpoint,
            "response_model": o.response_model,
            "status_code": o.status_code,
            "tags": o.tags,
            "dependencies": o.dependencies,
            "summary": o.summary,
            "description": o.description,
            "response_description": o.response_description,
            "responses": o.responses,
            "deprecated": o.deprecated,
            "name": o.name,
            "methods": o.methods,
            "operation_id": o.operation_id,
            "response_model_include": o.response_model_include,
            "response_model_exclude": o.response_model_exclude,
            "response_model_by_alias": o.response_model_by_alias,
            "response_model_exclude_unset": o.response_model_exclude_unset,
            "response_model_exclude_defaults": o.response_model_exclude_defaults,
            "response_model_exclude_none": o.response_model_exclude_none,
            "include_in_schema": o.include_in_schema,
            "response_class": o.response_class,
            "dependency_overrides_provider": o.dependency_overrides_provider,
            "callbacks": o.callbacks,
            "openapi_extra": o.openapi_extra,
            "generate_unique_id_function": o.generate_unique_id_function,
        },
        custom_deserializer=lambda s: APIRoute(**s),
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
    register_pydantic_serializer(serialization_context)
    register_fastapi_serializer(serialization_context)
    register_starlette_serializer(serialization_context)

    if sys.platform != "win32":
        from ray._private.arrow_serialization import (
            _register_custom_datasets_serializers,
        )

        _register_custom_datasets_serializers(serialization_context)
