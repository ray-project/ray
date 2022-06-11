"""
This module is intended for implementing internal serializers for some
site packages.
"""


def register_pydantic_serializer(serialization_context):
    try:
        import pydantic.fields
    except ImportError:
        return

    # Pydantic's Cython validators are not serializable.
    # https://github.com/cloudpipe/cloudpickle/issues/408
    serialization_context._register_cloudpickle_serializer(
        pydantic.fields.ModelField,
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
        custom_deserializer=lambda kwargs: pydantic.fields.ModelField(**kwargs),
    )


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


def apply(serialization_context):
    register_pydantic_serializer(serialization_context)
    register_starlette_serializer(serialization_context)
