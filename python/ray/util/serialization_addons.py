"""
This module is intended for implementing internal serializers for some
site packages.
"""

import sys

from ray.util.annotations import DeveloperAPI


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


def _register_numpy_serializer(serialization_context):

    try:
        import numpy  # noqa:F401
    except ModuleNotFoundError:
        return

    from ray._private.numpy_serialization import _register_numpy_ndarray_data_serializer

    _register_numpy_ndarray_data_serializer(serialization_context)


@DeveloperAPI
def apply(serialization_context):
    from ray._private.pydantic_compat import register_pydantic_serializers

    register_pydantic_serializers(serialization_context)
    register_starlette_serializer(serialization_context)
    _register_numpy_serializer(serialization_context)

    if sys.platform != "win32":
        from ray._private.arrow_serialization import (
            _register_custom_datasets_serializers,
        )

        _register_custom_datasets_serializers(serialization_context)
