import ray.cloudpickle as cloudpickle
import warnings

import numpy as np

from ray._private.ray_constants import DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE

_numpy_serializer_has_warned = False


def _register_numpy_ndarray_data_serializer(serialization_context):
    serialization_context._register_cloudpickle_reducer(
        np.ndarray, _numpy_ndarray_reduce
    )


def _numpy_ndarray_reduce(obj: np.ndarray):
    global _numpy_serializer_has_warned
    if (
        not _numpy_serializer_has_warned
        and obj.nbytes >= DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE
        and not obj.flags.c_contiguous
    ):

        warnings.warn(
            "Non-contiguous numpy arrays cannot be zero-copy "
            "deserialized which may lead to worse performance. "
            "To avoid this, use numpy.ascontiguousarray(arr) before "
            "passing or returning the array."
            f"\n\tArray shape: {obj.shape}"
            f"\n\tArray dtype: {obj.dtype}",
            UserWarning,
        )
        _numpy_serializer_has_warned = True
    return obj.__reduce_ex__(cloudpickle.DEFAULT_PROTOCOL)
