import warnings

import numpy as np

_numpy_serializer_has_warned = False


def _register_numpy_ndarray_data_serializer(serialization_context):
    serialization_context._register_cloudpickle_reducer(
        np.ndarray, _numpy_ndarray_reduce
    )


def _numpy_ndarray_reduce(obj: np.ndarray):
    global _numpy_serializer_has_warned
    if not _numpy_serializer_has_warned and not obj.flags.c_contiguous:
        warnings.warn(
            "Non-contiguous numpy ndarrays are not zero-copy "
            "serialized and lead to worse performance. "
            "Use numpy.ascontiguousarray(arr)."
        )
        _numpy_serializer_has_warned = True
    return obj.__reduce__()
