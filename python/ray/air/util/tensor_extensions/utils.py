from typing import Any, Sequence, Union
import warnings

import numpy as np
from pandas.core.dtypes.generic import ABCSeries


def _is_ndarray_variable_shaped_tensor(arr: np.ndarray) -> bool:
    """Return whether the provided NumPy ndarray is representing a variable-shaped
    tensor.

    NOTE: This is an O(rows) check.
    """
    if arr.dtype.type is not np.object_:
        return False
    if len(arr) == 0:
        return False
    if not isinstance(arr[0], np.ndarray):
        return False
    shape = arr[0].shape
    for a in arr[1:]:
        if not isinstance(a, np.ndarray):
            return False
        if a.shape != shape:
            return True
    return True


def _create_strict_ragged_ndarray(values: Any) -> np.ndarray:
    """Create a ragged ndarray; the representation will be ragged (1D array of
    subndarray pointers) even if it's possible to represent it as a non-ragged ndarray.
    """
    # Use the create-empty-and-fill method. This avoids the following pitfalls of the
    # np.array constructor - np.array(values, dtype=object):
    #  1. It will fail to construct an ndarray if the first element dimension is
    #  uniform, e.g. for imagery whose first element dimension is the channel.
    #  2. It will construct the wrong representation for a single-row column (i.e. unit
    #  outer dimension). Namely, it will consolidate it into a single multi-dimensional
    #  ndarray rather than a 1D array of subndarray pointers, resulting in the single
    #  row not being well-typed (having object dtype).

    # Create an empty object-dtyped 1D array.
    arr = np.empty(len(values), dtype=object)
    # Try to fill the 1D array of pointers with the (ragged) tensors.
    arr[:] = list(values)
    return arr


def _create_possibly_ragged_ndarray(
    values: Union[np.ndarray, ABCSeries, Sequence[Any]]
) -> np.ndarray:
    """
    Create a possibly ragged ndarray.

    Using the np.array() constructor will fail to construct a ragged ndarray that has a
    uniform first dimension (e.g. uniform channel dimension in imagery). This function
    catches this failure and tries a create-and-fill method to construct the ragged
    ndarray.
    """
    try:
        with warnings.catch_warnings():
            # For NumPy < 1.24, constructing a ragged ndarray directly via
            # `np.array(...)` without the `dtype=object` parameter will raise a
            # VisibleDeprecationWarning which we suppress.
            # More details: https://stackoverflow.com/q/63097829
            warnings.simplefilter("ignore", category=np.VisibleDeprecationWarning)
            return np.array(values, copy=False)
    except ValueError as e:
        # Constructing a ragged ndarray directly via `np.array(...)`
        # without the `dtype=object` parameter will raise a ValueError.
        # For NumPy < 1.24, the message is of the form:
        # "could not broadcast input array from shape..."
        # For NumPy >= 1.24, the message is of the form:
        # "The requested array has an inhomogeneous shape..."
        # More details: https://github.com/numpy/numpy/pull/22004
        error_str = str(e)
        if (
            "could not broadcast input array from shape" in error_str
            or "The requested array has an inhomogeneous shape" in error_str
        ):
            # Fall back to strictly creating a ragged ndarray.
            return _create_strict_ragged_ndarray(values)
        else:
            # Re-raise original error if the failure wasn't a broadcast error.
            raise e from None
