import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Protocol,
    Sequence,
    Union,
)

import numpy as np

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pandas.core.dtypes.generic import ABCSeries


@DeveloperAPI(stability="beta")
class ArrayLike(Protocol):
    """Protocol matching ndarray-like objects (like torch.Tensor)"""

    def __array__(self):
        ...

    def __len__(self):
        ...


@DeveloperAPI(stability="beta")
def is_ndarray_like(value: ArrayLike) -> bool:
    """Checks whether objects are ndarray-like (for ex, torch.Tensor)
    but NOT and ndarray itself"""
    return (
        hasattr(value, "__array__")
        and hasattr(value, "__len__")
        and not isinstance(value, np.ndarray)
    )


def _is_arrow_array(value: ArrayLike) -> bool:
    import pyarrow as pa

    return isinstance(value, (pa.Array, pa.ChunkedArray))


def _should_convert_to_tensor(
    column_values: Union[List[Any], np.ndarray, ArrayLike], column_name: str
) -> bool:
    assert len(column_values) > 0

    # We convert passed in column values into a tensor representation (involving
    # Arrow/Pandas extension types) in either of the following cases:
    return (
        # - Column name is `TENSOR_COLUMN_NAME` (for compatibility)
        column_name == TENSOR_COLUMN_NAME
        # - Provided column values are already represented by a Numpy tensor (ie
        #   ndarray with ndim > 1)
        or _is_ndarray_tensor(column_values)
        # - Provided collection is already implementing Ndarray protocol like
        #   `torch.Tensor`, `pd.Series`, etc (but *excluding* `pyarrow.Array`,
        #   `pyarrow.ChunkedArray`)
        or _is_ndarray_like_not_pyarrow_array(column_values)
        # - Provided column values is a list of a) ndarrays or b) ndarray-like objects
        #   (excluding Pyarrow arrays). This is done for compatibility with previous
        #   existing behavior where all column values were blindly converted to Numpy
        #   leading to list of ndarrays being converted a tensor):
        or isinstance(column_values[0], np.ndarray)
        or _is_ndarray_like_not_pyarrow_array(column_values[0])
    )


def _is_ndarray_like_not_pyarrow_array(column_values):
    return is_ndarray_like(column_values) and not _is_arrow_array(column_values)


def _is_ndarray_tensor(t: Any) -> bool:
    """Return whether provided ndarray is a tensor (ie ndim > 1).

    NOTE: Tensor is defined as a NumPy array such that `len(arr.shape) > 1`
    """

    if not isinstance(t, np.ndarray):
        return False

    # Case of uniform-shaped (ie non-ragged) tensor
    if t.ndim > 1:
        return True

    # Case of ragged tensor (as produced by `create_ragged_ndarray` utility)
    elif t.dtype.type is np.object_ and len(t) > 0 and isinstance(t[0], np.ndarray):
        return True

    return False


def _is_ndarray_variable_shaped_tensor(arr: np.ndarray) -> bool:
    """Return whether the provided NumPy ndarray is comprised of variable-shaped
    tensors.

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


def _create_possibly_ragged_ndarray(
    values: Union[np.ndarray, "ABCSeries", Sequence[Any]]
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
            if np.lib.NumpyVersion(np.__version__) >= "2.0.0":
                copy_if_needed = None
                warning_type = np.exceptions.VisibleDeprecationWarning
            else:
                copy_if_needed = False
                warning_type = np.VisibleDeprecationWarning

            warnings.simplefilter("ignore", category=warning_type)
            arr = np.array(values, copy=copy_if_needed)
            return arr
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
            return create_ragged_ndarray(values)
        else:
            # Re-raise original error if the failure wasn't a broadcast error.
            raise e from None


@PublicAPI(stability="alpha")
def create_ragged_ndarray(values: Sequence[Any]) -> np.ndarray:
    """Create an array that contains arrays of different length

    If you're working with variable-length arrays like images, use this function to
    create ragged arrays instead of ``np.array``.

    .. note::
        ``np.array`` fails to construct ragged arrays if the input arrays have a uniform
        first dimension:

        .. testsetup::

            import numpy as np
            from ray.air.util.tensor_extensions.utils import create_ragged_ndarray

        .. doctest::

            >>> values = [np.zeros((3, 1)), np.zeros((3, 2))]
            >>> np.array(values, dtype=object)
            Traceback (most recent call last):
                ...
            ValueError: could not broadcast input array from shape (3,1) into shape (3,)
            >>> create_ragged_ndarray(values)
            array([array([[0.],
                          [0.],
                          [0.]]), array([[0., 0.],
                                         [0., 0.],
                                         [0., 0.]])], dtype=object)

        Or if you're creating a ragged array from a single array:

        .. doctest::

            >>> values = [np.zeros((3, 1))]
            >>> np.array(values, dtype=object)[0].dtype
            dtype('O')
            >>> create_ragged_ndarray(values)[0].dtype
            dtype('float64')

        ``create_ragged_ndarray`` avoids the limitations of ``np.array`` by creating an
        empty array and filling it with pointers to the variable-length arrays.
    """  # noqa: E501
    # Create an empty object-dtyped 1D array.
    arr = np.empty(len(values), dtype=object)
    # Try to fill the 1D array of pointers with the (ragged) tensors.
    arr[:] = list(values)
    return arr
