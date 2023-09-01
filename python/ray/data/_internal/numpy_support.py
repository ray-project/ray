from typing import Any

import numpy as np

from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.util import _truncated_repr

logger = DatasetLogger(__name__)


def is_array_like(value: Any) -> bool:
    """Checks whether objects are array-like, excluding numpy scalars."""

    return hasattr(value, "__array__") and hasattr(value, "__len__")


def is_valid_udf_return(udf_return_col: Any) -> bool:
    """Check whether a UDF column is valid.

    Valid columns must either be a list of elements, or an array-like object.
    """

    return isinstance(udf_return_col, list) or is_array_like(udf_return_col)


def is_scalar_list(udf_return_col: Any) -> bool:
    """Check whether a UDF column is is a scalar list."""

    return isinstance(udf_return_col, list) and (
        not udf_return_col or np.isscalar(udf_return_col[0])
    )


def convert_udf_returns_to_numpy(udf_return_col: Any) -> Any:
    """Convert UDF columns (output of map_batches) to numpy, if possible.

    This includes lists of scalars, objects supporting the array protocol, and lists
    of objects supporting the array protocol, such as `[1, 2, 3]`, `Tensor([1, 2, 3])`,
    and `[array(1), array(2), array(3)]`.

    Returns:
        The input as an np.ndarray if possible, otherwise the original input.

    Raises:
        ValueError if an input was array-like but we failed to convert it to an array.
    """

    if isinstance(udf_return_col, np.ndarray):
        # No copy/conversion needed, just keep it verbatim.
        return udf_return_col

    if isinstance(udf_return_col, list):
        if len(udf_return_col) == 1 and isinstance(udf_return_col[0], np.ndarray):
            # Optimization to avoid conversion overhead from list to np.array.
            udf_return_col = np.expand_dims(udf_return_col[0], axis=0)
            return udf_return_col

        # Try to convert list values into an numpy array via
        # np.array(), so users don't need to manually cast.
        # NOTE: we don't cast generic iterables, since types like
        # `str` are also Iterable.
        try:
            # Try to cast the inner scalars to numpy as well, to avoid unnecessarily
            # creating an inefficient array of array of object dtype. Don't convert
            # scalar lists though, since those can be represented as pyarrow list type
            # without needing to go through our tensor extension.
            if all(
                is_valid_udf_return(e) and not is_scalar_list(e) for e in udf_return_col
            ):
                udf_return_col = [np.array(e) for e in udf_return_col]
            shapes = set()
            has_object = False
            for e in udf_return_col:
                if isinstance(e, np.ndarray):
                    shapes.add((e.dtype, e.shape))
                elif isinstance(e, bytes):
                    # Don't convert variable length binary data to Numpy arrays as it
                    # treats zero encoding as termination by default.
                    # Per recommendation from
                    # https://github.com/apache/arrow/issues/26470,
                    # we use object dtype.
                    # https://github.com/ray-project/ray/issues/35586#issuecomment-1558148261
                    has_object = True
                elif not np.isscalar(e):
                    has_object = True
            if has_object or len(shapes) > 1:
                # This util works around some limitations of np.array(dtype=object).
                udf_return_col = create_ragged_ndarray(udf_return_col)
            else:
                udf_return_col = np.array(udf_return_col)
        except Exception as e:
            raise ValueError(
                "Failed to convert column values to numpy array: "
                f"({_truncated_repr(udf_return_col)}): {e}."
            )
    elif hasattr(udf_return_col, "__array__"):
        # Converts other array-like objects such as torch.Tensor.
        try:
            udf_return_col = np.array(udf_return_col)
        except Exception as e:
            raise ValueError(
                "Failed to convert column values to numpy array: "
                f"({_truncated_repr(udf_return_col)}): {e}."
            )

    return udf_return_col
