import collections
from datetime import datetime
from typing import Any, Dict, List, Union

import numpy as np

from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
from ray.data._internal.util import _truncated_repr


def is_array_like(value: Any) -> bool:
    """Checks whether objects are array-like, excluding numpy scalars."""

    return hasattr(value, "__array__") and hasattr(value, "__len__")


def is_valid_udf_return(udf_return_col: Any) -> bool:
    """Check whether a UDF column is valid.

    Valid columns must either be a list of elements, or an array-like object.
    """

    return isinstance(udf_return_col, list) or is_array_like(udf_return_col)


def is_nested_list(udf_return_col: List[Any]) -> bool:
    for e in udf_return_col:
        if isinstance(e, list):
            return True
    return False


def validate_numpy_batch(batch: Union[Dict[str, np.ndarray], Dict[str, list]]) -> None:
    if not isinstance(batch, collections.abc.Mapping) or any(
        not is_valid_udf_return(col) for col in batch.values()
    ):
        raise ValueError(
            "Batch must be an ndarray or dictionary of ndarrays when converting "
            f"a numpy batch to a block, got: {type(batch)} "
            f"({_truncated_repr(batch)})"
        )


def _detect_highest_datetime_precision(datetime_list: List[datetime]) -> str:
    highest_precision = "D"

    for dt in datetime_list:
        if dt.microsecond != 0 and dt.microsecond % 1000 != 0:
            highest_precision = "us"
            break
        elif dt.microsecond != 0 and dt.microsecond % 1000 == 0:
            highest_precision = "ms"
        elif dt.hour != 0 or dt.minute != 0 or dt.second != 0:
            # pyarrow does not support h or m, use s for those cases too
            highest_precision = "s"

    return highest_precision


def _convert_datetime_list_to_array(datetime_list: List[datetime]) -> np.ndarray:
    precision = _detect_highest_datetime_precision(datetime_list)

    return np.array(
        [np.datetime64(dt, precision) for dt in datetime_list],
        dtype=f"datetime64[{precision}]",
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

        if all(isinstance(elem, datetime) for elem in udf_return_col):
            return _convert_datetime_list_to_array(udf_return_col)

        # Try to convert list values into an numpy array via
        # np.array(), so users don't need to manually cast.
        # NOTE: we don't cast generic iterables, since types like
        # `str` are also Iterable.
        try:
            # Try to cast the inner scalars to numpy as well, to avoid unnecessarily
            # creating an inefficient array of array of object dtype.
            # But don't convert if the list is nested. Because if sub-lists have
            # heterogeneous shapes, we need to create a ragged ndarray.
            if not is_nested_list(udf_return_col) and all(
                is_valid_udf_return(e) for e in udf_return_col
            ):
                # Use np.asarray() instead of np.array() to avoid copying if possible.
                udf_return_col = [np.asarray(e) for e in udf_return_col]
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
