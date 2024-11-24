import collections
import logging
from datetime import datetime
from typing import Any, Dict, List, Union

import numpy as np

from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
from ray.data._internal.util import _truncated_repr

logger = logging.getLogger(__name__)


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


def convert_to_numpy(column_values: Any) -> np.ndarray:
    """Convert UDF columns (output of map_batches) to numpy, if possible.

    This includes lists of scalars, objects supporting the array protocol, and lists
    of objects supporting the array protocol, such as `[1, 2, 3]`, `Tensor([1, 2, 3])`,
    and `[array(1), array(2), array(3)]`.

    Returns:
        The input as an np.ndarray if possible, otherwise the original input.

    Raises:
        ValueError if an input was array-like but we failed to convert it to an array.
    """

    if isinstance(column_values, np.ndarray):
        # No copy/conversion needed, just keep it verbatim.
        return column_values

    elif isinstance(column_values, list):
        if len(column_values) == 1 and isinstance(column_values[0], np.ndarray):
            # Optimization to avoid conversion overhead from list to np.array.
            return np.expand_dims(column_values[0], axis=0)

        if all(isinstance(elem, datetime) for elem in column_values):
            return _convert_datetime_list_to_array(column_values)

        # Try to convert list values into an numpy array via
        # np.array(), so users don't need to manually cast.
        # NOTE: we don't cast generic iterables, since types like
        # `str` are also Iterable.
        try:
            # Convert array-like objects (like torch.Tensor) to `np.ndarray`s
            if all(is_array_like(e) for e in column_values):
                # Use np.asarray() instead of np.array() to avoid copying if possible.
                column_values = [np.asarray(e) for e in column_values]

            shapes = set()
            has_object = False
            for e in column_values:
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

            # When column values are
            #   - Arrays of heterogeneous shapes
            #   - Byte-strings (viewed as arrays of heterogeneous shapes)
            #   - Non-scalar objects (tuples, lists, arbitrary object types)
            #
            # Custom "ragged ndarray" is created, represented as an array of
            # references (ie ndarray with dtype=object)
            if has_object or len(shapes) > 1:
                # This util works around some limitations of np.array(dtype=object).
                return create_ragged_ndarray(column_values)
            else:
                return np.array(column_values)

        except Exception as e:
            logger.error(
                f"Failed to convert column values to numpy array: "
                f"{_truncated_repr(column_values)}",
                exc_info=e,
            )

            raise ValueError(
                "Failed to convert column values to numpy array: "
                f"({_truncated_repr(column_values)}): {e}."
            ) from e

    elif is_array_like(column_values):
        # Converts other array-like objects such as torch.Tensor.
        try:
            # Use np.asarray() instead of np.array() to avoid copying if possible.
            return np.asarray(column_values)
        except Exception as e:
            logger.error(
                f"Failed to convert column values to numpy array: "
                f"{_truncated_repr(column_values)}",
                exc_info=e,
            )

            raise ValueError(
                "Failed to convert column values to numpy array: "
                f"({_truncated_repr(column_values)}): {e}."
            ) from e

    else:
        return column_values
