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
    """Detect the highest precision for a list of datetime objects.

    Args:
        datetime_list: List of datetime objects.

    Returns:
        A string representing the highest precision among the datetime objects
        ('D', 's', 'ms', 'us', 'ns').
    """
    # Define precision hierarchy
    precision_hierarchy = ["D", "s", "ms", "us", "ns"]
    highest_precision_index = 0  # Start with the lowest precision ("D")

    for dt in datetime_list:
        # Safely get the nanosecond value using getattr for backward compatibility
        nanosecond = getattr(dt, "nanosecond", 0)
        if nanosecond != 0:
            current_precision = "ns"
        elif dt.microsecond != 0:
            # Check if the microsecond precision is exactly millisecond
            if dt.microsecond % 1000 == 0:
                current_precision = "ms"
            else:
                current_precision = "us"
        elif dt.second != 0 or dt.minute != 0 or dt.hour != 0:
            # pyarrow does not support h or m, use s for those cases to
            current_precision = "s"
        else:
            current_precision = "D"

        # Update highest_precision_index based on the hierarchy
        current_index = precision_hierarchy.index(current_precision)
        highest_precision_index = max(highest_precision_index, current_index)

        # Stop early if highest possible precision is reached
        if highest_precision_index == len(precision_hierarchy) - 1:
            break

    return precision_hierarchy[highest_precision_index]


def _convert_to_datetime64(dt: datetime, precision: str) -> np.datetime64:
    """
    Converts a datetime object to a numpy datetime64 object with the specified
    precision.

    Args:
        dt: A datetime object to be converted.
        precision: The desired precision for the datetime64 conversion. Possible
        values are 'D', 's', 'ms', 'us', 'ns'.

    Returns:
        np.datetime64: A numpy datetime64 object with the specified precision.
    """
    if precision == "ns":
        # Calculate nanoseconds from microsecond and nanosecond
        microseconds_as_ns = dt.microsecond * 1000
        # Use getattr for backward compatibility where nanosecond attribute may not
        # exist
        nanoseconds = getattr(dt, "nanosecond", 0)
        total_nanoseconds = microseconds_as_ns + nanoseconds
        # Create datetime64 from base datetime with microsecond precision
        base_dt = np.datetime64(dt, "us")
        # Add remaining nanoseconds as timedelta
        return base_dt + np.timedelta64(total_nanoseconds - microseconds_as_ns, "ns")
    else:
        return np.datetime64(dt).astype(f"datetime64[{precision}]")


def _convert_datetime_list_to_array(datetime_list: List[datetime]) -> np.ndarray:
    """Convert a list of datetime objects to a NumPy array of datetime64 with proper
    precision.

    Args:
        datetime_list (List[datetime]): A list of `datetime` objects to be converted.
        Each `datetime` object represents a specific point in time.

    Returns:
        np.ndarray: A NumPy array containing the `datetime64` values of the datetime
        objects from the input list, with the appropriate precision (e.g., nanoseconds,
        microseconds, milliseconds, etc.).
    """
    # Detect the highest precision for the datetime objects
    precision = _detect_highest_datetime_precision(datetime_list)

    # Convert each datetime to the corresponding numpy datetime64 with the appropriate
    # precision
    return np.array([_convert_to_datetime64(dt, precision) for dt in datetime_list])


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
