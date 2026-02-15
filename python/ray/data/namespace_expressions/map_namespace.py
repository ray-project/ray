from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import numpy as np
import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


class MapComponent(str, Enum):
    KEYS = "keys"
    VALUES = "values"


def _get_child_array(
    arr: pyarrow.Array, component: MapComponent
) -> pyarrow.Array | None:
    """Extract the flat keys or values array from a map-like array.

    Example: MapArray [{"a": 1}, {"b": 2}] -> keys ["a", "b"] or values [1, 2]
    """
    if isinstance(arr, pyarrow.MapArray):
        if component == MapComponent.KEYS:
            return arr.keys
        else:
            return arr.items

    if isinstance(arr, (pyarrow.ListArray, pyarrow.LargeListArray)):
        flat_values = arr.values
        if (
            isinstance(flat_values, pyarrow.StructArray)
            and flat_values.type.num_fields >= 2
        ):
            idx = 0 if component == MapComponent.KEYS else 1
            return flat_values.field(idx)

    return None


def _make_empty_list_array(
    arr: pyarrow.Array, component: MapComponent
) -> pyarrow.Array:
    """Create an all-null ListArray matching the input length.

    Example: arr of length 3 -> ListArray [null, null, null]
    """
    if len(arr) > 0 and arr.null_count < len(arr):
        raise TypeError(
            f"Expression is not a valid map type. .map.{component.value}() requires "
            f"pyarrow.MapArray or pyarrow.ListArray<Struct> with at least 2 fields "
            f"(key and value), but got: {arr.type}."
        )
    return pyarrow.ListArray.from_arrays(
        offsets=np.repeat(0, len(arr) + 1),
        values=pyarrow.array([], type=pyarrow.null()),
        mask=pyarrow.array(np.repeat(True, len(arr))),
    )


def _rebuild_list_array(
    arr: pyarrow.Array, child_array: pyarrow.Array
) -> pyarrow.Array:
    """Rebuild a ListArray from parent offsets and child values, normalizing sliced offsets.

    Example: offsets [5, 7, 10] -> slice child to [5:10], normalize offsets to [0, 2, 5]
    """
    offsets = arr.offsets
    if len(offsets) > 0:
        start_offset = offsets[0]
        if start_offset != 0:
            end_offset = offsets[-1]
            child_array = child_array.slice(
                offset=int(start_offset), length=int(end_offset) - int(start_offset)
            )
            offsets = pc.subtract(offsets, start_offset)

    return pyarrow.ListArray.from_arrays(
        offsets=offsets, values=child_array, mask=arr.is_null()
    )


def _get_result_type(
    arr_type: pyarrow.DataType, component: MapComponent
) -> pyarrow.DataType:
    """Infer the result list type from the input map type."""
    if pyarrow.types.is_map(arr_type):
        inner = (
            arr_type.key_type if component == MapComponent.KEYS else arr_type.item_type
        )
        return pyarrow.list_(inner)
    if pyarrow.types.is_list(arr_type) or pyarrow.types.is_large_list(arr_type):
        struct_type = arr_type.value_type
        if pyarrow.types.is_struct(struct_type) and struct_type.num_fields >= 2:
            idx = 0 if component == MapComponent.KEYS else 1
            return pyarrow.list_(struct_type.field(idx).type)
    return pyarrow.list_(pyarrow.null())


def _extract_map_component(
    arr: pyarrow.Array, component: MapComponent
) -> pyarrow.Array:
    """Extract keys or values from a MapArray or ListArray<Struct>.

    This serves as the primary implementation since PyArrow does not yet
    expose dedicated compute kernels for map projection in the Python API.
    """
    if isinstance(arr, pyarrow.ChunkedArray):
        chunks = [_extract_map_component(chunk, component) for chunk in arr.chunks]
        if not chunks:
            return pyarrow.chunked_array([], type=_get_result_type(arr.type, component))
        return pyarrow.chunked_array(chunks)

    child_array = _get_child_array(arr, component)

    if child_array is None:
        return _make_empty_list_array(arr, component)

    return _rebuild_list_array(arr, child_array)


@dataclass
class _MapNamespace:
    """Namespace for map operations on expression columns.

    This namespace provides methods for operating on map-typed columns
    (including MapArrays and ListArrays of Structs) using PyArrow UDFs.

    Example:
        >>> from ray.data.expressions import col
        >>> # Get keys from map column
        >>> expr = col("headers").map.keys()
        >>> # Get values from map column
        >>> expr = col("headers").map.values()
    """

    _expr: "Expr"

    def keys(self) -> "UDFExpr":
        """Returns a list expression containing the keys of the map.

        Example:
            >>> from ray.data.expressions import col
            >>> # Get keys from map column
            >>> expr = col("headers").map.keys()

        Returns:
            A list expression containing the keys.
        """
        return self._create_projection_udf(MapComponent.KEYS)

    def values(self) -> "UDFExpr":
        """Returns a list expression containing the values of the map.

        Example:
            >>> from ray.data.expressions import col
            >>> # Get values from map column
            >>> expr = col("headers").map.values()

        Returns:
            A list expression containing the values.
        """
        return self._create_projection_udf(MapComponent.VALUES)

    def _create_projection_udf(self, component: MapComponent) -> "UDFExpr":
        """Helper to generate UDFs for map projections."""

        return_dtype = DataType(object)
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            result_arrow_type = _get_result_type(arrow_type, component)
            return_dtype = DataType.from_arrow(result_arrow_type)

        @pyarrow_udf(return_dtype=return_dtype)
        def _project_map(arr: pyarrow.Array) -> pyarrow.Array:
            return _extract_map_component(arr, component)

        return _project_map(self._expr)
