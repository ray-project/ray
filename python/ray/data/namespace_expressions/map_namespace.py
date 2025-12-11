from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


class MapComponent(str, Enum):
    KEYS = "keys"
    VALUES = "values"


def _extract_map_component(
    arr: pyarrow.Array, component: MapComponent
) -> pyarrow.Array:
    """
    Extracts keys or values from a MapArray or ListArray<Struct>.

    This serves as the primary implementation since PyArrow does not yet
    expose dedicated compute kernels for map projection in the Python API.
    """
    # 1. Handle Chunked Arrays (Recursion)
    if isinstance(arr, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [_extract_map_component(chunk, component) for chunk in arr.chunks]
        )

    child_array = None

    # Case 1: MapArray
    if isinstance(arr, pyarrow.MapArray):
        if component == MapComponent.KEYS:
            child_array = arr.keys
        else:
            child_array = arr.items

    # Case 2: ListArray<Struct<Key, Value>>
    elif isinstance(arr, pyarrow.ListArray):
        flat_values = arr.values
        if (
            isinstance(flat_values, pyarrow.StructArray)
            and flat_values.type.num_fields >= 2
        ):
            idx = 0 if component == MapComponent.KEYS else 1
            child_array = flat_values.field(idx)

    # Case 3: If structure is unknown (e.g. FixedSizeList), attempt to cast to strict MapType.
    if child_array is None:
        try:
            inner_type = arr.type.value_type
            map_type = pyarrow.map_(inner_type[0].type, inner_type[1].type)
            return _extract_map_component(arr.cast(map_type), component)
        except (AttributeError, IndexError, pyarrow.ArrowInvalid):
            raise ValueError(
                f"Cannot extract {component} from array of type {arr.type}"
            )

    # Reconstruct ListArray & Normalize Offsets
    offsets = arr.offsets
    if len(offsets) > 0:  # Handle offsets changes
        start_offset = offsets[0]
        if start_offset.as_py() != 0:
            offsets = pc.subtract(offsets, start_offset)

    return pyarrow.ListArray.from_arrays(
        offsets=offsets, values=child_array, mask=arr.is_null()
    )


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

            is_physical_map = (
                pyarrow.types.is_list(arrow_type)
                and pyarrow.types.is_struct(arrow_type.value_type)
                and arrow_type.value_type.num_fields >= 2
            )

            inner_arrow_type = None
            if pyarrow.types.is_map(arrow_type):
                inner_arrow_type = (
                    arrow_type.key_type
                    if component == MapComponent.KEYS
                    else arrow_type.item_type
                )
            elif is_physical_map:
                idx = 0 if component == MapComponent.KEYS else 1
                inner_arrow_type = arrow_type.value_type.field(idx).type

            if inner_arrow_type:
                return_dtype = DataType.list(DataType.from_arrow(inner_arrow_type))

        @pyarrow_udf(return_dtype=return_dtype)
        def _project_map(arr: pyarrow.Array) -> pyarrow.Array:
            return _extract_map_component(arr, component)

        return _project_map(self._expr)
