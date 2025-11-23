"""
Map/dict namespace for expression operations.

This namespace exposes helpers to extract keys or values from Arrow
``MapArray`` columns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr

# Check for native kernel availability once at module level
_HAS_MAP_KEYS = hasattr(pc, "map_keys")
_HAS_MAP_VALUES = hasattr(pc, "map_values")


def _extract_map_component_vectorized(
    arr: pyarrow.Array, component: Literal["keys", "values"]
) -> pyarrow.Array:
    """
    Zero-copy fallback when PyArrow lacks map_keys/map_values kernels.

    This handles both formal ``MapArray`` types and ``ListArray<Struct>``
    types (which represent maps physically) by accessing underlying
    child arrays directly.
    """
    if isinstance(arr, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [
                _extract_map_component_vectorized(chunk, component)
                for chunk in arr.chunks
            ]
        )

    child_array = None

    # Case 1: Formal MapArray
    # MapArray exposes properties to get the flat key/item arrays directly.
    if isinstance(arr, pyarrow.MapArray):
        if component == "keys":
            child_array = arr.keys
        else:
            child_array = arr.items

    # Case 2: ListArray (Physical Map representation)
    # If it's a List<Struct<Key, Value>>, we can access the underlying
    # StructArray via `.values` without invoking a compute kernel.
    elif isinstance(arr, pyarrow.ListArray):
        flat_values = arr.values
        if (
            isinstance(flat_values, pyarrow.StructArray)
            and flat_values.type.num_fields >= 2
        ):
            # By convention, field 0 is Key, field 1 is Value
            idx = 0 if component == "keys" else 1
            child_array = flat_values.field(idx)

    # Case 3: Edge cases / Fallback
    # If the structure is unknown (e.g., fixed-size list or complex nesting),
    # we attempt to cast to MapArray to normalize the layout.
    if child_array is None:
        try:
            # Construct the expected MapType from the inner values
            inner_type = arr.type.value_type
            map_type = pyarrow.map_(inner_type[0].type, inner_type[1].type)
            return _extract_map_component_vectorized(arr.cast(map_type), component)
        except (AttributeError, IndexError, pyarrow.ArrowInvalid):
            raise ValueError(
                f"Cannot extract {component} from array of type {arr.type}"
            )

    # Reconstruct a ListArray using the extracted child components
    # offsets and null-mask are preserved from the parent
    return pyarrow.ListArray.from_arrays(
        offsets=arr.offsets, values=child_array, mask=arr.is_null()
    )


@dataclass
class _MapNamespace:
    """Namespace for map/dict operations on expression columns.

    This namespace exposes helpers to extract keys or values from Arrow
    ``MapArray`` columns.

    Example:
        >>> from ray.data.expressions import col
        >>> # Extract keys from a map column (returns a list)
        >>> _ = col("tags").map.keys()
        >>> # Extract values from a map column (returns a list)
        >>> expr = col("tags").map.values()
    """

    _expr: "Expr"

    def keys(self) -> "UDFExpr":
        """Return a list of keys for each map.

        Example:
            >>> from ray.data.expressions import col
            >>> # Get the keys from the "parameters" map column
            >>> col("parameters").map.keys()

        Returns:
            An expression producing a list of keys for each map entry.
        """
        return self._create_projection_udf("keys")

    def values(self) -> "UDFExpr":
        """Return a list of values for each map.

        Example:
            >>> from ray.data.expressions import col
            >>> # Get the values from the "parameters" map column
            >>> _ = col("parameters").map.values()

        Returns:
            An expression producing a list of values for each map entry.
        """
        return self._create_projection_udf("values")

    def _create_projection_udf(self, component: Literal["keys", "values"]) -> "UDFExpr":
        """Helper to generate UDFs for map projections."""

        # 1. Infer Return Type
        return_dtype = DataType(object)  # Default fallback
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_map(arrow_type):
                inner_type = (
                    arrow_type.key_type if component == "keys" else arrow_type.item_type
                )
                return_dtype = DataType.list(DataType.from_arrow(inner_type))

        # 2. Define Kernel Selection
        use_native = _HAS_MAP_KEYS if component == "keys" else _HAS_MAP_VALUES

        @pyarrow_udf(return_dtype=return_dtype)
        def _project_map(arr: pyarrow.Array) -> pyarrow.Array:
            if use_native:
                fn = pc.map_keys if component == "keys" else pc.map_values
                return fn(arr)

            return _extract_map_component_vectorized(arr, component)

        return _project_map(self._expr)
