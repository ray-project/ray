"""
Map/dict namespace for expression operations.

This namespace exposes helpers to extract keys or values from Arrow
``MapArray`` columns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


_HAS_MAP_KEYS = hasattr(pc, "map_keys")
_HAS_MAP_VALUES = hasattr(pc, "map_values")


def _extract_map_lists(
    arr: pyarrow.Array,
    *,
    extract_keys: bool,
    list_type: pyarrow.DataType | None,
) -> pyarrow.Array:
    """Fallback helper when PyArrow lacks map_keys/map_values kernels."""

    python_lists = []
    for scalar in arr:
        if scalar is None or not scalar.is_valid:
            python_lists.append(None)
            continue

        as_value = scalar.as_py()
        if isinstance(as_value, dict):
            if extract_keys:
                python_lists.append(list(as_value.keys()))
            else:
                python_lists.append(list(as_value.values()))
            continue

        # PyArrow < 19 returns list of (key, value) tuples.
        if extract_keys:
            python_lists.append([pair[0] for pair in as_value])
        else:
            python_lists.append([pair[1] for pair in as_value])

    return pyarrow.array(python_lists, type=list_type)


@dataclass
class _MapNamespace:
    """Namespace for map/dict operations on expression columns."""

    _expr: "Expr"

    def keys(self) -> "UDFExpr":
        """Return a list of keys for each map."""
        # Infer return type from map's key type.
        return_dtype = DataType(object)  # fallback
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_map(arrow_type):
                return_dtype = DataType.list(DataType.from_arrow(arrow_type.key_type))
        list_arrow_type = (
            return_dtype.to_arrow_dtype() if return_dtype.is_arrow_type() else None
        )

        @pyarrow_udf(return_dtype=return_dtype)
        def _keys(arr: pyarrow.Array) -> pyarrow.Array:
            if _HAS_MAP_KEYS:
                return pc.map_keys(arr)
            return _extract_map_lists(
                arr,
                extract_keys=True,
                list_type=list_arrow_type,
            )

        return _keys(self._expr)

    def values(self) -> "UDFExpr":
        """Return a list of values for each map."""
        # Infer return type from map's value type.
        return_dtype = DataType(object)  # fallback
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_map(arrow_type):
                return_dtype = DataType.list(DataType.from_arrow(arrow_type.item_type))
        list_arrow_type = (
            return_dtype.to_arrow_dtype() if return_dtype.is_arrow_type() else None
        )

        @pyarrow_udf(return_dtype=return_dtype)
        def _values(arr: pyarrow.Array) -> pyarrow.Array:
            if _HAS_MAP_VALUES:
                return pc.map_values(arr)
            return _extract_map_lists(
                arr,
                extract_keys=False,
                list_type=list_arrow_type,
            )

        return _values(self._expr)
