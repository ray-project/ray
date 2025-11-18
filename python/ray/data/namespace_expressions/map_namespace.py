"""
Map/dict namespace for expression operations.

This namespace exposes helpers to extract keys or values from Arrow
``MapArray`` columns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


@dataclass
class _MapNamespace:
    """Namespace for map/dict operations on expression columns."""

    _expr: "Expr"

    def keys(self) -> "UDFExpr":
        """Return a list of keys for each map."""
        return_dtype = DataType(object)

        @pyarrow_udf(return_dtype=return_dtype)
        def _keys(arr: pyarrow.Array) -> pyarrow.Array:
            py_lists = []
            for item in arr:
                if item is None:
                    py_lists.append(None)
                else:
                    py_lists.append(list(item.keys))
            return pyarrow.array(py_lists)

        return _keys(self._expr)

    def values(self) -> "UDFExpr":
        """Return a list of values for each map."""
        # Infer return type from map's value type.
        return_dtype = DataType(object)  # fallback
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_map(arrow_type):
                return_dtype = DataType.list(DataType.from_arrow(arrow_type.item_type))

        @pyarrow_udf(return_dtype=return_dtype)
        def _values(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.map_values(arr)

        return _values(self._expr)
