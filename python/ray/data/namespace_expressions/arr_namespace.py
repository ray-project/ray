"""
Fixed‑size array namespace for expression operations.

This namespace handles Arrow ``FixedSizeListArray`` columns.  It provides
helper methods for flattening nested arrays and converting them into
Python lists.
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

@dataclass
class _ArrayNamespace:
    """Namespace for fixed‑size array operations on expression columns."""
    _expr: "Expr"

    def flatten(self) -> "UDFExpr":
        """Flatten each fixed‑size array into a variable‑length list."""
        return_dtype = DataType(object)
        @pyarrow_udf(return_dtype=return_dtype)
        def _flatten(arr: pyarrow.Array) -> pyarrow.Array:
            try:
                return pc.list_flatten(arr)
            except Exception:
                py_lists = []
                for sub in arr:
                    if sub is None:
                        py_lists.append(None)
                    else:
                        py_lists.append(list(sub))
                return pyarrow.array(py_lists)
        return _flatten(self._expr)

    def to_list(self) -> "UDFExpr":
        """Convert each fixed‑size array into a Python list."""
        return_dtype = DataType(object)
        @pyarrow_udf(return_dtype=return_dtype)
        def _to_list(arr: pyarrow.Array) -> pyarrow.Array:
            py_lists = []
            for sub in arr:
                if sub is None:
                    py_lists.append(None)
                else:
                    py_lists.append(list(sub))
            return pyarrow.array(py_lists)
        return _to_list(self._expr)
