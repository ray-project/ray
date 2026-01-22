"""Array namespace for expression operations on array-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


@dataclass
class _ArrayNamespace:
    """Namespace for array operations on expression columns.

    Example:
        >>> from ray.data.expressions import col
        >>> # Convert fixed-size lists to variable-length lists
        >>> expr = col("features").arr.to_list()
    """

    _expr: Expr

    def to_list(self) -> "UDFExpr":
        """Convert FixedSizeList columns into variable-length lists."""
        return_dtype = DataType(object)

        expr_dtype = self._expr.data_type
        if expr_dtype.is_list_type():
            arrow_type = expr_dtype.to_arrow_dtype()
            if pyarrow.types.is_fixed_size_list(arrow_type):
                return_dtype = DataType.from_arrow(pyarrow.list_(arrow_type.value_type))
            else:
                return_dtype = expr_dtype

        @pyarrow_udf(return_dtype=return_dtype)
        def _to_list(arr: pyarrow.Array) -> pyarrow.Array:
            arr_dtype = DataType.from_arrow(arr.type)
            if not arr_dtype.is_list_type():
                raise pyarrow.lib.ArrowInvalid(
                    "to_list() can only be called on list-like columns, "
                    f"but got {arr.type}"
                )

            if isinstance(arr.type, pyarrow.FixedSizeListType):
                return arr.cast(pyarrow.list_(arr.type.value_type))
            return arr

        return _to_list(self._expr)
