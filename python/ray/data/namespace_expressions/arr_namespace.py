"""
Fixed-size array namespace for expression operations.

This namespace handles Arrow ``FixedSizeListArray`` columns. It provides
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
    """Namespace for fixed-size array operations on expression columns.

    Example
    -------
    >>> from ray.data.expressions import col
    >>> # "features" is a FixedSizeList column, e.g. 3-d embeddings
    >>> expr = col("features").arr.to_list()
    >>> # You can then use this expression inside Dataset.select/with_columns.
    """

    _expr: "Expr"

    def flatten(self) -> "UDFExpr":
        """Flattens a column of fixed-size lists of lists into a single list.

        This is a thin wrapper around :func:`pyarrow.compute.list_flatten`.

        For example, a column with type
        ``fixed_size_list(list<item: int64>, 2)`` will be converted to a
        ``list<item: int64>`` column where each row's nested lists are
        concatenated.

        This operation is only valid for nested list types. If the input
        is not a list-of-lists, PyArrow will raise a type error.
        """
        # Default to object if we cannot infer a better type.
        return_dtype = DataType(object)

        expr_dtype = self._expr.data_type
        if expr_dtype.is_arrow_type() and expr_dtype.is_list_type():
            outer_arrow_type = expr_dtype.to_arrow_dtype()
            inner_arrow_type = outer_arrow_type.value_type

            # Inner list type; list_flatten(list<list<T>>) -> list<T>
            inner_dtype = DataType.from_arrow(inner_arrow_type)
            if inner_dtype.is_list_type():
                value_arrow_type = inner_arrow_type.value_type
                return_dtype = DataType.from_arrow(pyarrow.list_(value_arrow_type))

        @pyarrow_udf(return_dtype=return_dtype)
        def _flatten(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.list_flatten(arr)

        return _flatten(self._expr)

    def to_list(self) -> "UDFExpr":
        """Convert each fixed-size array into a variable-length list.

        This converts a ``FixedSizeListArray`` into an equivalent
        ``ListArray`` with the same value type, using PyArrow's cast
        operator. Non-fixed-size list arrays are returned unchanged.
        """
        # Default to object; refine when we know more.
        return_dtype = DataType(object)

        expr_dtype = self._expr.data_type
        if expr_dtype.is_arrow_type():
            arrow_type = expr_dtype.to_arrow_dtype()
            if pyarrow.types.is_fixed_size_list(arrow_type):
                # FixedSizeListArray<T> -> ListArray<T>
                return_dtype = DataType.from_arrow(pyarrow.list_(arrow_type.value_type))
            elif expr_dtype.is_list_type():
                # For other list-like arrays we return the input unchanged,
                # so the dtype is the same as the original expression.
                return_dtype = expr_dtype

        @pyarrow_udf(return_dtype=return_dtype)
        def _to_list(arr: pyarrow.Array) -> pyarrow.Array:
            # Check if it's any list type.
            is_list_like = (
                pyarrow.types.is_list(arr.type)
                or pyarrow.types.is_large_list(arr.type)
                or pyarrow.types.is_fixed_size_list(arr.type)
                or (
                    hasattr(pyarrow.types, "is_list_view")
                    and pyarrow.types.is_list_view(arr.type)
                )
                or (
                    hasattr(pyarrow.types, "is_large_list_view")
                    and pyarrow.types.is_large_list_view(arr.type)
                )
            )
            if not is_list_like:
                raise pyarrow.lib.ArrowInvalid(
                    f"arr.to_list() can only be called on list-like columns, but got {arr.type}"
                )

            # Only FixedSizeListArray needs conversion; other list-like
            # types are returned unchanged.
            if isinstance(arr.type, pyarrow.FixedSizeListType):
                return arr.cast(pyarrow.list_(arr.type.value_type))
            return arr

        return _to_list(self._expr)
