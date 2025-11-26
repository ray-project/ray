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
        return_dtype = DataType(object)

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
        return_dtype = DataType(object)

        @pyarrow_udf(return_dtype=return_dtype)
        def _to_list(arr: pyarrow.Array) -> pyarrow.Array:
            # Only FixedSizeListArray needs conversion; for other list-like
            # types we just return the input unchanged.
            if isinstance(arr.type, pyarrow.FixedSizeListType):
                return arr.cast(pyarrow.list_(arr.type.value_type))
            return arr

        return _to_list(self._expr)
