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


def _fixed_size_list_to_list_array(arr: pyarrow.Array) -> pyarrow.Array:
    """Convert a FixedSizeListArray to a ListArray of Python lists.

    This helper keeps nulls in place (``None`` elements remain null in the
    resulting array) and constructs a ``ListArray`` with the same value type
    as the input ``FixedSizeListArray``.
    """
    # Each element in ``arr`` is either None or a sequence of child values.
    py_lists = []
    for sub in arr:
        if sub is None:
            py_lists.append(None)
        else:
            # Convert the child values (e.g., FixedSizeListArray[float]) to
            # a plain Python list.
            py_lists.append(list(sub))

    # Explicitly use a list type with the same value type as the input
    # FixedSizeListArray. PyArrow supports None values in the sequence, which
    # become nulls in the resulting ListArray.
    list_type = pyarrow.list_(arr.type.value_type)
    return pyarrow.array(py_lists, type=list_type)


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
        """Flatten each fixed-size array into a variable-length list.

        For FixedSizeListArray inputs, this returns a column backed by an
        Arrow ``ListArray``. We first try the native ``list_flatten`` compute
        function, and fall back to a pure-Python implementation for older
        Arrow versions that may not support this type.
        """
        return_dtype = DataType(object)

        @pyarrow_udf(return_dtype=return_dtype)
        def _flatten(arr: pyarrow.Array) -> pyarrow.Array:
            try:
                # PyArrow supports flattening FixedSizeListArray via list_flatten.
                # This returns a ListArray with variable-length lists.
                return pc.list_flatten(arr)
            except (TypeError, NotImplementedError) as exc:
                # On older Arrow versions or unsupported types, fall back to a
                # simple Python implementation that preserves nulls and shapes.
                try:
                    return _fixed_size_list_to_list_array(arr)
                except Exception:
                    # If the fallback also fails, surface the original error so
                    # users see the most relevant stack trace.
                    raise exc

        return _flatten(self._expr)

    def to_list(self) -> "UDFExpr":
        """Convert each fixed-size array into a Python list.

        The output is a variable-length list column (Arrow ListArray) that can
        be consumed as Python lists when materialized from a Dataset.
        """
        return_dtype = DataType(object)

        @pyarrow_udf(return_dtype=return_dtype)
        def _to_list(arr: pyarrow.Array) -> pyarrow.Array:
            return _fixed_size_list_to_list_array(arr)

        return _to_list(self._expr)
