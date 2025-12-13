"""List namespace for expression operations on list-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


@dataclass
class _ListNamespace:
    """Namespace for list operations on expression columns.

    This namespace provides methods for operating on list-typed columns using
    PyArrow compute functions.

    Example:
        >>> from ray.data.expressions import col
        >>> # Get length of list column
        >>> expr = col("items").list.len()
        >>> # Get first item using method
        >>> expr = col("items").list.get(0)
        >>> # Get first item using indexing
        >>> expr = col("items").list[0]
        >>> # Slice list
        >>> expr = col("items").list[1:3]
        >>> # Convert fixed-size lists to variable-length lists
        >>> expr = col("features").list.to_list()
        >>> # Flatten nested list columns
        >>> expr = col("features").list.flatten()
    """

    _expr: Expr

    def len(self) -> "UDFExpr":
        """Get the length of each list."""

        @pyarrow_udf(return_dtype=DataType.int32())
        def _list_len(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.list_value_length(arr)

        return _list_len(self._expr)

    def __getitem__(self, key: Union[int, slice]) -> "UDFExpr":
        """Get element or slice using bracket notation.

        Args:
            key: An integer for element access or slice for list slicing.

        Returns:
            UDFExpr that extracts the element or slice.

        Example:
            >>> col("items").list[0]      # Get first item  # doctest: +SKIP
            >>> col("items").list[1:3]    # Get slice [1, 3)  # doctest: +SKIP
            >>> col("items").list[-1]     # Get last item  # doctest: +SKIP
        """
        if isinstance(key, int):
            return self.get(key)
        elif isinstance(key, slice):
            return self.slice(key.start, key.stop, key.step)
        else:
            raise TypeError(
                f"List indices must be integers or slices, not {type(key).__name__}"
            )

    def get(self, index: int) -> "UDFExpr":
        """Get element at the specified index from each list.

        Args:
            index: The index of the element to retrieve. Negative indices are supported.

        Returns:
            UDFExpr that extracts the element at the given index.
        """
        # Infer return type from the list's value type
        return_dtype = DataType(object)  # fallback
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_list(arrow_type) or pyarrow.types.is_large_list(
                arrow_type
            ):
                return_dtype = DataType.from_arrow(arrow_type.value_type)
            elif pyarrow.types.is_fixed_size_list(arrow_type):
                return_dtype = DataType.from_arrow(arrow_type.value_type)

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_get(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.list_element(arr, index)

        return _list_get(self._expr)

    def slice(
        self, start: int | None = None, stop: int | None = None, step: int | None = None
    ) -> "UDFExpr":
        """Slice each list.

        Args:
            start: Start index (inclusive). Defaults to 0.
            stop: Stop index (exclusive). Defaults to list length.
            step: Step size. Defaults to 1.

        Returns:
            UDFExpr that extracts a slice from each list.
        """
        # Return type is the same as the input list type
        return_dtype = self._expr.data_type

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_slice(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.list_slice(
                arr,
                start=0 if start is None else start,
                stop=stop,
                step=1 if step is None else step,
            )

        return _list_slice(self._expr)

    def flatten(self) -> "UDFExpr":
        """Flatten nested list columns (including FixedSizeList variants).

        This wraps :func:`pyarrow.compute.list_flatten` and preserves the inner
        element type when possible.
        """
        return_dtype = DataType(object)

        expr_dtype = self._expr.data_type
        if expr_dtype.is_list_type():
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
