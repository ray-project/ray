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

    def sort(
        self, order: str = "ascending", null_placement: str = "at_end"
    ) -> "UDFExpr":
        """Sort the elements within each list.

        Args:
            order: Sorting order, must be ``\"ascending\"`` or ``\"descending\"``.
            null_placement: Placement for null values, ``\"at_start\"`` or ``\"at_end\"``.

        Returns:
            UDFExpr providing the sorted lists.
        """

        if order not in {"ascending", "descending"}:
            raise ValueError(
                "order must be either 'ascending' or 'descending', got " f"{order!r}"
            )
        if null_placement not in {"at_start", "at_end"}:
            raise ValueError(
                "null_placement must be 'at_start' or 'at_end', got "
                f"{null_placement!r}"
            )

        return_dtype = self._expr.data_type

        if hasattr(pc, "list_sort"):

            @pyarrow_udf(return_dtype=return_dtype)
            def _list_sort(arr: pyarrow.Array) -> pyarrow.Array:
                return pc.list_sort(arr, order=order, null_placement=null_placement)

            return _list_sort(self._expr)

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_sort_python(arr: pyarrow.Array) -> pyarrow.Array:
            """Fallback implementation when PyArrow list_sort is unavailable."""

            def _sort_single_list(values):
                if values is None:
                    return None
                non_null = [value for value in values if value is not None]
                non_null.sort(reverse=order == "descending")
                null_count = len(values) - len(non_null)
                nulls = [None] * null_count
                if null_placement == "at_start":
                    return nulls + non_null
                else:
                    return non_null + nulls

            sorted_lists = [_sort_single_list(values) for values in arr.to_pylist()]
            return pyarrow.array(sorted_lists, type=arr.type)

        return _list_sort_python(self._expr)

    def flatten(self) -> "UDFExpr":
        """Flatten one level of nesting for each list value."""

        def _is_list_like(pa_type: pyarrow.DataType) -> bool:
            return pyarrow.types.is_list(pa_type) or pyarrow.types.is_large_list(
                pa_type
            )

        return_dtype = DataType(object)
        target_arrow_type = None
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if _is_list_like(arrow_type):
                child_type = arrow_type.value_type
                if _is_list_like(child_type):
                    return_dtype = DataType.from_arrow(child_type)
                    target_arrow_type = child_type
                elif pyarrow.types.is_fixed_size_list(child_type):
                    inner_value = DataType.from_arrow(child_type.value_type)
                    return_dtype = DataType.list(inner_value)
                    target_arrow_type = pyarrow.list_(child_type.value_type)
                else:
                    target_arrow_type = arrow_type
            elif pyarrow.types.is_fixed_size_list(arrow_type):
                inner_value = DataType.from_arrow(arrow_type.value_type)
                return_dtype = inner_value
                target_arrow_type = arrow_type.value_type
            else:
                target_arrow_type = arrow_type

        if target_arrow_type is None and return_dtype.is_arrow_type():
            target_arrow_type = return_dtype.to_arrow_dtype()

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_flatten(arr: pyarrow.Array) -> pyarrow.Array:
            def _flatten_value(value):
                if value is None:
                    return None

                flattened = []
                for sub in value:
                    if sub is None:
                        continue
                    if isinstance(sub, list):
                        flattened.extend(sub)
                    else:
                        flattened.append(sub)
                return flattened

            flattened_values = [_flatten_value(value) for value in arr.to_pylist()]

            arrow_type = target_arrow_type
            if arrow_type is None:
                arr_type = arr.type
                if pyarrow.types.is_list(arr_type) or pyarrow.types.is_large_list(
                    arr_type
                ):
                    child_type = arr_type.value_type
                    if pyarrow.types.is_list(child_type) or pyarrow.types.is_large_list(
                        child_type
                    ):
                        arrow_type = child_type
                    elif pyarrow.types.is_fixed_size_list(child_type):
                        arrow_type = pyarrow.list_(child_type.value_type)
                    else:
                        arrow_type = arr_type
                elif pyarrow.types.is_fixed_size_list(arr_type):
                    child_type = arr_type.value_type
                    if pyarrow.types.is_list(child_type) or pyarrow.types.is_large_list(
                        child_type
                    ):
                        arrow_type = child_type
                    else:
                        arrow_type = pyarrow.list_(child_type)
                else:
                    arrow_type = arr_type

            return pyarrow.array(flattened_values, type=arrow_type)

        return _list_flatten(self._expr)
