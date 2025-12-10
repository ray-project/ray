"""List namespace for expression operations on list-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

import numpy as np
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

        return_dtype = DataType(object)
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_list(arrow_type) or pyarrow.types.is_large_list(
                arrow_type
            ):
                child_type = arrow_type.value_type
                list_factory = (
                    pyarrow.large_list
                    if pyarrow.types.is_large_list(arrow_type)
                    else pyarrow.list_
                )
                if (
                    pyarrow.types.is_list(child_type)
                    or pyarrow.types.is_large_list(child_type)
                    or pyarrow.types.is_fixed_size_list(child_type)
                ):
                    flattened_type = list_factory(child_type.value_type)
                    return_dtype = DataType.from_arrow(flattened_type)
            elif pyarrow.types.is_fixed_size_list(arrow_type):
                child_type = arrow_type.value_type
                if (
                    pyarrow.types.is_list(child_type)
                    or pyarrow.types.is_large_list(child_type)
                    or pyarrow.types.is_fixed_size_list(child_type)
                ):
                    flattened_type = pyarrow.list_(child_type.value_type)
                    return_dtype = DataType.from_arrow(flattened_type)

        if hasattr(pc, "list_flatten"):

            @pyarrow_udf(return_dtype=return_dtype)
            def _list_flatten_arrow(arr: pyarrow.Array) -> pyarrow.Array:
                if isinstance(arr, pyarrow.ChunkedArray):
                    arr = arr.combine_chunks()

                arr_type = arr.type
                arr_is_list = pyarrow.types.is_list(
                    arr_type
                ) or pyarrow.types.is_large_list(arr_type)
                arr_is_fixed_size = pyarrow.types.is_fixed_size_list(arr_type)
                if not (arr_is_list or arr_is_fixed_size):
                    raise TypeError(
                        "list.flatten() requires a list column whose elements are also lists."
                    )

                child_type = arr_type.value_type
                child_is_list = pyarrow.types.is_list(
                    child_type
                ) or pyarrow.types.is_large_list(child_type)
                child_is_fixed_size = pyarrow.types.is_fixed_size_list(child_type)
                if not (child_is_list or child_is_fixed_size):
                    raise TypeError(
                        "list.flatten() requires a list column whose elements are also lists."
                    )

                flattened_child_lists = pc.list_flatten(arr)
                scalar_values = pc.list_flatten(flattened_child_lists)

                child_to_parent = pc.list_parent_indices(arr)
                scalar_to_child = pc.list_parent_indices(flattened_child_lists)

                if len(scalar_values) == 0:
                    counts = np.zeros(len(arr), dtype=np.int64)
                else:
                    parent_index_array = pc.take(child_to_parent, scalar_to_child)
                    parent_indices = parent_index_array.to_numpy(zero_copy_only=False)
                    counts = np.bincount(
                        parent_indices,
                        minlength=len(arr),
                    ).astype(np.int64, copy=False)

                offsets = np.zeros(len(arr) + 1, dtype=np.int64)
                if counts.size > 0:
                    np.cumsum(counts, out=offsets[1:])

                arr_is_large = pyarrow.types.is_large_list(arr_type)
                offsets_type = pyarrow.int64() if arr_is_large else pyarrow.int32()
                offsets_array = pyarrow.array(offsets, type=offsets_type)
                null_mask = arr.is_null() if arr.null_count else None

                array_cls = (
                    pyarrow.LargeListArray if arr_is_large else pyarrow.ListArray
                )
                return array_cls.from_arrays(
                    offsets_array,
                    scalar_values,
                    mask=null_mask,
                )

            return _list_flatten_arrow(self._expr)

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_flatten_python(arr: pyarrow.Array) -> pyarrow.Array:
            """Fallback implementation when PyArrow list_flatten is unavailable."""
            import itertools

            flattened_lists = []
            for list_of_lists in arr.to_pylist():
                if list_of_lists is None:
                    flattened_lists.append(None)
                    continue

                if not isinstance(list_of_lists, list):
                    raise TypeError(
                        "list.flatten() requires a list column whose elements are also lists."
                    )

                valid_sublists = [sub for sub in list_of_lists if sub is not None]
                for sub in valid_sublists:
                    if not isinstance(sub, list):
                        raise TypeError(
                            "list.flatten() requires a list column whose elements are also lists."
                        )
                flattened = list(itertools.chain.from_iterable(valid_sublists))
                flattened_lists.append(flattened)

            result_type = None
            if return_dtype.is_arrow_type():
                result_type = return_dtype.to_arrow_dtype()

            return pyarrow.array(flattened_lists, type=result_type)

        return _list_flatten_python(self._expr)
