"""List namespace for expression operations on list-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pyarrow
import pyarrow.compute as pc

from ray.data._internal.arrow_utils import _combine_as_list_array, _counts_to_offsets
from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


def _ensure_array(arr: pyarrow.Array) -> pyarrow.Array:
    """Convert ChunkedArray to Array if needed."""
    if isinstance(arr, pyarrow.ChunkedArray):
        return arr.combine_chunks()
    return arr


def _is_list_like(pa_type: pyarrow.DataType) -> bool:
    """Return True for list-like Arrow types (list, large_list, fixed_size_list)."""
    return (
        pyarrow.types.is_list(pa_type)
        or pyarrow.types.is_large_list(pa_type)
        or pyarrow.types.is_fixed_size_list(pa_type)
        or (
            hasattr(pyarrow.types, "is_list_view")
            and pyarrow.types.is_list_view(pa_type)
        )
        or (
            hasattr(pyarrow.types, "is_large_list_view")
            and pyarrow.types.is_large_list_view(pa_type)
        )
    )


def _infer_flattened_dtype(expr: "Expr") -> DataType:
    """Infer the return DataType after flattening one level of list nesting."""
    if not expr.data_type.is_arrow_type():
        return DataType(object)

    arrow_type = expr.data_type.to_arrow_dtype()
    if not _is_list_like(arrow_type):
        return DataType(object)

    child_type = arrow_type.value_type
    if not _is_list_like(child_type):
        return DataType(object)

    if pyarrow.types.is_large_list(arrow_type):
        return DataType.from_arrow(pyarrow.large_list(child_type.value_type))
    else:
        return DataType.from_arrow(pyarrow.list_(child_type.value_type))


def _validate_nested_list(arr_type: pyarrow.DataType) -> None:
    """Raise TypeError if arr_type is not a list of lists."""
    if not _is_list_like(arr_type):
        raise TypeError(
            "list.flatten() requires a list column whose elements are also lists."
        )

    if not _is_list_like(arr_type.value_type):
        raise TypeError(
            "list.flatten() requires a list column whose elements are also lists."
        )


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
        self,
        order: Literal["ascending", "descending"] = "ascending",
        null_placement: Literal["at_start", "at_end"] = "at_end",
    ) -> "UDFExpr":
        """Sort the elements within each (nested) list.

        Args:
            order: Sorting order, must be ``\"ascending\"`` or ``\"descending\"``.
            null_placement: Placement for null values, ``\"at_start\"`` or ``\"at_end\"``.

        Returns:
            UDFExpr providing the sorted lists.

        Example:
            >>> from ray.data.expressions import col
            >>> # [[3,1],[2,None]] -> [[1,3],[2,None]]
            >>> expr = col("items").list.sort()  # doctest: +SKIP
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

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_sort(arr: pyarrow.Array) -> pyarrow.Array:
            # Approach:
            # 1) Normalize fixed_size_list -> list for list_* kernels (preserve nulls).
            # 2) Flatten to (row_index, value) pairs, sort by row then value.
            # 3) Rebuild list array using per-row lengths and restore original type.
            arr = _ensure_array(arr)

            arr_type = arr.type
            arr_dtype = DataType.from_arrow(arr_type)
            if not arr_dtype.is_list_type():
                raise TypeError("list.sort() requires a list column.")

            original_type = arr_type
            null_mask = arr.is_null() if arr.null_count else None
            sort_arr = arr
            if pyarrow.types.is_fixed_size_list(arr_type):
                # Example: FixedSizeList<2>[ [3,1], None, [2,4] ]
                # Fill null row -> [[3,1],[None,None],[2,4]], cast to list<child> for sort,
                # then cast back to fixed_size to preserve schema. list_* kernels operate
                # on list/large_list, so we cast fixed_size_list<T> to list<T> here.
                child_type = arr_type.value_type
                list_size = arr_type.list_size
                if null_mask is not None:
                    # Fill null rows with fixed-size null lists so each row keeps
                    # the same list_size when we sort and rebuild offsets.
                    filler_values = pyarrow.nulls(len(arr) * list_size, type=child_type)
                    filler = pyarrow.FixedSizeListArray.from_arrays(
                        filler_values, list_size
                    )
                    sort_arr = pc.if_else(null_mask, filler, arr)
                list_type = pyarrow.list_(child_type)
                sort_arr = sort_arr.cast(list_type)
                arr_type = sort_arr.type
            # Flatten to (row_index, value) pairs, sort within each row by value.
            values = pc.list_flatten(sort_arr)
            if len(values):
                row_indices = pc.list_parent_indices(sort_arr)
                struct = pyarrow.StructArray.from_arrays(
                    [row_indices, values],
                    ["row", "value"],
                )
                sorted_indices = pc.sort_indices(
                    struct,
                    sort_keys=[("row", "ascending"), ("value", order)],
                    null_placement=null_placement,
                )
                values = pc.take(values, sorted_indices)

            # Reconstruct list array with original row boundaries and nulls.
            lengths = pc.list_value_length(sort_arr)
            lengths = pc.fill_null(lengths, 0)
            is_large = pyarrow.types.is_large_list(arr_type)
            offsets = _counts_to_offsets(lengths)
            sorted_arr = _combine_as_list_array(
                offsets=offsets,
                values=values,
                is_large=is_large,
                null_mask=null_mask,
            )

            if pyarrow.types.is_fixed_size_list(original_type):
                sorted_arr = sorted_arr.cast(original_type)

            return sorted_arr

        return _list_sort(self._expr)

    def flatten(self) -> "UDFExpr":
        """Flatten one level of nesting for each list value."""

        return_dtype = _infer_flattened_dtype(self._expr)

        @pyarrow_udf(return_dtype=return_dtype)
        def _list_flatten(arr: pyarrow.Array) -> pyarrow.Array:
            # Approach:
            # 1) Flatten list<list<T>> to a flat values array and parent indices.
            # 2) Count values per original row.
            # 3) Rebuild list array using offsets while preserving top-level nulls.
            arr = _ensure_array(arr)

            _validate_nested_list(arr.type)

            inner_lists: pyarrow.Array = pc.list_flatten(arr)
            all_scalars: pyarrow.Array = pc.list_flatten(inner_lists)

            n_rows: int = len(arr)
            if len(all_scalars) == 0:
                # All rows are empty/None after flatten, so build zero counts to
                # preserve row count and produce empty lists for each row.
                counts = pyarrow.array(np.repeat(0, n_rows), type=pyarrow.int64())
                offsets = _counts_to_offsets(counts)
            else:
                # Example: arr = [[[1,2],[3]], [[4], None], None]
                # inner_lists = [[1,2],[3],[4],None], all_scalars = [1,2,3,4]
                # parent(arr)=[0,0,1,1], parent(inner)=[0,0,1,2] -> row_indices=[0,0,0,1]
                # counts=[3,1,0] -> offsets=[0,3,4,4]
                row_indices: pyarrow.Array = pc.take(
                    pc.list_parent_indices(arr),
                    pc.list_parent_indices(inner_lists),
                )

                vc: pyarrow.StructArray = pc.value_counts(row_indices)
                rows_with_scalars: pyarrow.Array = pc.struct_field(vc, "values")
                scalar_counts: pyarrow.Array = pc.struct_field(vc, "counts")

                # Compute per-row counts of flattened scalars. value_counts gives counts
                # only for rows that appear, so we map those counts back onto the full
                # row range [0, n_rows) and fill missing rows with 0.
                row_sequence: pyarrow.Array = pyarrow.array(
                    np.arange(n_rows, dtype=np.int64), type=pyarrow.int64()
                )
                positions: pyarrow.Array = pc.index_in(
                    row_sequence, value_set=rows_with_scalars
                )

                counts: pyarrow.Array = pc.if_else(
                    pc.is_null(positions),
                    0,
                    pc.take(scalar_counts, pc.fill_null(positions, 0)),
                )

                offsets = _counts_to_offsets(counts)

            is_large: bool = pyarrow.types.is_large_list(arr.type)
            null_mask: pyarrow.Array | None = arr.is_null() if arr.null_count else None
            # Rebuild a list/large_list array while preserving top-level nulls.
            return _combine_as_list_array(
                offsets=offsets,
                values=all_scalars,
                is_large=is_large,
                null_mask=null_mask,
            )

        return _list_flatten(self._expr)
