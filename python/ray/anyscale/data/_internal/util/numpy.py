from typing import List, Union, Tuple, Any

import numpy as np

from ray.data._internal.util import NULL_SENTINEL


def find_insertion_index(
    columns: List[np.ndarray],
    pivot: Tuple[Union[Any]],
    descending: List[bool],
    _start_from_idx: int = 0,
) -> int:
    """For the given list of *sorted* columns, find the index where ``pivot`` value
    should be added, while maintaining sorted order.

    We do this by iterating over each key column, and binary searching for the
    insertion index in the column. Each binary search shortens the "range" of indices
    (represented by ``left`` and ``right``, which are indices of rows) where the pivot
    value could be inserted.

    Args:
        columns: List of *sorted* arrays (as ndarrays).
        pivot: A (row-like) tuple of corresponding column values, for which insertion
                needs to be determined.
        descending: List of booleans designating whether key columns are in ascending
                    or descending order, since ``np.searchsorted`` expects these in
                    ascending order.

    Returns:
        Index where the pivot value would have been inserted into to maintain sorted
        order of ``key_columns``.
    """

    assert len(columns) > 0, "Expected non-empty list of key columns"

    # NOTE: Left and right offsets track 2 insertion points:
    #
    #   - Left: for value of pivot `P`, it is an index `i`,
    #     such that: A[i - 1] < P <= A[i]
    #   - Right: for value of pivot P, it is an index `i`,
    #     such that: A[i - 1] <= P < A[i]
    #
    # Tracking both is necessary to be able to properly find an appropriate
    # insertion point in the multi-column scenario, since lexicographic ordering
    # in the second (and beyond) columns might not necessarily match the
    # non-lexicographic ordering.
    left, right = _start_from_idx, len(columns[0])

    for col_idx, cur_pivot_val in enumerate(pivot):
        if left == right:
            return right

        # Project column range view for the given [left, right) range
        #
        # This is necessary to make sure that the values in the projected range
        # are in the ascending/descending order (in multi-column scenario)
        column_range_view = columns[col_idx][left:right]

        # Replace null values with sentinel (our custom sentinel implements
        # comparison protocol, unlike default Python's NoneType)
        if cur_pivot_val is None:
            cur_pivot_val = NULL_SENTINEL

        if descending[col_idx] is True:
            # ``np.searchsorted`` expects the array to be sorted in ascending
            # order, so we pass ``sorter``, which is an array of integer indices
            # that turn ``column_range_view`` into ascending order.
            asc_indices = np.arange(len(column_range_view) - 1, -1, -1)

            # The returned index is an index into the ascending order of
            # ``column_range_view``, so we need to subtract it from
            # ``len(column_range_view)`` to get the index in the original descending
            # order of ``column_range_view``.
            left_ins_offset_asc = np.searchsorted(
                column_range_view,
                cur_pivot_val,
                side="left",
                sorter=asc_indices,
            )

            right_ins_offset_asc = np.searchsorted(
                column_range_view,
                cur_pivot_val,
                side="right",
                sorter=asc_indices,
            )

            # NOTE: Converting back from ascending offsets into original
            #       ones, the ordering of the left and right offsets are reversed
            pivot_left_ins_offset = len(column_range_view) - right_ins_offset_asc
            pivot_right_ins_offset = len(column_range_view) - left_ins_offset_asc

        else:

            pivot_left_ins_offset = np.searchsorted(
                column_range_view, cur_pivot_val, side="left"
            )

            pivot_right_ins_offset = np.searchsorted(
                column_range_view, cur_pivot_val, side="right"
            )

        prev_left = left

        left = prev_left + pivot_left_ins_offset
        right = prev_left + pivot_right_ins_offset

    return right if descending[0] is True else left
