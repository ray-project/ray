"""Unit tests for list namespace expressions.

These tests verify expression construction logic without requiring Ray.
"""

import pyarrow as pa
import pytest

from ray.data.expressions import PyArrowComputeUDFExpr, UDFExpr, col
from ray.data.namespace_expressions.list_namespace import (
    _list_element_from_end,
    _slice_lists_with_negative_bounds,
)

ROWS = [[1, 2, 3], None, [], [4, None]]

LIST_TYPES = [
    pa.list_(pa.int64()),
    pa.large_list(pa.int64()),
    pa.list_view(pa.int64()),
    pa.large_list_view(pa.int64()),
    pa.list_(pa.field("element", pa.int64())),
]


def _null_row_spanning_values() -> pa.Array:
    """[[0, 1], None, [4, 5, 6]] where the null row's offsets cover [2, 3]."""
    return pa.ListArray.from_arrays(
        pa.array([0, 2, 4, 7], type=pa.int32()),
        pa.array(range(7)),
        mask=pa.array([False, True, False]),
    )


class TestListNamespaceErrors:
    """Tests for proper error handling in list namespace."""

    def test_list_invalid_index_type(self):
        """Test list bracket notation rejects invalid types."""
        with pytest.raises(TypeError, match="List indices must be integers or slices"):
            col("items").list["invalid"]


class TestListNegativeIndexExpressions:
    """Non-negative indices keep the pushdown-capable pc.* expression."""

    def test_non_negative_index_and_slice_use_compute_expr(self):
        assert isinstance(col("items").list[0], PyArrowComputeUDFExpr)
        assert isinstance(col("items").list[1:3], PyArrowComputeUDFExpr)

    @pytest.mark.parametrize(
        "make_expr",
        [
            lambda: col("items").list[-1],
            lambda: col("items").list.get(-2),
            lambda: col("items").list[-2:],
            lambda: col("items").list[:-1],
            lambda: col("items").list.slice(1, -1),
        ],
    )
    def test_negative_index_and_slice_build_udf(self, make_expr):
        expr = make_expr()
        assert isinstance(expr, UDFExpr)
        assert not isinstance(expr, PyArrowComputeUDFExpr)


class TestListElementFromEnd:
    @pytest.mark.parametrize("list_type", LIST_TYPES)
    def test_last_element(self, list_type):
        arr = pa.array([[1, 2, 3], None, [4, None], [5]], type=list_type)
        result = _list_element_from_end(arr, -1)
        assert result.type == pa.int64()
        assert result.to_pylist() == [3, None, None, 5]

    def test_fixed_size_list(self):
        arr = pa.array([[1, 2, 3], None, [4, 5, 6]], type=pa.list_(pa.int64(), 3))
        assert _list_element_from_end(arr, -3).to_pylist() == [1, None, 4]

    def test_null_row_spanning_values(self):
        arr = _null_row_spanning_values()
        assert _list_element_from_end(arr, -1).to_pylist() == [1, None, 6]
        assert _list_element_from_end(arr.slice(1), -2).to_pylist() == [None, 5]

    def test_sliced_array(self):
        arr = pa.array([[1], [2, 3], [4, 5, 6], [7]]).slice(1, 2)
        assert _list_element_from_end(arr, -2).to_pylist() == [2, 5]

    def test_empty_array(self):
        result = _list_element_from_end(pa.array([], type=pa.list_(pa.int64())), -1)
        assert result.to_pylist() == []
        assert result.type == pa.int64()

    @pytest.mark.parametrize("index", [-2, -5])
    def test_out_of_bounds(self, index):
        arr = pa.array([[1, 2, 3], [4], None])
        with pytest.raises(
            pa.lib.ArrowInvalid, match=f"Index {index} is out of bounds"
        ):
            _list_element_from_end(arr, index)

    def test_empty_list_out_of_bounds(self):
        with pytest.raises(pa.lib.ArrowInvalid, match="Index -1 is out of bounds"):
            _list_element_from_end(pa.array([[1], []]), -1)


class TestSliceListsWithNegativeBounds:
    @pytest.mark.parametrize("list_type", LIST_TYPES)
    @pytest.mark.parametrize(
        "start,stop,step",
        [
            (-2, None, 1),
            (None, -1, 1),
            (-3, -1, 1),
            (1, -1, 1),
            (-1, 1, 1),
            (-10, 10, 1),
            (-1, -3, 1),
            (-4, None, 2),
            (None, -1, 3),
        ],
    )
    def test_matches_python_slicing(self, list_type, start, stop, step):
        arr = pa.array(ROWS, type=list_type)
        result = _slice_lists_with_negative_bounds(arr, start, stop, step)
        assert result.type == arr.type
        assert result.to_pylist() == [
            None if row is None else row[start:stop:step] for row in ROWS
        ]

    @pytest.mark.parametrize(
        "start,stop,expected_size",
        [(-2, None, 2), (None, -1, 2), (-5, 2, 2), (-1, -3, 0)],
    )
    def test_fixed_size_list_keeps_type(self, start, stop, expected_size):
        rows = [[1, 2, 3], None, [4, 5, 6]]
        arr = pa.array(rows, type=pa.list_(pa.int64(), 3))
        result = _slice_lists_with_negative_bounds(arr, start, stop, 1)
        assert result.type == pa.list_(pa.int64(), expected_size)
        assert result.to_pylist() == [
            None if row is None else row[start:stop] for row in rows
        ]

    def test_null_row_spanning_values(self):
        arr = _null_row_spanning_values()
        result = _slice_lists_with_negative_bounds(arr, -2, None, 1)
        assert result.to_pylist() == [[0, 1], None, [5, 6]]

    def test_sliced_array(self):
        arr = pa.array([[1], [2, 3], [4, 5, 6], [7]]).slice(1, 2)
        result = _slice_lists_with_negative_bounds(arr, None, -1, 1)
        assert result.to_pylist() == [[2], [4, 5]]

    def test_empty_array(self):
        arr = pa.array([], type=pa.list_(pa.int64()))
        result = _slice_lists_with_negative_bounds(arr, -1, None, 1)
        assert result.to_pylist() == []
        assert result.type == arr.type

    @pytest.mark.parametrize("step", [0, -1])
    def test_rejects_non_positive_step(self, step):
        with pytest.raises(
            pa.lib.ArrowInvalid, match="`step` must be greater than or equal to 1"
        ):
            _slice_lists_with_negative_bounds(pa.array([[1, 2]]), -1, None, step)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
