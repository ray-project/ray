import sys
from typing import Union

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.arrow_block import (
    ArrowBlockAccessor,
    ArrowBlockBuilder,
    ArrowBlockColumnAccessor,
    _get_max_chunk_size,
)
from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunked_array, concat
from ray.data._internal.tensor_extensions.arrow import (
    MIN_PYARROW_VERSION_FIXED_SHAPE_TENSOR_SCALAR,
    ArrowTensorArray,
)
from ray.data._internal.utils.arrow_utils import get_pyarrow_version


def simple_array():
    return pa.array([1, 2, None, 6], type=pa.int64())


def simple_chunked_array():
    return pa.chunked_array([pa.array([1, 2]), pa.array([None, 6])])


def _wrap_as_pa_scalar(v, dtype: pa.DataType):
    return pa.scalar(v, type=dtype)


@pytest.mark.parametrize("arr", [simple_array(), simple_chunked_array()])
@pytest.mark.parametrize("as_py", [True, False])
class TestArrowBlockColumnAccessor:
    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 3),
            (False, 4),
        ],
    )
    def test_count(self, arr, ignore_nulls, as_py, expected):
        accessor = ArrowBlockColumnAccessor(arr)
        result = accessor.count(ignore_nulls=ignore_nulls, as_py=as_py)

        if not as_py:
            expected = _wrap_as_pa_scalar(expected, dtype=pa.int64())

        assert result == expected

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 9),
            (False, None),
        ],
    )
    def test_sum(self, arr, ignore_nulls, as_py, expected):
        accessor = ArrowBlockColumnAccessor(arr)
        result = accessor.sum(ignore_nulls=ignore_nulls, as_py=as_py)

        if not as_py:
            expected = _wrap_as_pa_scalar(expected, dtype=pa.int64())

        assert result == expected

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 1),
            (False, None),
        ],
    )
    def test_min(self, arr, ignore_nulls, as_py, expected):
        accessor = ArrowBlockColumnAccessor(arr)
        result = accessor.min(ignore_nulls=ignore_nulls, as_py=as_py)

        if not as_py:
            expected = _wrap_as_pa_scalar(expected, dtype=pa.int64())

        assert result == expected

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 6),
            (False, None),
        ],
    )
    def test_max(self, arr, ignore_nulls, as_py, expected):
        accessor = ArrowBlockColumnAccessor(arr)
        result = accessor.max(ignore_nulls=ignore_nulls, as_py=as_py)

        if not as_py:
            expected = _wrap_as_pa_scalar(expected, dtype=pa.int64())

        assert result == expected

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 3),
            (False, None),
        ],
    )
    def test_mean(self, arr, ignore_nulls, as_py, expected):
        accessor = ArrowBlockColumnAccessor(arr)
        result = accessor.mean(ignore_nulls=ignore_nulls, as_py=as_py)

        if not as_py:
            expected = _wrap_as_pa_scalar(expected, dtype=pa.float64())

        assert result == expected

    @pytest.mark.parametrize(
        "provided_mean, expected",
        [
            (3.0, 14.0),
            (None, 14.0),
        ],
    )
    def test_sum_of_squared_diffs_from_mean(self, arr, provided_mean, as_py, expected):
        accessor = ArrowBlockColumnAccessor(arr)
        result = accessor.sum_of_squared_diffs_from_mean(
            ignore_nulls=True, mean=provided_mean, as_py=as_py
        )

        if not as_py:
            expected = _wrap_as_pa_scalar(expected, dtype=pa.float64())

        assert result == expected

    def test_to_pylist(self, arr, as_py):
        accessor = ArrowBlockColumnAccessor(arr)
        assert accessor.to_pylist() == arr.to_pylist()


@pytest.mark.parametrize(
    "input_,expected_output",
    [
        # Empty chunked array
        (pa.chunked_array([], type=pa.int8()), pa.array([], type=pa.int8())),
        # Fixed-shape tensors
        (
            pa.chunked_array(
                [
                    ArrowTensorArray.from_numpy(np.arange(3).reshape(3, 1)),
                    ArrowTensorArray.from_numpy(np.arange(3).reshape(3, 1)),
                ]
            ),
            ArrowTensorArray.from_numpy(
                np.concatenate(
                    [
                        np.arange(3).reshape(3, 1),
                        np.arange(3).reshape(3, 1),
                    ]
                )
            ),
        ),
        # Ragged (variable-shaped) tensors
        (
            pa.chunked_array(
                [
                    ArrowTensorArray.from_numpy(np.arange(3).reshape(3, 1)),
                    ArrowTensorArray.from_numpy(np.arange(5).reshape(5, 1)),
                ]
            ),
            ArrowTensorArray.from_numpy(
                np.concatenate(
                    [
                        np.arange(3).reshape(3, 1),
                        np.arange(5).reshape(5, 1),
                    ]
                )
            ),
        ),
        # Small (< 2 GiB) arrays
        (
            pa.chunked_array(
                [
                    pa.array([1, 2, 3], type=pa.int16()),
                    pa.array([4, 5, 6], type=pa.int16()),
                ]
            ),
            pa.array([1, 2, 3, 4, 5, 6], type=pa.int16()),
        ),
    ],
)
def test_combine_chunked_array_small(
    input_, expected_output: Union[pa.Array, pa.ChunkedArray]
):
    result = combine_chunked_array(input_)

    assert expected_output.equals(result)


@pytest.mark.parametrize(
    "input_block, fill_column_name, fill_value, expected_output_block",
    [
        (
            pa.Table.from_pydict({"a": [0, 1]}),
            "b",
            2,
            pa.Table.from_pydict({"a": [0, 1], "b": [2, 2]}),
        ),
        (
            pa.Table.from_pydict({"a": [0, 1]}),
            "b",
            pa.scalar(2),
            pa.Table.from_pydict({"a": [0, 1], "b": [2, 2]}),
        ),
    ],
)
def test_fill_column(input_block, fill_column_name, fill_value, expected_output_block):
    block_accessor = ArrowBlockAccessor.for_block(input_block)

    actual_output_block = block_accessor.fill_column(fill_column_name, fill_value)

    assert actual_output_block.equals(expected_output_block)


def test_add_blocks_with_different_column_names():
    builder = ArrowBlockBuilder()

    builder.add_block(pa.Table.from_pydict({"col1": ["spam"]}))
    builder.add_block(pa.Table.from_pydict({"col2": ["foo"]}))
    block = builder.build()

    expected_table = pa.Table.from_pydict(
        {"col1": ["spam", None], "col2": [None, "foo"]}
    )
    assert block.equals(expected_table)


@pytest.mark.parametrize(
    "table_data,max_chunk_size_bytes,expected",
    [
        ({"a": []}, 100, None),
        ({"a": list(range(100))}, 7, 1),
        ({"a": list(range(100))}, 10, 1),
        ({"a": list(range(100))}, 25, 3),
        ({"a": list(range(100))}, 50, 6),
        ({"a": list(range(100))}, 100, 12),
    ],
)
def test_arrow_block_max_chunk_size(table_data, max_chunk_size_bytes, expected):
    table = pa.table(table_data)
    assert _get_max_chunk_size(table, max_chunk_size_bytes) == expected


def test_arrow_block_concat():
    table1 = pa.table(
        {
            "a": [1, 2, 3],
            "s": [{"x": 1} for _ in range(3)],
        }
    )
    table2 = pa.table(
        {
            "b": [4, 5, 6],
        }
    )
    concatenated = concat([table1, table2])
    assert set(concatenated.column_names) == {"a", "s", "b"}
    expected = pa.table(
        {
            "a": [1, 2, 3, None, None, None],
            "s": [{"x": 1} for _ in range(3)] + [None] * 3,
            "b": [None, None, None, 4, 5, 6],
        }
    )
    assert concatenated.select(["a", "s", "b"]) == expected


@pytest.mark.skipif(
    get_pyarrow_version() < MIN_PYARROW_VERSION_FIXED_SHAPE_TENSOR_SCALAR,
    reason="Requires pyarrow>=16 for native FixedShapeTensorArray",
)
def test_iter_rows_native_tensor_null_rows():
    native = pa.FixedShapeTensorArray.from_numpy_ndarray(
        np.array([[10.0, 11.0], [20.0, 21.0]])
    )
    table = pa.table({"id": [0, 1, 2], "x": native.take(pa.array([0, None, 1]))})

    rows = list(ArrowBlockAccessor.for_block(table).iter_rows(public_row_format=True))

    assert [row["id"] for row in rows] == [0, 1, 2]
    np.testing.assert_array_equal(rows[0]["x"], [10.0, 11.0])
    assert rows[1]["x"] is None
    np.testing.assert_array_equal(rows[2]["x"], [20.0, 21.0])


@pytest.mark.skipif(
    get_pyarrow_version() < MIN_PYARROW_VERSION_FIXED_SHAPE_TENSOR_SCALAR,
    reason="Requires pyarrow>=16 for native FixedShapeTensorArray",
)
def test_iter_rows_native_tensor_no_nulls():
    source = np.arange(12, dtype=np.float64).reshape(3, 2, 2)
    table = pa.table({"x": pa.FixedShapeTensorArray.from_numpy_ndarray(source)})

    rows = list(ArrowBlockAccessor.for_block(table).iter_rows(public_row_format=True))

    assert len(rows) == 3
    for row, expected in zip(rows, source):
        assert isinstance(row["x"], np.ndarray)
        assert row["x"].shape == (2, 2)
        np.testing.assert_array_equal(row["x"], expected)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
