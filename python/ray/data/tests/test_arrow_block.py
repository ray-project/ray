import base64
import os
import sys
import types
from tempfile import TemporaryDirectory
from typing import Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import ArrowInvalid

import ray
from ray._private.test_utils import run_string_as_driver
from ray.air.util.tensor_extensions.arrow import (
    ArrowTensorArray,
)
from ray.data._internal.arrow_block import (
    ArrowBlockAccessor,
    ArrowBlockBuilder,
    ArrowBlockColumnAccessor,
    _get_max_chunk_size,
)
from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunked_array
from ray.data._internal.util import GiB, MiB
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.extensions.object_extension import _object_extension_type_allowed


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


@pytest.fixture(scope="module")
def binary_dataset_single_file_gt_2gb():
    total_size = int(2.1 * GiB)
    chunk_size = 256 * MiB
    num_chunks = total_size // chunk_size
    remainder = total_size % chunk_size

    with TemporaryDirectory() as tmp_dir:
        dataset_path = f"{tmp_dir}/binary_dataset_gt_2gb_single_file"

        # Create directory
        os.mkdir(dataset_path)

        with open(f"{dataset_path}/chunk.bin", "wb") as f:
            for i in range(num_chunks):
                f.write(b"a" * chunk_size)

                print(f">>> Written chunk #{i}")

            if remainder:
                f.write(b"a" * remainder)

        print(f">>> Wrote chunked dataset at: {dataset_path}")

        yield dataset_path, total_size

        print(f">>> Cleaning up dataset: {dataset_path}")


@pytest.mark.parametrize(
    "col_name",
    [
        "bytes",
        # TODO fix numpy conversion
        # "text",
    ],
)
def test_single_row_gt_2gb(
    ray_start_regular,
    restore_data_context,
    binary_dataset_single_file_gt_2gb,
    col_name,
):
    # Disable (automatic) fallback to `ArrowPythonObjectType` extension type
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False

    dataset_path, target_binary_size = binary_dataset_single_file_gt_2gb

    def _id(row):
        bs = row[col_name]
        assert round(len(bs) / GiB, 1) == round(target_binary_size / GiB, 1)
        return row

    if col_name == "text":
        ds = ray.data.read_text(dataset_path)
    elif col_name == "bytes":
        ds = ray.data.read_binary_files(dataset_path)

    total = ds.map(_id).count()

    assert total == 1


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

    expected_output.equals(result)


def test_combine_chunked_fixed_width_array_large():
    """Verifies `combine_chunked_array` on fixed-width arrays > 2 GiB, produces
    single contiguous PA Array"""

    # 144 MiB
    ones_1gb = np.ones(shape=(550, 128, 128, 4), dtype=np.int32()).ravel()

    # Total ~2.15 GiB
    input_ = pa.chunked_array(
        [
            pa.array(ones_1gb),
        ]
        * 16
    )

    assert round(input_.nbytes / GiB, 2) == 2.15

    result = combine_chunked_array(input_)

    assert isinstance(result, pa.Int32Array)


@pytest.mark.parametrize(
    "array_type,input_factory",
    [
        (
            pa.binary(),
            lambda num_bytes: np.arange(num_bytes, dtype=np.uint8).tobytes(),
        ),
        (
            pa.string(),
            lambda num_bytes: base64.encodebytes(
                np.arange(num_bytes, dtype=np.int8).tobytes()
            ).decode("ascii"),
        ),
        (pa.list_(pa.uint8()), lambda num_bytes: np.arange(num_bytes, dtype=np.uint8)),
    ],
)
def test_combine_chunked_variable_width_array_large(array_type, input_factory):
    """Verifies `combine_chunked_array` on variable-width arrays > 2 GiB,
    safely produces new ChunkedArray with provided chunks recombined into
    larger ones up to INT32_MAX in size"""

    one_half_gb_arr = pa.array([input_factory(GiB / 2)], type=array_type)
    chunked_arr = pa.chunked_array(
        [one_half_gb_arr, one_half_gb_arr, one_half_gb_arr, one_half_gb_arr]
    )

    # 2 GiB + offsets (4 x int32)
    num_bytes = chunked_arr.nbytes
    expected_num_bytes = 4 * one_half_gb_arr.nbytes

    num_chunks = len(chunked_arr.chunks)
    assert num_chunks == 4
    assert num_bytes == expected_num_bytes

    # Assert attempt to combine directly fails
    with pytest.raises(ArrowInvalid):
        chunked_arr.combine_chunks()

    # Safe combination succeeds by avoiding overflowing combination
    combined = combine_chunked_array(chunked_arr)

    num_bytes = combined.nbytes

    num_chunks = len(combined.chunks)
    assert num_chunks == 2
    assert num_bytes == expected_num_bytes


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


def test_random_shuffle(ray_start_regular_shared):
    TOTAL_ROWS = 10000
    table = pa.table({"id": pa.array(range(TOTAL_ROWS))})
    block_accessor = ArrowBlockAccessor(table)

    # Perform the random shuffle
    shuffled_table = block_accessor.random_shuffle(random_seed=None)
    assert shuffled_table.num_rows == TOTAL_ROWS

    # Access the shuffled data
    block_accessor = ArrowBlockAccessor(shuffled_table)
    shuffled_data = block_accessor.to_pandas()["id"].tolist()
    original_data = list(range(TOTAL_ROWS))

    # Ensure the shuffled data is not identical to the original
    assert (
        shuffled_data != original_data
    ), "Shuffling should result in a different order"

    # Ensure the entire set of original values is still in the shuffled dataset
    assert (
        sorted(shuffled_data) == original_data
    ), "The shuffled data should contain all the original values"


def test_register_arrow_types(tmp_path):
    # Test that our custom arrow extension types are registered on initialization.
    ds = ray.data.from_items(np.zeros((8, 8, 8), dtype=np.int64))
    tmp_file = f"{tmp_path}/test.parquet"
    ds.write_parquet(tmp_file)

    ds = ray.data.read_parquet(tmp_file)
    schema = "Column  Type\n------  ----\nitem    ArrowTensorTypeV2(shape=(8, 8), dtype=int64)"
    assert str(ds.schema()) == schema

    # Also run in driver script to eliminate existing imports.
    driver_script = """import ray
ds = ray.data.read_parquet("{0}")
schema = ds.schema()
assert str(schema) == \"\"\"{1}\"\"\"
""".format(
        tmp_file, schema
    )
    run_string_as_driver(driver_script)


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension type not supported."
)
def test_dict_doesnt_fallback_to_pandas_block(ray_start_regular_shared):
    # If the UDF returns a column with dict, previously, we would
    # fall back to pandas, because we couldn't convert it to
    # an Arrow block. This test checks that the block
    # construction now correctly goes to Arrow.
    def fn(batch):
        batch["data_dict"] = [{"data": 0} for _ in range(len(batch["id"]))]
        batch["data_objects"] = [
            types.SimpleNamespace(a=1, b="test") for _ in range(len(batch["id"]))
        ]
        return batch

    ds = ray.data.range(10).map_batches(fn)
    ds = ds.materialize()
    block = ray.get(ds.get_internal_block_refs()[0])
    assert isinstance(block, pa.Table), type(block)
    df_from_block = block.to_pandas()
    assert df_from_block["data_dict"].iloc[0] == {"data": 0}
    assert df_from_block["data_objects"].iloc[0] == types.SimpleNamespace(a=1, b="test")

    def fn2(batch):
        batch["data_none"] = [None for _ in range(len(batch["id"]))]
        return batch

    ds2 = ray.data.range(10).map_batches(fn2)
    ds2 = ds2.materialize()
    block = ray.get(ds2.get_internal_block_refs()[0])
    assert isinstance(block, pa.Table), type(block)
    df_from_block = block.to_pandas()
    assert df_from_block["data_none"].iloc[0] is None


# Test for https://github.com/ray-project/ray/issues/49338.
def test_build_block_with_null_column(ray_start_regular_shared):
    # The blocks need to contain a tensor column to trigger the bug.
    block1 = BlockAccessor.batch_to_block(
        {"string": [None], "array": np.zeros((1, 2, 2))}
    )
    block2 = BlockAccessor.batch_to_block(
        {"string": ["spam"], "array": np.zeros((1, 2, 2))}
    )

    builder = ArrowBlockBuilder()
    builder.add_block(block1)
    builder.add_block(block2)
    block = builder.build()

    rows = list(BlockAccessor.for_block(block).iter_rows(True))
    assert len(rows) == 2
    assert rows[0]["string"] is None
    assert rows[1]["string"] == "spam"
    assert np.array_equal(rows[0]["array"], np.zeros((2, 2)))
    assert np.array_equal(rows[1]["array"], np.zeros((2, 2)))


def test_add_rows_with_different_column_names():
    builder = ArrowBlockBuilder()

    builder.add({"col1": "spam"})
    builder.add({"col2": "foo"})
    block = builder.build()

    expected_table = pa.Table.from_pydict(
        {"col1": ["spam", None], "col2": [None, "foo"]}
    )
    assert block.equals(expected_table)


def test_add_blocks_with_different_column_names():
    builder = ArrowBlockBuilder()

    builder.add_block(pa.Table.from_pydict({"col1": ["spam"]}))
    builder.add_block(pa.Table.from_pydict({"col2": ["foo"]}))
    block = builder.build()

    expected_table = pa.Table.from_pydict(
        {"col1": ["spam", None], "col2": [None, "foo"]}
    )
    assert block.equals(expected_table)


def test_arrow_block_timestamp_ns(ray_start_regular_shared):
    # Input data with nanosecond precision timestamps
    data_rows = [
        {"col1": 1, "col2": pd.Timestamp("2023-01-01T00:00:00.123456789")},
        {"col1": 2, "col2": pd.Timestamp("2023-01-01T01:15:30.987654321")},
        {"col1": 3, "col2": pd.Timestamp("2023-01-01T02:30:15.111111111")},
        {"col1": 4, "col2": pd.Timestamp("2023-01-01T03:45:45.222222222")},
        {"col1": 5, "col2": pd.Timestamp("2023-01-01T05:00:00.333333333")},
    ]

    # Initialize ArrowBlockBuilder
    arrow_builder = ArrowBlockBuilder()
    for row in data_rows:
        arrow_builder.add(row)
    arrow_block = arrow_builder.build()

    assert arrow_block.schema.field("col2").type == pa.timestamp("ns")
    for i, row in enumerate(data_rows):
        result_timestamp = arrow_block["col2"][i].as_py()
        # Convert both values to pandas Timestamp to preserve nanosecond precision for
        # comparison.
        assert pd.Timestamp(row["col2"]) == pd.Timestamp(
            result_timestamp
        ), f"Timestamp mismatch at row {i} in ArrowBlockBuilder output"


def test_arrow_nan_element():
    ds = ray.data.from_items(
        [
            1.0,
            1.0,
            2.0,
            np.nan,
            np.nan,
        ]
    )
    ds = ds.groupby("item").count()
    ds = ds.filter(lambda v: np.isnan(v["item"]))
    result = ds.take_all()
    assert result[0]["count()"] == 2


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
