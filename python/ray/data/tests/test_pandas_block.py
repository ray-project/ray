import inspect
import random
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.data
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.data._internal.pandas_block import (
    PandasBlockAccessor,
    PandasBlockBuilder,
    PandasBlockColumnAccessor,
)
from ray.data._internal.util import is_null
from ray.data.extensions.object_extension import _object_extension_type_allowed

# Set seed for the test for size as it related to sampling
np.random.seed(42)

# Tolerance for size comparison (1%)
SIZE_TOLERANCE = 0.01
# Target block size: 128MB
TARGET_SIZE_BYTES = 128 * 1024 * 1024


def get_actual_object_store_size(block_ref: ray.ObjectRef) -> int:
    """Get actual object store size using get_local_object_locations."""
    locations = ray.experimental.get_local_object_locations([block_ref])
    if block_ref not in locations:
        raise ValueError(f"Block reference {block_ref} not found in object store")
    object_size = locations[block_ref].get("object_size")
    if object_size is None:
        raise ValueError(f"Object size not available for {block_ref}")
    return object_size


def simple_series(null):
    return pd.Series([1, 2, null, 6])


@pytest.mark.parametrize("arr", [simple_series(None), simple_series(np.nan)])
class TestPandasBlockColumnAccessor:
    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 3),
            (False, 4),
        ],
    )
    def test_count(self, arr, ignore_nulls, expected):
        accessor = PandasBlockColumnAccessor(arr)
        result = accessor.count(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 9),
            (False, np.nan),
        ],
    )
    def test_sum(self, arr, ignore_nulls, expected):
        accessor = PandasBlockColumnAccessor(arr)
        result = accessor.sum(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or is_null(result) and is_null(expected)

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 1),
            (False, np.nan),
        ],
    )
    def test_min(self, arr, ignore_nulls, expected):
        accessor = PandasBlockColumnAccessor(arr)
        result = accessor.min(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or is_null(result) and is_null(expected)

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 6),
            (False, np.nan),
        ],
    )
    def test_max(self, arr, ignore_nulls, expected):
        accessor = PandasBlockColumnAccessor(arr)
        result = accessor.max(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or is_null(result) and is_null(expected)

    @pytest.mark.parametrize(
        "ignore_nulls, expected",
        [
            (True, 3.0),
            (False, np.nan),
        ],
    )
    def test_mean(self, arr, ignore_nulls, expected):
        accessor = PandasBlockColumnAccessor(arr)
        result = accessor.mean(ignore_nulls=ignore_nulls, as_py=True)
        assert result == expected or is_null(result) and is_null(expected)

    @pytest.mark.parametrize(
        "provided_mean, expected",
        [
            (3.0, 14.0),
            (None, 14.0),
        ],
    )
    def test_sum_of_squared_diffs_from_mean(self, arr, provided_mean, expected):
        accessor = PandasBlockColumnAccessor(arr)
        result = accessor.sum_of_squared_diffs_from_mean(
            ignore_nulls=True, mean=provided_mean, as_py=True
        )
        assert result == expected or is_null(result) and is_null(expected)

    def test_to_pylist(self, arr):
        accessor = PandasBlockColumnAccessor(arr)

        result = accessor.to_pylist()
        expected = arr.to_list()

        assert all(
            [a == b or is_null(a) and is_null(b) for a, b in zip(expected, result)]
        )


class TestPandasBlockColumnAccessorAllNullSeries:
    @pytest.fixture
    def all_null_series(self):
        return pd.Series([None] * 3, dtype=np.float64)

    def test_count_all_null(self, all_null_series):
        accessor = PandasBlockColumnAccessor(all_null_series)
        # When ignoring nulls, count should be 0; otherwise, count returns length.
        assert accessor.count(ignore_nulls=True, as_py=True) == 0
        assert accessor.count(ignore_nulls=False, as_py=True) == len(all_null_series)

    @pytest.mark.parametrize("ignore_nulls", [True, False])
    def test_sum_all_null(self, all_null_series, ignore_nulls):
        accessor = PandasBlockColumnAccessor(all_null_series)
        result = accessor.sum(ignore_nulls=ignore_nulls)
        assert is_null(result)

    @pytest.mark.parametrize("ignore_nulls", [True, False])
    def test_min_all_null(self, all_null_series, ignore_nulls):
        accessor = PandasBlockColumnAccessor(all_null_series)
        result = accessor.min(ignore_nulls=ignore_nulls, as_py=True)
        assert is_null(result)

    @pytest.mark.parametrize("ignore_nulls", [True, False])
    def test_max_all_null(self, all_null_series, ignore_nulls):
        accessor = PandasBlockColumnAccessor(all_null_series)
        result = accessor.max(ignore_nulls=ignore_nulls)
        assert is_null(result)

    @pytest.mark.parametrize("ignore_nulls", [True, False])
    def test_mean_all_null(self, all_null_series, ignore_nulls):
        accessor = PandasBlockColumnAccessor(all_null_series)
        result = accessor.mean(ignore_nulls=ignore_nulls)
        assert is_null(result)

    @pytest.mark.parametrize("ignore_nulls", [True, False])
    def test_sum_of_squared_diffs_all_null(self, all_null_series, ignore_nulls):
        accessor = PandasBlockColumnAccessor(all_null_series)
        result = accessor.sum_of_squared_diffs_from_mean(
            ignore_nulls=ignore_nulls, mean=None
        )
        assert is_null(result)


@pytest.mark.parametrize(
    "input_block, fill_column_name, fill_value, expected_output_block",
    [
        (
            pd.DataFrame({"a": [0, 1]}),
            "b",
            2,
            pd.DataFrame({"a": [0, 1], "b": [2, 2]}),
        ),
    ],
)
def test_fill_column(input_block, fill_column_name, fill_value, expected_output_block):
    block_accessor = PandasBlockAccessor.for_block(input_block)

    actual_output_block = block_accessor.fill_column(fill_column_name, fill_value)

    assert actual_output_block.equals(expected_output_block)


def test_pandas_block_timestamp_ns(ray_start_regular_shared):
    # Input data with nanosecond precision timestamps
    data_rows = [
        {"col1": 1, "col2": pd.Timestamp("2023-01-01T00:00:00.123456789")},
        {"col1": 2, "col2": pd.Timestamp("2023-01-01T01:15:30.987654321")},
        {"col1": 3, "col2": pd.Timestamp("2023-01-01T02:30:15.111111111")},
        {"col1": 4, "col2": pd.Timestamp("2023-01-01T03:45:45.222222222")},
        {"col1": 5, "col2": pd.Timestamp("2023-01-01T05:00:00.333333333")},
    ]

    # Initialize PandasBlockBuilder
    pandas_builder = PandasBlockBuilder()
    for row in data_rows:
        pandas_builder.add(row)
    pandas_block = pandas_builder.build()

    assert pd.api.types.is_datetime64_ns_dtype(pandas_block["col2"])

    for original_row, result_row in zip(
        data_rows, pandas_block.to_dict(orient="records")
    ):
        assert (
            original_row["col2"] == result_row["col2"]
        ), "Timestamp mismatch in PandasBlockBuilder output"


@pytest.mark.skipif(
    _object_extension_type_allowed(), reason="Objects can be put into Arrow"
)
def test_dict_fallback_to_pandas_block(ray_start_regular_shared):
    # If the UDF returns a column with dict, this throws
    # an error during block construction because we cannot cast dicts
    # to a supported arrow type. This test checks that the block
    # construction falls back to pandas and still succeeds.
    def fn(batch):
        batch["data_dict"] = [{"data": 0} for _ in range(len(batch["id"]))]
        return batch

    ds = ray.data.range(10).map_batches(fn)
    ds = ds.materialize()
    block = ray.get(ds.get_internal_block_refs()[0])
    # TODO: Once we support converting dict to a supported arrow type,
    # the block type should be Arrow.
    assert isinstance(block, pd.DataFrame)

    def fn2(batch):
        batch["data_none"] = [None for _ in range(len(batch["id"]))]
        return batch

    ds2 = ray.data.range(10).map_batches(fn2)
    ds2 = ds2.materialize()
    block = ray.get(ds2.get_internal_block_refs()[0])
    assert isinstance(block, pd.DataFrame)


def _log_size_estimation(block: pd.DataFrame):
    """Helper function to log size estimation comparison.

    Note: Pandas DataFrames are serialized as Arrow tables in Ray's object
    store. The size_bytes() method should estimate the Arrow-serialized size,
    not the in-memory Pandas size. The actual_size here is the Arrow-serialized
    size in the object store.
    """
    # Get caller information
    caller_frame = inspect.stack()[1]
    caller_name = caller_frame.function

    block_accessor = PandasBlockAccessor.for_block(block)
    estimated_size = block_accessor.size_bytes()

    block_ref = ray.put(block)
    actual_size = get_actual_object_store_size(block_ref)

    size_diff = actual_size - estimated_size
    size_diff_percent = (abs(size_diff) / actual_size) * 100

    if size_diff > 0:
        direction = "UNDERESTIMATED"
    elif size_diff < 0:
        direction = "OVERESTIMATED"
    else:
        direction = "EXACT"

    # Always log when there's over or underestimation
    if size_diff != 0:
        print(
            f"[{caller_name}] Size estimation {direction}: "
            f"estimated={estimated_size:,} bytes, "
            f"actual={actual_size:,} bytes, "
            f"difference={abs(size_diff):,} bytes "
            f"({size_diff_percent:.2f}% error)"
        )


class TestSizeBytes:
    def test_small(ray_start_regular_shared):
        animals = ["Flamingo", "Centipede"]
        block = pd.DataFrame({"animals": animals})
        _log_size_estimation(block)

    def test_large_str(ray_start_regular_shared):
        animals = [
            random.choice(["alligator", "crocodile", "centipede", "flamingo"])
            for i in range(100_000)
        ]
        block = pd.DataFrame({"animals": animals})
        block["animals"] = block["animals"].astype("string")
        _log_size_estimation(block)

    def test_large_str_object(ray_start_regular_shared):
        """Note - this test breaks if you refactor/move the list of animals."""
        num = 100_000
        animals = [
            random.choice(["alligator", "crocodile", "centipede", "flamingo"])
            for i in range(num)
        ]
        block = pd.DataFrame({"animals": animals})
        _log_size_estimation(block)

    def test_large_floats(ray_start_regular_shared):
        animals = [random.random() for i in range(100_000)]
        block = pd.DataFrame({"animals": animals})
        _log_size_estimation(block)

    def test_bytes_object(ray_start_regular_shared):
        def generate_data(batch):
            for _ in range(8):
                yield {"data": [[b"\x00" * 128 * 1024 * 128]]}

        ds = (
            ray.data.range(1, override_num_blocks=1)
            .map_batches(generate_data, batch_size=1)
            .map_batches(lambda batch: batch, batch_format="pandas")
        )

        true_value = 128 * 1024 * 128 * 8
        for bundle in ds.iter_internal_ref_bundles():
            size = bundle.size_bytes()
            # assert that true_value is within 10% of bundle.size_bytes()
            assert size == pytest.approx(true_value, rel=0.1), (
                size,
                true_value,
            )

    def test_nested_numpy(ray_start_regular_shared):
        size = 1024
        rows = 1_000
        data = [
            np.random.randint(size=size, low=0, high=100, dtype=np.int8)
            for _ in range(rows)
        ]
        df = pd.DataFrame({"data": data})
        _log_size_estimation(df)

    def test_nested_objects(ray_start_regular_shared):
        size = 10
        rows = 10_000
        lists = [[random.randint(0, 100) for _ in range(size)] for _ in range(rows)]
        data = {"lists": lists}
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_mixed_types(ray_start_regular_shared):
        rows = 10_000

        data = {
            "integers": [random.randint(0, 100) for _ in range(rows)],
            "floats": [random.random() for _ in range(rows)],
            "strings": [
                random.choice(["apple", "banana", "cherry"]) for _ in range(rows)
            ],
            "object": [b"\x00" * 128 for _ in range(rows)],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_nested_lists_strings(ray_start_regular_shared):
        rows = 5_000
        nested_lists = ["a"] * 3 + ["bb"] * 4 + ["ccc"] * 3
        data = {
            "nested_lists": [nested_lists for _ in range(rows)],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    @pytest.mark.parametrize("size", [10, 1024])
    def test_multi_level_nesting(ray_start_regular_shared, size):
        rows = 1_000
        data = {
            "complex": [
                {"list": [np.random.rand(size)], "value": {"key": "val"}}
                for _ in range(rows)
            ],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_boolean(ray_start_regular_shared):
        data = [random.choice([True, False, None]) for _ in range(100_000)]
        block = pd.DataFrame({"flags": pd.Series(data, dtype="boolean")})
        _log_size_estimation(block)

    def test_arrow(ray_start_regular_shared):
        data = [
            random.choice(["alligator", "crocodile", "flamingo"]) for _ in range(50_000)
        ]
        arrow_dtype = pd.ArrowDtype(pa.string())
        block = pd.DataFrame({"animals": pd.Series(data, dtype=arrow_dtype)})
        _log_size_estimation(block)

    def test_tensors(ray_start_regular_shared):
        """Test size estimation with tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*8 bytes), need ~32K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 8  # float64
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.arange(i * 1000, (i + 1) * 1000, dtype=np.float64).reshape(
                tensor_shape
            )
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_large(ray_start_regular_shared):
        """Test size estimation with larger tensor columns."""
        # Target ~128MB: each tensor is 7.5KB (50*50*3 bytes), need ~34K rows
        tensor_shape = (50, 50, 3)
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_int32(ray_start_regular_shared):
        """Test size estimation with int32 tensor columns."""
        # Target ~128MB: each tensor is 4KB (10*10*10*4 bytes), need ~64K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # int32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 1000, dtype=np.int32)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_int64(ray_start_regular_shared):
        """Test size estimation with int64 tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*8 bytes), need ~32K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 8  # int64
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i, dtype=np.int64)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_float32(ray_start_regular_shared):
        """Test size estimation with float32 tensor columns."""
        # Target ~128MB: each tensor is 4KB (10*10*10*4 bytes), need ~64K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.1, dtype=np.float32)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_1d(ray_start_regular_shared):
        """Test size estimation with 1D tensor columns."""
        # Target ~128MB: each tensor is 4KB (1000*4 bytes float32), need ~64K rows
        tensor_shape = (1000,)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.arange(i * 1000, (i + 1) * 1000, dtype=np.float32)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_2d(ray_start_regular_shared):
        """Test size estimation with 2D tensor columns."""
        # Target ~128MB: each tensor is 4KB (100*10*4 bytes float32), need ~64K rows
        tensor_shape = (100, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.01, dtype=np.float32)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_4d(ray_start_regular_shared):
        """Test size estimation with 4D tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*2*4 bytes float32), need ~32K rows
        tensor_shape = (10, 10, 10, 2)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.1, dtype=np.float32)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_multiple_columns(ray_start_regular_shared):
        """Test size estimation with multiple tensor columns."""
        # Target ~128MB: two columns, each ~128MB
        tensor_shape = (50, 50, 3)
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = (TARGET_SIZE_BYTES // 2) // bytes_per_tensor
        tensors1 = []
        tensors2 = []
        for i in range(rows):
            tensor1 = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensor2 = np.full(tensor_shape, (i + 100) % 256, dtype=np.uint8)
            tensors1.append(tensor1)
            tensors2.append(tensor2)
        tensor_array1 = TensorArray(tensors1)
        tensor_array2 = TensorArray(tensors2)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor1": tensor_array1,
                "tensor2": tensor_array2,
            }
        )
        _log_size_estimation(block)

    def test_tensors_very_large(ray_start_regular_shared):
        """Test size estimation with very large tensors."""
        # Target ~128MB: each tensor is ~256KB, need ~1K rows
        tensor_shape = (100, 100, 64)  # Large 3D tensor
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_mixed_dtypes(ray_start_regular_shared):
        """Test size estimation with mixed tensor dtypes."""
        # Target ~128MB: mix of different dtypes
        rows = 10_000
        float32_tensors = []
        uint8_tensors = []
        for i in range(rows):
            float32_tensor = np.full((50, 50), i * 0.1, dtype=np.float32)
            uint8_tensor = np.full((50, 50, 3), i % 256, dtype=np.uint8)
            float32_tensors.append(float32_tensor)
            uint8_tensors.append(uint8_tensor)
        tensor_array1 = TensorArray(float32_tensors)
        tensor_array2 = TensorArray(uint8_tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "float32_tensor": tensor_array1,
                "uint8_tensor": tensor_array2,
            }
        )
        _log_size_estimation(block)

    def test_tensors_different_sizes(ray_start_regular_shared):
        """Test size estimation with tensors of different sizes."""
        # Target ~128MB: tensors with varying sizes
        rows = 5_000
        tensors = []
        for i in range(rows):
            # Vary size based on row index
            size = 20 + (i % 30)
            tensor = np.full((size, size, 3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_variable_shapes(ray_start_regular_shared):
        """Test size estimation with ragged/variable-shaped tensors."""
        # Target ~128MB: tensors with completely different shapes
        rows = 3_000
        tensors = []
        for i in range(rows):
            # Create tensors with varying shapes
            if i % 3 == 0:
                shape = (50, 50, 3)
            elif i % 3 == 1:
                shape = (100, 100, 3)
            else:
                shape = (75, 75, 3)
            tensor = np.full(shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_1d_variable_length(ray_start_regular_shared):
        """Test size estimation with 1D tensors of variable lengths."""
        # Target ~128MB: 1D tensors with varying lengths
        rows = 10_000
        tensors = []
        for i in range(rows):
            # Vary length from 100 to 1000
            length = 100 + (i % 900)
            tensor = np.full((length,), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_2d_variable_dims(ray_start_regular_shared):
        """Test size estimation with 2D tensors of variable dimensions."""
        # Target ~128MB: 2D tensors with varying dimensions
        rows = 5_000
        tensors = []
        for i in range(rows):
            # Vary both dimensions
            h = 20 + (i % 80)
            w = 20 + (i % 80)
            tensor = np.full((h, w), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_3d_variable_dims(ray_start_regular_shared):
        """Test size estimation with 3D tensors of variable dimensions."""
        # Target ~128MB: 3D tensors with varying dimensions
        rows = 2_000
        tensors = []
        for i in range(rows):
            # Vary all three dimensions
            d1 = 10 + (i % 40)
            d2 = 10 + (i % 40)
            d3 = 3 + (i % 5)
            tensor = np.full((d1, d2, d3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_sparse_like(ray_start_regular_shared):
        """Test size estimation with sparse-like tensors (mostly zeros)."""
        # Target ~128MB: tensors that are mostly zeros (sparse-like)
        rows = 1_000
        tensors = []
        for i in range(rows):
            # Create mostly-zero tensors to simulate sparse behavior
            tensor = np.zeros((100, 100), dtype=np.float32)
            # Fill only 1% of values
            num_nonzero = 100
            # Use deterministic indices based on row index
            np.random.seed(i)
            indices = np.random.choice(10000, num_nonzero, replace=False)
            tensor.flat[indices] = i * 0.1
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_mixed_dtypes(ray_start_regular_shared):
        """Test size estimation with ragged tensors of mixed dtypes."""
        # Target ~128MB: ragged tensors with different dtypes
        rows = 2_000
        tensors = []
        for i in range(rows):
            if i % 2 == 0:
                # Float32 tensor
                tensor = np.full((50, 50), i * 0.1, dtype=np.float32)
            else:
                # Uint8 tensor
                tensor = np.full((50, 50, 3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_extreme_sizes(ray_start_regular_shared):
        """Test size estimation with ragged tensors of extreme size differences."""
        # Target ~128MB: mix of very small and very large tensors
        rows = 1_000
        tensors = []
        for i in range(rows):
            if i % 2 == 0:
                # Very small tensor
                tensor = np.full((10, 10, 3), i % 256, dtype=np.uint8)
            else:
                # Very large tensor
                tensor = np.full((200, 200, 3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_high_dimensional(ray_start_regular_shared):
        """Test size estimation with high-dimensional ragged tensors."""
        # Target ~128MB: high-dimensional tensors with varying shapes
        rows = 500
        tensors = []
        for i in range(rows):
            # 5D tensors with varying shapes
            shape = tuple(5 + (i % 10) + j for j in range(5))
            tensor = np.full(shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_pil_images(ray_start_regular_shared):
        """Test size estimation with PIL image columns."""

        # Target ~128MB: each image is 30KB (100*100*3 bytes), need ~8.5K rows
        image_shape = (100, 100, 3)
        bytes_per_image = np.prod(image_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_image
        images = []
        for i in range(rows):
            img_array = np.full(image_shape, i % 256, dtype=np.uint8)
            images.append(img_array)
        tensor_array = TensorArray(images)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "image": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_pil_images_scaled(ray_start_regular_shared):
        """Test size estimation with PIL images after scaling."""
        from PIL import Image

        # Target ~128MB: each final image is 30KB (100*100*3 bytes)
        final_image_shape = (100, 100, 3)
        bytes_per_image = np.prod(final_image_shape)
        rows = TARGET_SIZE_BYTES // bytes_per_image
        images = []
        original_size = (200, 200)
        target_size_px = (100, 100)

        for i in range(rows):
            img_array = np.full((*original_size, 3), i % 256, dtype=np.uint8)
            img = Image.fromarray(img_array)
            img_scaled = img.resize(target_size_px, Image.BILINEAR)
            img_final = np.asarray(img_scaled)
            images.append(img_final)

        tensor_array = TensorArray(images)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "image": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_large_strings_object_dtype(self, ray_start_regular_shared):
        """Test size estimation with large strings in object dtype."""
        # Target ~128MB: each string ~50KB, need ~5K rows
        string_size = 50_000  # ~50KB per string
        num_rows = TARGET_SIZE_BYTES // string_size
        large_strings = [
            f"large_string_{i}" * (string_size // 20) for i in range(num_rows)
        ]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": large_strings})
        _log_size_estimation(block)

    def test_sparse_unique_strings(self, ray_start_regular_shared):
        """Test size estimation with many unique strings."""
        # Target ~128MB: each string ~25KB, need ~10K rows
        string_size = 25_000  # ~25KB per string
        num_rows = TARGET_SIZE_BYTES // string_size
        unique_strings = [
            f"unique_string_{i}_" + "x" * (string_size // 2) for i in range(num_rows)
        ]
        block = pd.DataFrame({"id": list(range(num_rows)), "text": unique_strings})
        _log_size_estimation(block)

    def test_mixed_object_types_large(self, ray_start_regular_shared):
        """Test size estimation with mixed object types."""
        # Target ~128MB: ~85KB per row, need ~3K rows
        bytes_per_row = 85_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        data = {
            "id": list(range(num_rows)),
            "bytes_data": [b"x" * 40_000 for _ in range(num_rows)],
            "list_data": [[i] * 1000 for i in range(num_rows)],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_nested_structures_large(self, ray_start_regular_shared):
        """Test size estimation with nested data structures."""
        # Target ~128MB: ~128KB per row, need ~2K rows
        bytes_per_row = 128_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        nested_data = []
        for i in range(num_rows):
            nested_data.append(
                {
                    "key1": list(range(1000)),
                    "key2": {"nested": "value" * 500},
                }
            )
        block = pd.DataFrame({"id": list(range(num_rows)), "nested": nested_data})
        _log_size_estimation(block)

    def test_bytes_objects_large(self, ray_start_regular_shared):
        """Test size estimation with large bytes objects."""
        # Target ~128MB: ~128KB per row, need ~2K rows
        bytes_per_row = 128_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        large_bytes = [b"\x00" * bytes_per_row for _ in range(num_rows)]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": large_bytes})
        _log_size_estimation(block)

    def test_very_large_strings(self, ray_start_regular_shared):
        """Test size estimation with very large strings that sampling misses."""
        # Target ~128MB: each string ~256KB, need ~1K rows
        string_size = 256_000
        num_rows = TARGET_SIZE_BYTES // string_size
        very_large_strings = ["A" * string_size + f"_{i}" for i in range(num_rows)]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": very_large_strings})
        _log_size_estimation(block)

    def test_strings_with_unicode(self, ray_start_regular_shared):
        """Test size estimation with unicode strings that have encoding overhead."""
        # Target ~128MB: each string ~128KB, need ~2K rows
        string_size = 128_000
        num_rows = TARGET_SIZE_BYTES // string_size
        unicode_strings = [
            "🚀" * (string_size // 10) + f"test_{i}" + "🎉" * (string_size // 20)
            for i in range(num_rows)
        ]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": unicode_strings})
        _log_size_estimation(block)

    def test_deeply_nested_structures(self, ray_start_regular_shared):
        """Test size estimation with deeply nested structures."""
        # Target ~128MB: ~256KB per row, need ~1K rows
        bytes_per_row = 256_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        nested_data = []
        for i in range(num_rows):
            nested_data.append(
                {
                    "level1": {
                        "level2": {
                            "level3": {
                                "level4": {
                                    "data": list(range(1000)),
                                    "metadata": "x" * 2000,
                                }
                            }
                        }
                    }
                }
            )
        block = pd.DataFrame({"id": list(range(num_rows)), "nested": nested_data})
        _log_size_estimation(block)

    def test_mixed_large_objects(self, ray_start_regular_shared):
        """Test size estimation with mixed large object types."""
        # Target ~128MB: ~170KB per row, need ~1.5K rows
        bytes_per_row = 170_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        data = {
            "id": list(range(num_rows)),
            "large_strings": ["x" * 50_000 for _ in range(num_rows)],
            "large_bytes": [b"y" * 50_000 for _ in range(num_rows)],
            "large_lists": [[i] * 10_000 for i in range(num_rows)],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_pil_images_multiple_transforms(self, ray_start_regular_shared):
        """Test size estimation with PIL images after multiple transforms."""
        from PIL import Image

        # Target ~128MB: each final image is 30KB (100*100*3 bytes)
        final_image_shape = (100, 100, 3)
        bytes_per_image = np.prod(final_image_shape)
        rows = TARGET_SIZE_BYTES // bytes_per_image
        images = []
        original_size = (300, 300)

        for i in range(rows):
            img_array = np.full((*original_size, 3), i % 256, dtype=np.uint8)
            img = Image.fromarray(img_array)
            # Apply multiple transformations
            img_scaled1 = img.resize((200, 200), Image.BILINEAR)
            img_scaled2 = img_scaled1.resize((150, 150), Image.BILINEAR)
            img_scaled3 = img_scaled2.resize((100, 100), Image.BILINEAR)
            img_final = np.asarray(img_scaled3)
            images.append(img_final)

        tensor_array = TensorArray(images)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "image": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_tensors_variable_shapes(self, ray_start_regular_shared):
        """Test size estimation with tensors of variable shapes."""
        # Target ~128MB: average tensor ~320KB, need ~800 rows
        # Average shape is (25, 25, 3) = 1875 bytes, but with overhead ~320KB
        avg_bytes_per_tensor = 320_000
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Variable shapes might cause overhead
            shape = (20 + (i % 10), 20 + (i % 10), 3)
            tensor = np.full(shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        tensor_array = TensorArray(tensors)
        block = pd.DataFrame(
            {
                "id": list(range(rows)),
                "tensor": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_strings_with_escape_chars(self, ray_start_regular_shared):
        """Test size estimation with strings containing escape characters."""
        # Target ~128MB: each string ~128KB, need ~2K rows
        string_size = 128_000
        num_rows = TARGET_SIZE_BYTES // string_size
        escape_strings = [
            f"line1\nline2\nline3\t{i}\r\n" * (string_size // 20)
            for i in range(num_rows)
        ]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": escape_strings})
        _log_size_estimation(block)

    def test_complex_dict_structures(self, ray_start_regular_shared):
        """Test size estimation with complex dictionary structures."""
        # Target ~128MB: ~213KB per row, need ~1.2K rows
        bytes_per_row = 213_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        complex_data = []
        for i in range(num_rows):
            complex_data.append(
                {
                    "metadata": {
                        "timestamp": f"2024-01-01T{i:04d}",
                        "tags": [f"tag_{j}" for j in range(200)],
                        "attributes": {
                            f"attr_{k}": f"value_{k}" * 50 for k in range(100)
                        },
                    },
                    "payload": "x" * 100_000,
                }
            )
        block = pd.DataFrame({"id": list(range(num_rows)), "complex": complex_data})
        _log_size_estimation(block)


def test_iter_rows_with_na(ray_start_regular_shared):
    block = pd.DataFrame({"col": [pd.NA]})
    block_accessor = PandasBlockAccessor.for_block(block)

    rows = block_accessor.iter_rows(public_row_format=True)

    # We should return None for NaN values.
    assert list(rows) == [{"col": None}]


def test_empty_dataframe_with_object_columns(ray_start_regular_shared):
    """Test that size_bytes handles empty DataFrames with object/string columns.

    The warning log:
    "Error calculating size for column 'parent': cannot call `vectorize`
    on size 0 inputs unless `otypes` is set"
    should not be logged in the presence of empty columns.
    """
    from unittest.mock import patch

    # Create an empty DataFrame but with defined columns and dtypes
    block = pd.DataFrame(
        {
            "parent": pd.Series([], dtype=object),
            "child": pd.Series([], dtype="string"),
            "data": pd.Series([], dtype=object),
        }
    )

    block_accessor = PandasBlockAccessor.for_block(block)

    # Check that NO warning is logged after calling size_bytes
    with patch("ray.data._internal.pandas_block.logger.warning") as mock_warning:
        bytes_size = block_accessor.size_bytes()
        mock_warning.assert_not_called()

    assert bytes_size >= 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
