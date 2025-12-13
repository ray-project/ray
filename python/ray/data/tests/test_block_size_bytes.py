"""Test suite for block size estimation.

This test suite verifies that size_bytes() methods in ArrowBlockAccessor and
PandasBlockAccessor accurately estimate the serialized size of blocks in Ray's
object store, using ray.experimental.get_local_object_locations to get the
actual object store size.
"""

import inspect
import sys
from typing import Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.pandas_block import PandasBlockAccessor

# Tolerance for size comparison (1%)
SIZE_TOLERANCE = 0.01
# Target block size: 16MB
TARGET_SIZE_BYTES = 16 * 1024 * 1024


def _create_tensor_block_pandas(tensors, id_column=None):
    """Create a Pandas DataFrame with tensor column."""
    tensor_array = TensorArray(tensors)
    data = {"tensor": tensor_array}
    if id_column is not None:
        data["id"] = id_column
    return pd.DataFrame(data)


def _create_tensor_block_arrow(tensors, id_column=None):
    """Create an Arrow Table with tensor column."""
    tensor_array = ArrowTensorArray.from_numpy(tensors)
    data = {"tensor": tensor_array}
    if id_column is not None:
        data["id"] = id_column
    return pa.table(data)


def _create_multi_tensor_block_pandas(tensors_list, id_column=None):
    """Create a Pandas DataFrame with multiple tensor columns."""
    data = {}
    for idx, tensors in enumerate(tensors_list):
        tensor_array = TensorArray(tensors)
        data[f"tensor{idx + 1}"] = tensor_array
    if id_column is not None:
        data["id"] = id_column
    return pd.DataFrame(data)


def _create_multi_tensor_block_arrow(tensors_list, id_column=None):
    """Create an Arrow Table with multiple tensor columns."""
    data = {}
    for idx, tensors in enumerate(tensors_list):
        tensor_array = ArrowTensorArray.from_numpy(tensors)
        data[f"tensor{idx + 1}"] = tensor_array
    if id_column is not None:
        data["id"] = id_column
    return pa.table(data)


def get_actual_object_store_size(block_ref: ray.ObjectRef) -> int:
    """Get actual object store size using get_local_object_locations."""
    locations = ray.experimental.get_local_object_locations([block_ref])
    if block_ref not in locations:
        raise ValueError(f"Block reference {block_ref} not found in object store")
    object_size = locations[block_ref].get("object_size")
    if object_size is None:
        raise ValueError(f"Object size not available for {block_ref}")
    return object_size


def _log_size_estimation(
    block: Union[pd.DataFrame, pa.Table], tolerance: Optional[float] = None
) -> None:
    """Helper function to log size estimation comparison for blocks.

    Works with both Pandas DataFrames and Arrow Tables.

    Note: Pandas DataFrames are serialized as Arrow tables in Ray's object
    store. Arrow tables are serialized using Arrow IPC format. The size_bytes()
    method should estimate the serialized size, not the in-memory size.

    Args:
        block: Pandas DataFrame or Arrow Table to test
        tolerance: Optional tolerance override (default: SIZE_TOLERANCE)
    """
    if tolerance is None:
        tolerance = SIZE_TOLERANCE

    # Get caller information
    caller_frame = inspect.stack()[1]
    caller_name = caller_frame.function

    # Determine block type and get appropriate accessor
    if isinstance(block, pd.DataFrame):
        block_accessor = PandasBlockAccessor.for_block(block)
        block_type = "Pandas"
    elif isinstance(block, pa.Table):
        block_accessor = ArrowBlockAccessor.for_block(block)
        block_type = "Arrow"
    else:
        raise TypeError(f"Unsupported block type: {type(block)}")

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
            f"[{caller_name}] {block_type} Size estimation {direction}: "
            f"estimated={estimated_size:,} bytes, "
            f"actual={actual_size:,} bytes, "
            f"difference={abs(size_diff):,} bytes "
            f"({size_diff_percent:.2f}% error)"
        )

    if size_diff_percent > tolerance * 100:
        pytest.fail(
            f"[{caller_name}] {block_type} Size difference "
            f"{size_diff_percent:.2f}% exceeds tolerance "
            f"{tolerance * 100}%"
        )


class TestPandasBlockSizeBytes:
    """Test suite for Pandas block size estimation."""

    def test_strings(self, ray_start_regular_shared):
        """Test size estimation with string columns (string and object dtypes)."""
        # Target ~128MB: string columns with different dtypes
        string_size = 50  # Average string size
        num_rows = TARGET_SIZE_BYTES // string_size
        animals = [
            "alligator"
            if i % 4 == 0
            else "crocodile"
            if i % 4 == 1
            else "centipede"
            if i % 4 == 2
            else "flamingo"
            for i in range(num_rows)
        ]
        # Test with string dtype
        block_str = pd.DataFrame({"animals": animals})
        block_str["animals"] = block_str["animals"].astype("string")
        _log_size_estimation(block_str)
        # Test with object dtype
        block_obj = pd.DataFrame({"animals": animals})
        _log_size_estimation(block_obj)

    def test_large_floats(self, ray_start_regular_shared):
        """Test size estimation with large float column."""
        # Target ~128MB: float64 column
        bytes_per_row = 8  # float64
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        data = [float(i) for i in range(num_rows)]
        block = pd.DataFrame({"floats": data})
        _log_size_estimation(block)

    def test_bytes_object(self, ray_start_regular_shared):
        """Test size estimation with bytes objects."""
        # Target ~128MB: bytes objects
        bytes_per_item = 64_000  # 64KB per item
        num_items = TARGET_SIZE_BYTES // bytes_per_item
        data = {"data": [b"\x00" * bytes_per_item for _ in range(num_items)]}
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_nested_numpy(self, ray_start_regular_shared):
        """Test size estimation with nested numpy arrays."""
        # Target ~128MB: nested numpy arrays
        array_size = 1024
        bytes_per_array = array_size  # int8
        num_rows = TARGET_SIZE_BYTES // bytes_per_array
        data = [np.full(array_size, i % 100, dtype=np.int8) for i in range(num_rows)]
        df = pd.DataFrame({"data": data})
        _log_size_estimation(df)

    def test_nested_objects(self, ray_start_regular_shared):
        """Test size estimation with nested object lists."""
        # Target ~128MB: nested lists
        list_size = 1000
        bytes_per_list = list_size * 4 + 100  # int + list overhead
        num_rows = TARGET_SIZE_BYTES // bytes_per_list
        lists = [[i % 100 for _ in range(list_size)] for i in range(num_rows)]
        data = {"lists": lists}
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_mixed_types(self, ray_start_regular_shared):
        """Test size estimation with mixed data types."""
        # Target ~128MB: mixed types
        bytes_per_row_estimate = 8 + 8 + 50 + 128  # int, float, string, bytes
        num_rows = TARGET_SIZE_BYTES // bytes_per_row_estimate
        data = {
            "integers": list(range(num_rows)),
            "floats": [float(i) for i in range(num_rows)],
            "strings": [
                "apple" if i % 3 == 0 else "banana" if i % 3 == 1 else "cherry"
                for i in range(num_rows)
            ],
            "object": [b"x" * 128 for _ in range(num_rows)],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_nested_lists_strings(self, ray_start_regular_shared):
        """Test size estimation with nested string lists."""
        # Target ~128MB: nested string lists
        nested_list_item_size = 20  # Average string size in list
        bytes_per_list = (
            nested_list_item_size * 10
        ) + 100  # 10 strings + list overhead
        num_rows = TARGET_SIZE_BYTES // bytes_per_list
        nested_lists = ["a" * 3, "bb" * 4, "ccc" * 3] * (num_rows // 3 + 1)
        nested_lists = nested_lists[:num_rows]
        data = {
            "nested_lists": [nested_lists for _ in range(num_rows)],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_multi_level_nesting(self, ray_start_regular_shared):
        """Test size estimation with multi-level nested structures."""
        # Target ~128MB: complex nested structures
        size = 1024  # Array size
        bytes_per_row = size * 8 + 100  # float64 array + dict overhead
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        data = {
            "complex": [
                {
                    "list": [float(i + j) for j in range(size)],
                    "value": {"key": "val"},
                }
                for i in range(num_rows)
            ],
        }
        block = pd.DataFrame(data)
        _log_size_estimation(block)

    def test_boolean(self, ray_start_regular_shared):
        """Test size estimation with boolean column."""
        # Target ~128MB: boolean column
        # Based on measurements: 134M rows -> 268MB actual, so ~2 bytes per boolean actual.
        # To get ~128MB, use ~64M rows.
        num_rows = TARGET_SIZE_BYTES // 2
        data = [True if i % 2 == 0 else False for i in range(num_rows)]
        block = pd.DataFrame({"flags": pd.Series(data, dtype="boolean")})
        _log_size_estimation(block)

    def test_arrow(self, ray_start_regular_shared):
        """Test size estimation with Arrow dtype strings."""
        # Target ~128MB: Arrow string dtype
        string_size = 50  # Average string size
        num_rows = TARGET_SIZE_BYTES // string_size
        data = [
            "alligator" if i % 3 == 0 else "crocodile" if i % 3 == 1 else "flamingo"
            for i in range(num_rows)
        ]
        arrow_dtype = pd.ArrowDtype(pa.string())
        block = pd.DataFrame({"animals": pd.Series(data, dtype=arrow_dtype)})
        _log_size_estimation(block)

    def test_tensors(self, ray_start_regular_shared):
        """Test size estimation with tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*8 bytes), need ~16K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 8  # float64
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.arange(i * 1000, (i + 1) * 1000, dtype=np.float64).reshape(
                tensor_shape
            )
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_large(self, ray_start_regular_shared):
        """Test size estimation with larger tensor columns."""
        # Target ~128MB: each tensor is 7.5KB (50*50*3 bytes), need ~17K rows
        tensor_shape = (50, 50, 3)
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_int32(self, ray_start_regular_shared):
        """Test size estimation with int32 tensor columns."""
        # Target ~128MB: each tensor is 4KB (10*10*10*4 bytes), need ~32K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # int32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 1000, dtype=np.int32)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_int64(self, ray_start_regular_shared):
        """Test size estimation with int64 tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*8 bytes), need ~16K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 8  # int64
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i, dtype=np.int64)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_float32(self, ray_start_regular_shared):
        """Test size estimation with float32 tensor columns."""
        # Target ~128MB: each tensor is 4KB (10*10*10*4 bytes), need ~32K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.1, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_1d(self, ray_start_regular_shared):
        """Test size estimation with 1D tensor columns."""
        # Target ~128MB: each tensor is 4KB (1000*4 bytes float32), need ~32K rows
        tensor_shape = (1000,)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.arange(i * 1000, (i + 1) * 1000, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_2d(self, ray_start_regular_shared):
        """Test size estimation with 2D tensor columns."""
        # Target ~128MB: each tensor is 4KB (100*10*4 bytes float32), need ~32K rows
        tensor_shape = (100, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.01, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_4d(self, ray_start_regular_shared):
        """Test size estimation with 4D tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*2*4 bytes float32), need ~16K rows
        tensor_shape = (10, 10, 10, 2)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.1, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_multiple_columns(self, ray_start_regular_shared):
        """Test size estimation with multiple tensor columns."""
        # Target ~128MB: two columns, each ~64MB
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
        block = _create_multi_tensor_block_pandas(
            [tensors1, tensors2], list(range(rows))
        )
        _log_size_estimation(block)

    def test_tensors_very_large(self, ray_start_regular_shared):
        """Test size estimation with very large tensors."""
        # Target ~128MB: each tensor is ~256KB, need ~500 rows
        tensor_shape = (100, 100, 64)  # Large 3D tensor
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_mixed_dtypes(self, ray_start_regular_shared):
        """Test size estimation with mixed tensor dtypes."""
        # Target ~128MB: mix of different dtypes
        # Average tensor size: (50*50 float32) + (50*50*3 uint8) =
        # 10KB + 7.5KB = 17.5KB. Need ~7.3K rows
        avg_bytes_per_tensor = 17_500
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        float32_tensors = []
        uint8_tensors = []
        for i in range(rows):
            float32_tensor = np.full((50, 50), i * 0.1, dtype=np.float32)
            uint8_tensor = np.full((50, 50, 3), i % 256, dtype=np.uint8)
            float32_tensors.append(float32_tensor)
            uint8_tensors.append(uint8_tensor)
        block = _create_multi_tensor_block_pandas(
            [float32_tensors, uint8_tensors], list(range(rows))
        )
        # Rename columns to match expected names
        block = block.rename(
            columns={"tensor1": "float32_tensor", "tensor2": "uint8_tensor"}
        )
        _log_size_estimation(block)

    def test_tensors_different_sizes(self, ray_start_regular_shared):
        """Test size estimation with tensors of different sizes."""
        # Target ~128MB: tensors with varying sizes
        # Average tensor size: (35*35*3) = ~3.6KB. Need ~35K rows
        avg_bytes_per_tensor = 3_600
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary size based on row index
            size = 20 + (i % 30)
            tensor = np.full((size, size, 3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_variable_shapes(self, ray_start_regular_shared):
        """Test size estimation with ragged/variable-shaped tensors."""
        # Target ~128MB: tensors with completely different shapes
        # Average tensor size: (75*75*3) = ~17KB. Need ~7.5K rows
        avg_bytes_per_tensor = 17_000
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
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
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_1d_variable_length(self, ray_start_regular_shared):
        """Test size estimation with 1D tensors of variable lengths."""
        # Target ~128MB: 1D tensors with varying lengths
        # Average tensor size: (550 uint8) = ~0.55KB. Need ~230K rows
        # Variable-shaped tensors have higher serialization overhead, so use
        # a higher tolerance (5%)
        avg_bytes_per_tensor = 550
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary length from 100 to 1000
            length = 100 + (i % 900)
            tensor = np.full((length,), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block, tolerance=0.05)

    def test_tensors_ragged_2d_variable_dims(self, ray_start_regular_shared):
        """Test size estimation with 2D tensors of variable dimensions."""
        # Target ~128MB: 2D tensors with varying dimensions
        # Average tensor size: (60*60 uint8) = ~3.6KB. Need ~35K rows
        # Variable-shaped tensors have higher serialization overhead, so use
        # a higher tolerance (2%)
        avg_bytes_per_tensor = 3_600
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary both dimensions
            h = 20 + (i % 80)
            w = 20 + (i % 80)
            tensor = np.full((h, w), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block, tolerance=0.02)

    def test_tensors_ragged_3d_variable_dims(self, ray_start_regular_shared):
        """Test size estimation with 3D tensors of variable dimensions."""
        # Target ~128MB: 3D tensors with varying dimensions
        # Average tensor size: (30*30*4 uint8) = ~3.6KB. Need ~35K rows
        avg_bytes_per_tensor = 3_600
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary all three dimensions
            d1 = 10 + (i % 40)
            d2 = 10 + (i % 40)
            d3 = 3 + (i % 5)
            tensor = np.full((d1, d2, d3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_sparse_like(self, ray_start_regular_shared):
        """Test size estimation with sparse-like tensors (mostly zeros)."""
        # Target ~128MB: tensors that are mostly zeros (sparse-like)
        # Each tensor is 100*100 float32 = 40KB. Need ~3.2K rows
        bytes_per_tensor = 40_000
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
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
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_mixed_dtypes(self, ray_start_regular_shared):
        """Test size estimation with ragged tensors of mixed dtypes."""
        # Target ~128MB: ragged tensors with different dtypes
        # Average tensor size: (50*50 float32) + (50*50*3 uint8) = 10KB + 7.5KB = 17.5KB. Need ~7.3K rows
        avg_bytes_per_tensor = 17_500
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            if i % 2 == 0:
                # Float32 tensor
                tensor = np.full((50, 50), i * 0.1, dtype=np.float32)
            else:
                # Uint8 tensor
                tensor = np.full((50, 50, 3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_extreme_sizes(self, ray_start_regular_shared):
        """Test size estimation with ragged tensors of extreme size differences."""
        # Target ~128MB: mix of very small and very large tensors
        # Average tensor size: (10% large (200*200*3) + 90% small (10*10*3))
        # = ~12KB + ~0.3KB = ~12.3KB. Need ~10K rows
        avg_bytes_per_tensor = 12_300
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            if i % 10 == 0:  # 10% large tensors
                tensor = np.full((200, 200, 3), i % 256, dtype=np.uint8)
            else:  # 90% small tensors
                tensor = np.full((10, 10, 3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_high_dimensional(self, ray_start_regular_shared):
        """Test size estimation with high-dimensional ragged tensors."""
        # Target ~128MB: high-dimensional tensors with varying shapes
        # Average tensor size: (7*7*5*5*3) = ~3.6KB. Need ~35K rows
        avg_bytes_per_tensor = 3_600
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # 5D tensors with varying shapes
            shape = tuple(5 + (i % 10) + j for j in range(5))
            tensor = np.full(shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_pil_images(self, ray_start_regular_shared):
        """Test size estimation with PIL image columns."""
        # Target ~128MB: each image is 30KB (100*100*3 bytes), need ~4.2K rows
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

    def test_pil_images_scaled(self, ray_start_regular_shared):
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

    def test_large_strings_object_dtype(self, ray_start_regular_shared):
        """Test size estimation with large strings in object dtype."""
        # Target ~128MB: each string ~50KB, need ~2.5K rows
        string_size = 50_000  # ~50KB per string
        num_rows = TARGET_SIZE_BYTES // string_size
        large_strings = [
            f"large_string_{i}" * (string_size // 20) for i in range(num_rows)
        ]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": large_strings})
        _log_size_estimation(block)

    def test_sparse_unique_strings(self, ray_start_regular_shared):
        """Test size estimation with many unique strings."""
        # Target ~128MB: each string ~25KB, need ~5K rows
        string_size = 25_000  # ~25KB per string
        num_rows = TARGET_SIZE_BYTES // string_size
        unique_strings = [
            f"unique_string_{i}_" + "x" * (string_size // 2) for i in range(num_rows)
        ]
        block = pd.DataFrame({"id": list(range(num_rows)), "text": unique_strings})
        _log_size_estimation(block)

    def test_mixed_object_types_large(self, ray_start_regular_shared):
        """Test size estimation with mixed object types."""
        # Target ~128MB: ~85KB per row, need ~1.5K rows
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
        # Target ~128MB: ~128KB per row, need ~1K rows
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
        # Target ~128MB: ~128KB per row, need ~1K rows
        bytes_per_row = 128_000
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        large_bytes = [b"\x00" * bytes_per_row for _ in range(num_rows)]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": large_bytes})
        _log_size_estimation(block)

    def test_very_large_strings(self, ray_start_regular_shared):
        """Test size estimation with very large strings."""
        # Target ~128MB: each string ~256KB, need ~500 rows
        string_size = 256_000
        num_rows = TARGET_SIZE_BYTES // string_size
        very_large_strings = ["A" * string_size + f"_{i}" for i in range(num_rows)]
        block = pd.DataFrame({"id": list(range(num_rows)), "data": very_large_strings})
        _log_size_estimation(block)

    def test_strings_with_unicode(self, ray_start_regular_shared):
        """Test size estimation with unicode strings."""
        # Target ~128MB: each string ~128KB, need ~1K rows
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
        # Target ~128MB: ~256KB per row, need ~500 rows
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
        # Target ~128MB: ~170KB per row, need ~750 rows
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

    def test_tensors_variable_shapes(self, ray_start_regular_shared):
        """Test size estimation with tensors of variable shapes."""
        # Target ~128MB: average tensor ~320KB, need ~400 rows
        # Average shape is (25, 25, 3) = 1875 bytes, but with overhead ~320KB
        # Variable-shaped tensors have higher serialization overhead, so use
        # a higher tolerance (2%)
        avg_bytes_per_tensor = 320_000
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Variable shapes might cause overhead
            shape = (20 + (i % 10), 20 + (i % 10), 3)
            tensor = np.full(shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_pandas(tensors, list(range(rows)))
        _log_size_estimation(block, tolerance=0.02)

    def test_strings_with_escape_chars(self, ray_start_regular_shared):
        """Test size estimation with strings containing escape characters."""
        # Target ~128MB: each string ~128KB, need ~1K rows
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
        # Target ~128MB: ~213KB per row, need ~600 rows
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


class TestArrowBlockSizeBytes:
    """Test suite for Arrow block size estimation."""

    def test_integers(self, ray_start_regular_shared):
        """Test size estimation with integer columns."""
        # Target ~128MB: int64 column
        bytes_per_row = 8  # int64
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        block = pa.table(
            {
                "id": list(range(num_rows)),
                "value": list(range(num_rows, num_rows * 2)),
            }
        )
        _log_size_estimation(block)

    def test_large_floats(self, ray_start_regular_shared):
        """Test size estimation with large float column."""
        # Target ~128MB: float64 column
        bytes_per_row = 8  # float64
        num_rows = TARGET_SIZE_BYTES // bytes_per_row
        block = pa.table({"floats": [float(i) for i in range(num_rows)]})
        _log_size_estimation(block)

    def test_strings(self, ray_start_regular_shared):
        """Test size estimation with string columns."""
        # Target ~128MB: string column
        string_size = 50  # Average string size
        num_rows = TARGET_SIZE_BYTES // string_size
        animals = [
            "alligator"
            if i % 4 == 0
            else "crocodile"
            if i % 4 == 1
            else "centipede"
            if i % 4 == 2
            else "flamingo"
            for i in range(num_rows)
        ]
        block = pa.table({"animals": animals})
        _log_size_estimation(block)

    def test_boolean(self, ray_start_regular_shared):
        """Test size estimation with boolean column."""
        # Target ~128MB: boolean column
        # Based on measurements: boolean columns use 1 byte per value in Arrow
        num_rows = TARGET_SIZE_BYTES
        data = [True if i % 2 == 0 else False for i in range(num_rows)]
        block = pa.table({"flags": data})
        _log_size_estimation(block)

    def test_mixed_types(self, ray_start_regular_shared):
        """Test size estimation with mixed data types."""
        # Target ~128MB: mixed types
        bytes_per_row_estimate = 8 + 8 + 50  # int, float, string
        num_rows = TARGET_SIZE_BYTES // bytes_per_row_estimate
        block = pa.table(
            {
                "integers": list(range(num_rows)),
                "floats": [float(i) for i in range(num_rows)],
                "strings": [
                    "apple" if i % 3 == 0 else "banana" if i % 3 == 1 else "cherry"
                    for i in range(num_rows)
                ],
            }
        )
        _log_size_estimation(block)

    def test_tensors(self, ray_start_regular_shared):
        """Test size estimation with tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*8 bytes), need ~16K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 8  # float64
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.arange(i * 1000, (i + 1) * 1000, dtype=np.float64).reshape(
                tensor_shape
            )
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_large(self, ray_start_regular_shared):
        """Test size estimation with larger tensor columns."""
        # Target ~128MB: each tensor is 7.5KB (50*50*3 bytes), need ~17K rows
        tensor_shape = (50, 50, 3)
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_int32(self, ray_start_regular_shared):
        """Test size estimation with int32 tensor columns."""
        # Target ~128MB: each tensor is 4KB (10*10*10*4 bytes), need ~32K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # int32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 1000, dtype=np.int32)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_int64(self, ray_start_regular_shared):
        """Test size estimation with int64 tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*8 bytes), need ~16K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 8  # int64
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i, dtype=np.int64)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_float32(self, ray_start_regular_shared):
        """Test size estimation with float32 tensor columns."""
        # Target ~128MB: each tensor is 4KB (10*10*10*4 bytes), need ~32K rows
        tensor_shape = (10, 10, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.1, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_1d(self, ray_start_regular_shared):
        """Test size estimation with 1D tensor columns."""
        # Target ~128MB: each tensor is 4KB (1000*4 bytes float32), need ~32K rows
        tensor_shape = (1000,)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.arange(i * 1000, (i + 1) * 1000, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_2d(self, ray_start_regular_shared):
        """Test size estimation with 2D tensor columns."""
        # Target ~128MB: each tensor is 4KB (100*10*4 bytes float32), need ~32K rows
        tensor_shape = (100, 10)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.01, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_4d(self, ray_start_regular_shared):
        """Test size estimation with 4D tensor columns."""
        # Target ~128MB: each tensor is 8KB (10*10*10*2*4 bytes float32), need ~16K rows
        tensor_shape = (10, 10, 10, 2)
        bytes_per_tensor = np.prod(tensor_shape) * 4  # float32
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i * 0.1, dtype=np.float32)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_multiple_columns(self, ray_start_regular_shared):
        """Test size estimation with multiple tensor columns."""
        # Target ~128MB: two columns, each ~64MB
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
        block = _create_multi_tensor_block_arrow(
            [tensors1, tensors2], list(range(rows))
        )
        _log_size_estimation(block)

    def test_tensors_very_large(self, ray_start_regular_shared):
        """Test size estimation with very large tensors."""
        # Target ~128MB: each tensor is ~256KB, need ~500 rows
        tensor_shape = (100, 100, 64)  # Large 3D tensor
        bytes_per_tensor = np.prod(tensor_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_tensor
        tensors = []
        for i in range(rows):
            tensor = np.full(tensor_shape, i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_mixed_dtypes(self, ray_start_regular_shared):
        """Test size estimation with mixed tensor dtypes."""
        # Target ~128MB: mix of different dtypes
        # Average tensor size: (50*50 float32) + (50*50*3 uint8) =
        # 10KB + 7.5KB = 17.5KB. Need ~7.3K rows
        avg_bytes_per_tensor = 17_500
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        float32_tensors = []
        uint8_tensors = []
        for i in range(rows):
            float32_tensor = np.full((50, 50), i * 0.1, dtype=np.float32)
            uint8_tensor = np.full((50, 50, 3), i % 256, dtype=np.uint8)
            float32_tensors.append(float32_tensor)
            uint8_tensors.append(uint8_tensor)
        tensor_array1 = ArrowTensorArray.from_numpy(float32_tensors)
        tensor_array2 = ArrowTensorArray.from_numpy(uint8_tensors)
        block = pa.table(
            {
                "id": list(range(rows)),
                "float32_tensor": tensor_array1,
                "uint8_tensor": tensor_array2,
            }
        )
        _log_size_estimation(block)

    def test_tensors_ragged_variable_shapes(self, ray_start_regular_shared):
        """Test size estimation with ragged/variable-shaped tensors."""
        # Target ~128MB: tensors with completely different shapes
        # Average tensor size: (75*75*3) = ~17KB. Need ~7.5K rows
        avg_bytes_per_tensor = 17_000
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
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
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_1d_variable_length(self, ray_start_regular_shared):
        """Test size estimation with 1D tensors of variable lengths."""
        # Target ~128MB: 1D tensors with varying lengths
        # Average tensor size: (550 uint8) = ~0.55KB. Need ~230K rows
        avg_bytes_per_tensor = 550
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary length from 100 to 1000
            length = 100 + (i % 900)
            tensor = np.full((length,), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_2d_variable_dims(self, ray_start_regular_shared):
        """Test size estimation with 2D tensors of variable dimensions."""
        # Target ~128MB: 2D tensors with varying dimensions
        # Average tensor size: (60*60 uint8) = ~3.6KB. Need ~35K rows
        avg_bytes_per_tensor = 3_600
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary both dimensions
            h = 20 + (i % 80)
            w = 20 + (i % 80)
            tensor = np.full((h, w), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_tensors_ragged_3d_variable_dims(self, ray_start_regular_shared):
        """Test size estimation with 3D tensors of variable dimensions."""
        # Target ~128MB: 3D tensors with varying dimensions
        # Average tensor size: (30*30*4 uint8) = ~3.6KB. Need ~35K rows
        avg_bytes_per_tensor = 3_600
        rows = TARGET_SIZE_BYTES // avg_bytes_per_tensor
        tensors = []
        for i in range(rows):
            # Vary all three dimensions
            d1 = 10 + (i % 40)
            d2 = 10 + (i % 40)
            d3 = 3 + (i % 5)
            tensor = np.full((d1, d2, d3), i % 256, dtype=np.uint8)
            tensors.append(tensor)
        block = _create_tensor_block_arrow(tensors, list(range(rows)))
        _log_size_estimation(block)

    def test_pil_images(self, ray_start_regular_shared):
        """Test size estimation with PIL image columns."""
        # Target ~128MB: each image is 30KB (100*100*3 bytes), need ~4.2K rows
        image_shape = (100, 100, 3)
        bytes_per_image = np.prod(image_shape)  # uint8
        rows = TARGET_SIZE_BYTES // bytes_per_image
        images = []
        for i in range(rows):
            img_array = np.full(image_shape, i % 256, dtype=np.uint8)
            images.append(img_array)
        tensor_array = ArrowTensorArray.from_numpy(images)
        block = pa.table(
            {
                "id": list(range(rows)),
                "image": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_pil_images_scaled(self, ray_start_regular_shared):
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

        tensor_array = ArrowTensorArray.from_numpy(images)
        block = pa.table(
            {
                "id": list(range(rows)),
                "image": tensor_array,
            }
        )
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

        tensor_array = ArrowTensorArray.from_numpy(images)
        block = pa.table(
            {
                "id": list(range(rows)),
                "image": tensor_array,
            }
        )
        _log_size_estimation(block)

    def test_large_strings(self, ray_start_regular_shared):
        """Test size estimation with large strings."""
        # Target ~128MB: each string ~50KB, need ~2.5K rows
        string_size = 50_000  # ~50KB per string
        num_rows = TARGET_SIZE_BYTES // string_size
        large_strings = [
            f"large_string_{i}" * (string_size // 20) for i in range(num_rows)
        ]
        block = pa.table({"id": list(range(num_rows)), "data": large_strings})
        _log_size_estimation(block)

    def test_sparse_unique_strings(self, ray_start_regular_shared):
        """Test size estimation with many unique strings."""
        # Target ~128MB: each string ~25KB, need ~5K rows
        string_size = 25_000  # ~25KB per string
        num_rows = TARGET_SIZE_BYTES // string_size
        unique_strings = [
            f"unique_string_{i}_" + "x" * (string_size // 2) for i in range(num_rows)
        ]
        block = pa.table({"id": list(range(num_rows)), "text": unique_strings})
        _log_size_estimation(block)

    def test_very_large_strings(self, ray_start_regular_shared):
        """Test size estimation with very large strings."""
        # Target ~128MB: each string ~256KB, need ~500 rows
        string_size = 256_000
        num_rows = TARGET_SIZE_BYTES // string_size
        very_large_strings = ["A" * string_size + f"_{i}" for i in range(num_rows)]
        block = pa.table({"id": list(range(num_rows)), "data": very_large_strings})
        _log_size_estimation(block)

    def test_strings_with_unicode(self, ray_start_regular_shared):
        """Test size estimation with unicode strings."""
        # Target ~128MB: each string ~128KB, need ~1K rows
        string_size = 128_000
        num_rows = TARGET_SIZE_BYTES // string_size
        unicode_strings = [
            "🚀" * (string_size // 10) + f"test_{i}" + "🎉" * (string_size // 20)
            for i in range(num_rows)
        ]
        block = pa.table({"id": list(range(num_rows)), "data": unicode_strings})
        _log_size_estimation(block)

    def test_strings_with_escape_chars(self, ray_start_regular_shared):
        """Test size estimation with strings containing escape characters."""
        # Target ~128MB: each string ~128KB, need ~1K rows
        string_size = 128_000
        num_rows = TARGET_SIZE_BYTES // string_size
        escape_strings = [
            f"line1\nline2\nline3\t{i}\r\n" * (string_size // 20)
            for i in range(num_rows)
        ]
        block = pa.table({"id": list(range(num_rows)), "data": escape_strings})
        _log_size_estimation(block)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
