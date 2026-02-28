import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
)
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
)
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.context import DataContext
from ray.data.extensions import (
    ArrowPythonObjectType,
    ArrowTensorArray,
    ArrowTensorType,
    _object_extension_type_allowed,
)


def test_convert_to_pyarrow(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(100)
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds.write_parquet(path)
    assert ray.data.read_parquet(path).count() == 100


def test_pyarrow(ray_start_regular_shared):
    ds = ray.data.range(5)
    assert ds.map(lambda x: {"b": x["id"] + 2}).take() == [
        {"b": 2},
        {"b": 3},
        {"b": 4},
        {"b": 5},
        {"b": 6},
    ]
    assert ds.map(lambda x: {"b": x["id"] + 2}).filter(
        lambda x: x["b"] % 2 == 0
    ).take() == [{"b": 2}, {"b": 4}, {"b": 6}]
    assert ds.filter(lambda x: x["id"] == 0).flat_map(
        lambda x: [{"b": x["id"] + 2}, {"b": x["id"] + 20}]
    ).take() == [{"b": 2}, {"b": 20}]


class UnsupportedType:
    pass


def _create_dataset(op, data):
    ds = ray.data.range(2, override_num_blocks=2)

    if op == "map":

        def map(x):
            return {
                "id": x["id"],
                "my_data": data[x["id"]],
            }

        ds = ds.map(map)
    else:
        assert op == "map_batches"

        def map_batches(x):
            row_id = x["id"][0]
            return {
                "id": x["id"],
                "my_data": [data[row_id]],
            }

        ds = ds.map_batches(map_batches, batch_size=None)

    # Needed for the map_batches case to trigger the error,
    # because the error happens when merging the blocks.
    ds = ds.map_batches(lambda x: x, batch_size=2)
    return ds


@pytest.mark.skipif(
    _object_extension_type_allowed(), reason="Arrow table supports pickled objects"
)
@pytest.mark.parametrize(
    "op, data",
    [
        ("map", [UnsupportedType(), 1]),
        ("map_batches", [None, 1]),
        ("map_batches", [{"a": 1}, {"a": 2}]),
    ],
)
def test_fallback_to_pandas_on_incompatible_data(
    op,
    data,
    ray_start_regular_shared,
):
    # Test if the first UDF output is incompatible with Arrow,
    # Ray Data will fall back to using Pandas.
    ds = _create_dataset(op, data)
    ds = ds.materialize()
    bundles = ds.iter_internal_ref_bundles()
    block = ray.get(next(bundles).block_refs[0])
    assert isinstance(block, pd.DataFrame)


_PYARROW_SUPPORTS_TYPE_PROMOTION = (
    get_pyarrow_version() >= MIN_PYARROW_VERSION_TYPE_PROMOTION
)


@pytest.mark.parametrize(
    "op, data, should_fail, expected_type",
    [
        # Case A: Upon serializing to Arrow fallback to `ArrowPythonObjectType`
        ("map_batches", [1, 2**100], False, ArrowPythonObjectType()),
        ("map_batches", [1.0, 2**100], False, ArrowPythonObjectType()),
        ("map_batches", ["1.0", 2**100], False, ArrowPythonObjectType()),
        # Case B: No fallback to `ArrowPythonObjectType`, but type promotion allows
        #         int to be promoted to a double
        (
            "map_batches",
            [1.0, 2**4],
            not _PYARROW_SUPPORTS_TYPE_PROMOTION,
            pa.float64(),
        ),
        # Case C: No fallback to `ArrowPythonObjectType` and no type promotion possible
        ("map_batches", ["1.0", 2**4], True, None),
    ],
)
def test_pyarrow_conversion_error_handling(
    ray_start_regular_shared,
    op,
    data,
    should_fail: bool,
    expected_type: pa.DataType,
):
    # Ray Data infers the block type (arrow or pandas) and the block schema
    # based on the first *block* produced by UDF.
    #
    # These tests simulate following scenarios
    #   1. (Case A) Type of the value of the first block is deduced as Arrow scalar
    #      type, but second block carries value that overflows pa.int64 representation,
    #      and column henceforth will be serialized as `ArrowPythonObjectExtensionType`
    #      coercing first block to it as well
    #   2. (Case B) Both blocks carry proper Arrow scalars which, however, have
    #      diverging types and therefore Arrow fails during merging of these blocks
    #      into 1
    ds = _create_dataset(op, data)

    if should_fail:
        with pytest.raises(Exception) as e:
            ds.materialize()

        error_msg = str(e.value)
        expected_msg = "ArrowConversionError: Error converting data to Arrow:"

        assert expected_msg in error_msg
        assert "my_data" in error_msg

    else:
        ds.materialize()

        assert ds.schema().base_schema == pa.schema(
            [pa.field("id", pa.int64()), pa.field("my_data", expected_type)]
        )

        results = sorted(ds.take_all(), key=lambda r: r["id"])
        assert results == [{"id": i, "my_data": data[i]} for i in range(len(data))]


@pytest.mark.parametrize("use_arrow_tensor_v2", [True, False])
@pytest.mark.skipif(
    get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION,
    reason="Requires Arrow version of at least 14.0.0",
)
def test_concat_with_mixed_tensor_types_and_native_pyarrow_types(
    use_arrow_tensor_v2, restore_data_context
):
    DataContext.get_current().use_arrow_tensor_v2 = use_arrow_tensor_v2

    num_rows = 1024

    # Block A: int is uint64; tensor = Ray tensor extension
    t_uint = pa.table(
        {
            "int": pa.array(np.zeros(num_rows // 2, dtype=np.uint64), type=pa.uint64()),
            "tensor": ArrowTensorArray.from_numpy(
                np.zeros((num_rows // 2, 3, 3), dtype=np.float32)
            ),
        }
    )

    # Block B: int is float64 with NaNs; tensor = same extension type
    f = np.ones(num_rows // 2, dtype=np.float64)
    f[::8] = np.nan
    t_float = pa.table(
        {
            "int": pa.array(f, type=pa.float64()),
            "tensor": ArrowTensorArray.from_numpy(
                np.zeros((num_rows // 2, 3, 3), dtype=np.float32)
            ),
        }
    )

    # Two input blocks with different Arrow dtypes for "int"
    ds = ray.data.from_arrow([t_uint, t_float])

    # Force a concat across blocks
    ds = ds.repartition(1)

    # This should not raise: RuntimeError: Types mismatch: double != uint64
    ds.materialize()

    # Ensure that the result is correct
    # Determine expected tensor type based on current DataContext setting
    if use_arrow_tensor_v2:
        expected_tensor_type = ArrowTensorTypeV2((3, 3), pa.float32())
    else:
        expected_tensor_type = ArrowTensorType((3, 3), pa.float32())

    assert ds.schema().base_schema == pa.schema(
        [("int", pa.float64()), ("tensor", expected_tensor_type)]
    )
    assert ds.count() == num_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
