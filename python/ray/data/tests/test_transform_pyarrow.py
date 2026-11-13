import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    FixedShapeTensorFormat,
)
from ray.data.context import DataContext
from ray.data.extensions import (
    ArrowConversionError,
    ArrowPythonObjectType,
    ArrowTensorArray,
    ArrowTensorType,
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


def test_map_batches_fallback_to_pandas_on_incompatible_data(
    ray_start_regular_shared,
    restore_data_context,
):
    # For map_batches, if the first UDF output is incompatible with Arrow,
    # Ray Data will fall back to using Pandas.
    class UnsupportedType:
        pass

    data = [UnsupportedType(), 1]
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False
    ds = _create_dataset("map_batches", data)
    ds = ds.materialize()
    bundles = ds.iter_internal_ref_bundles()
    block = ray.get(next(bundles).block_refs[0])
    assert isinstance(block, pd.DataFrame)


def test_map_raises_on_incompatible_data(
    ray_start_regular_shared,
    restore_data_context,
):
    # For row-based map, the output buffer builds Arrow blocks eagerly, so
    # incompatible data raises ArrowConversionError when object fallback is disabled.
    class UnsupportedType:
        pass

    data = [UnsupportedType(), 1]
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False
    ds = _create_dataset("map", data)
    with pytest.raises(ArrowConversionError):
        ds.materialize()


@pytest.mark.parametrize(
    "op, data, should_fail, expected_type",
    [
        # Case A: Upon serializing to Arrow fallback to `ArrowPythonObjectType`
        ("map_batches", [1, 2**100], False, ArrowPythonObjectType()),
        ("map_batches", [1.0, 2**100], False, ArrowPythonObjectType()),
        ("map_batches", ["1.0", 2**100], False, ArrowPythonObjectType()),
        # Case B: No fallback to `ArrowPythonObjectType`, but type promotion allows
        #         int to be promoted to a double
        ("map_batches", [1.0, 2**4], False, pa.float64()),
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
        expected_msg = "Error converting data to Arrow:"

        assert expected_msg in error_msg
        assert "my_data" in error_msg

    else:
        ds.materialize()

        assert ds.schema().base_schema == pa.schema(
            [pa.field("id", pa.int64()), pa.field("my_data", expected_type)]
        )

        results = sorted(ds.take_all(), key=lambda r: r["id"])
        assert results == [{"id": i, "my_data": data[i]} for i in range(len(data))]


def test_concat_empty_chunked_struct_column_with_extra_field():
    from ray.data._internal.arrow_ops import transform_pyarrow

    st1 = pa.struct([("x", pa.int64())])
    st2 = pa.struct([("x", pa.int64()), ("y", pa.string())])

    empty_filtered = pa.table({"s": pa.array([{"x": 1}], type=st1)}).filter(
        pa.array([False])
    )
    assert empty_filtered.column("s").num_chunks == 0

    non_empty = pa.table({"s": pa.array([{"x": 2, "y": "a"}], type=st2)})

    result = transform_pyarrow.concat([empty_filtered, non_empty], promote_types=True)
    assert result.schema.field("s").type == st2
    assert result.to_pydict() == {"s": [{"x": 2, "y": "a"}]}


@pytest.mark.parametrize(
    "tensor_format", [FixedShapeTensorFormat.V1, FixedShapeTensorFormat.V2]
)
def test_concat_with_mixed_tensor_types_and_native_pyarrow_types(tensor_format_context):
    tensor_format = tensor_format_context
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
    if tensor_format == FixedShapeTensorFormat.V2:
        expected_tensor_type = ArrowTensorTypeV2((3, 3), pa.float32())
    else:
        expected_tensor_type = ArrowTensorType((3, 3), pa.float32())

    assert ds.schema().base_schema == pa.schema(
        [("int", pa.float64()), ("tensor", expected_tensor_type)]
    )
    assert ds.count() == num_rows


def test_concat_identical_schema_skips_unification(monkeypatch):
    from ray.data._internal.arrow_ops import transform_pyarrow

    tables = [
        pa.table(
            {
                "id": pa.array([i], type=pa.int64()),
                "value": pa.array([f"value_{i}"]),
            }
        )
        for i in range(32)
    ]

    def unexpected_unification(*args, **kwargs):
        pytest.fail(
            "Schema unification should be skipped "
            "when all input schemas are identical."
        )

    monkeypatch.setattr(
        transform_pyarrow,
        "unify_schemas",
        unexpected_unification,
    )

    result = transform_pyarrow.concat(tables)

    assert result.num_rows == 32
    assert result.schema.equals(tables[0].schema)
    assert result.column("id").to_pylist() == list(range(32))


def test_concat_different_schemas_uses_unification(monkeypatch):
    from ray.data._internal.arrow_ops import transform_pyarrow

    tables = [
        pa.table({"value": pa.array([1], type=pa.int32())}),
        pa.table({"value": pa.array([2], type=pa.int64())}),
    ]

    original_unify = transform_pyarrow.unify_schemas
    calls = []

    def tracked_unification(*args, **kwargs):
        calls.append(1)
        return original_unify(*args, **kwargs)

    monkeypatch.setattr(
        transform_pyarrow,
        "unify_schemas",
        tracked_unification,
    )

    result = transform_pyarrow.concat(
        tables,
        promote_types=True,
    )

    assert len(calls) == 1
    assert result.column("value").to_pylist() == [1, 2]
    assert result.schema.field("value").type == pa.int64()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
