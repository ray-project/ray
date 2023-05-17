import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.extensions import (
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorType,
)
from ray.data._internal.arrow_ops.transform_pyarrow import concat, unify_schemas


def test_arrow_concat_empty():
    # Test empty.
    assert concat([]) == []


def test_arrow_concat_single_block():
    # Test single block:
    t = pa.table({"a": [1, 2]})
    out = concat([t])
    assert len(out) == 2
    assert out == t


def test_arrow_concat_basic():
    # Test two basic tables.
    t1 = pa.table({"a": [1, 2], "b": [5, 6]})
    t2 = pa.table({"a": [3, 4], "b": [7, 8]})
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == 4
    # Check schema.
    assert out.column_names == ["a", "b"]
    assert out.schema.types == [pa.int64(), pa.int64()]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == 2
    assert out["b"].num_chunks == 2
    # Check content.
    assert out["a"].to_pylist() == [1, 2, 3, 4]
    assert out["b"].to_pylist() == [5, 6, 7, 8]
    # Check equivalence.
    expected = pa.concat_tables(ts)
    assert out == expected


def test_arrow_concat_null_promotion():
    # Test null column --> well-typed column promotion.
    t1 = pa.table({"a": [None, None], "b": [5, 6]})
    t2 = pa.table({"a": [3, 4], "b": [None, None]})
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == 4
    # Check schema.
    assert out.column_names == ["a", "b"]
    assert out.schema.types == [pa.int64(), pa.int64()]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == 2
    assert out["b"].num_chunks == 2
    # Check content.
    assert out["a"].to_pylist() == [None, None, 3, 4]
    assert out["b"].to_pylist() == [5, 6, None, None]
    # Check equivalence.
    expected = pa.concat_tables(ts, promote=True)
    assert out == expected


def test_arrow_concat_tensor_extension_uniform():
    # Test tensor column concatenation.
    a1 = np.arange(12).reshape((3, 2, 2))
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})
    a2 = np.arange(12, 24).reshape((3, 2, 2))
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == 6
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema.types == [ArrowTensorType((2, 2), pa.int64())]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == 2
    # Check content.
    np.testing.assert_array_equal(out["a"].chunk(0).to_numpy(), a1)
    np.testing.assert_array_equal(out["a"].chunk(1).to_numpy(), a2)
    # Check equivalence.
    expected = pa.concat_tables(ts, promote=True)
    assert out == expected


def test_arrow_concat_tensor_extension_variable_shaped():
    # Test variable_shaped tensor column concatenation.
    a1 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})
    a2 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == 4
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema.types == [ArrowVariableShapedTensorType(pa.int64(), 2)]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == 2
    # Check content.
    for o, e in zip(out["a"].chunk(0).to_numpy(), a1):
        np.testing.assert_array_equal(o, e)
    for o, e in zip(out["a"].chunk(1).to_numpy(), a2):
        np.testing.assert_array_equal(o, e)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


def test_arrow_concat_tensor_extension_uniform_and_variable_shaped():
    # Test concatenating a homogeneous-shaped tensor column with a variable-shaped
    # tensor column.
    a1 = np.arange(12).reshape((3, 2, 2))
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})
    a2 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == 5
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema.types == [ArrowVariableShapedTensorType(pa.int64(), 2)]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == 2
    # Check content.
    for o, e in zip(out["a"].chunk(0).to_numpy(), a1):
        np.testing.assert_array_equal(o, e)
    for o, e in zip(out["a"].chunk(1).to_numpy(), a2):
        np.testing.assert_array_equal(o, e)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


def test_arrow_concat_tensor_extension_uniform_but_different():
    # Test concatenating two homogeneous-shaped tensor columns with differing shapes
    # between them.
    a1 = np.arange(12).reshape((3, 2, 2))
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})
    a2 = np.arange(12, 39).reshape((3, 3, 3))
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == 6
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema.types == [ArrowVariableShapedTensorType(pa.int64(), 2)]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == 2
    # Check content.
    for o, e in zip(out["a"].chunk(0).to_numpy(), a1):
        np.testing.assert_array_equal(o, e)
    for o, e in zip(out["a"].chunk(1).to_numpy(), a2):
        np.testing.assert_array_equal(o, e)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


def test_unify_schemas():
    # Unifying a schema with the same schema as itself
    tensor_arr_1 = pa.schema([("tensor_arr", ArrowTensorType((3, 5), pa.int32()))])
    assert unify_schemas([tensor_arr_1, tensor_arr_1]) == tensor_arr_1

    # Single columns with different shapes
    tensor_arr_2 = pa.schema([("tensor_arr", ArrowTensorType((2, 1), pa.int32()))])
    contains_diff_shaped = [tensor_arr_1, tensor_arr_2]
    assert unify_schemas(contains_diff_shaped) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )

    # Single columns with same shapes
    tensor_arr_3 = pa.schema([("tensor_arr", ArrowTensorType((3, 5), pa.int32()))])
    contains_diff_types = [tensor_arr_1, tensor_arr_3]
    assert unify_schemas(contains_diff_types) == pa.schema(
        [
            ("tensor_arr", ArrowTensorType((3, 5), pa.int32())),
        ]
    )

    # Single columns with a variable shaped tensor, same ndim
    var_tensor_arr = pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )
    contains_var_shaped = [tensor_arr_1, var_tensor_arr]
    assert unify_schemas(contains_var_shaped) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )

    # Single columns with a variable shaped tensor, different ndim
    var_tensor_arr_1d = pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 1)),
        ]
    )
    var_tensor_arr_3d = pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 3)),
        ]
    )
    contains_1d2d = [tensor_arr_1, var_tensor_arr_1d]
    assert unify_schemas(contains_1d2d) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )
    contains_2d3d = [tensor_arr_1, var_tensor_arr_3d]
    assert unify_schemas(contains_2d3d) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 3)),
        ]
    )

    # Multi-column schemas
    multicol_schema_1 = pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowTensorType((4, 2), pa.int32())),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )
    multicol_schema_2 = pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowTensorType((4, 2), pa.int32())),
            ("col_var_tensor", ArrowTensorType((9, 4, 1, 0, 5), pa.int16())),
        ]
    )
    assert unify_schemas([multicol_schema_1, multicol_schema_2]) == pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowTensorType((4, 2), pa.int32())),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )

    multicol_schema_3 = pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowVariableShapedTensorType(pa.int32(), 3)),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )
    assert unify_schemas([multicol_schema_1, multicol_schema_3]) == pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowVariableShapedTensorType(pa.int32(), 3)),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )

    # Unifying >2 schemas together
    assert unify_schemas(
        [multicol_schema_1, multicol_schema_2, multicol_schema_3]
    ) == pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowVariableShapedTensorType(pa.int32(), 3)),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )


def test_arrow_block_select():
    df = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13], "three": [14, 15, 16]})
    table = pa.Table.from_pandas(df)
    block_accessor = BlockAccessor.for_block(table)

    block = block_accessor.select(["two"])
    assert block.schema == pa.schema([("two", pa.int64())])
    assert block.to_pandas().equals(df[["two"]])

    block = block_accessor.select(["two", "one"])
    assert block.schema == pa.schema([("two", pa.int64()), ("one", pa.int64())])
    assert block.to_pandas().equals(df[["two", "one"]])

    with pytest.raises(ValueError):
        block = block_accessor.select([lambda x: x % 3, "two"])


def test_arrow_block_slice_copy():
    # Test that ArrowBlock slicing properly copies the underlying Arrow
    # table.
    def check_for_copy(table1, table2, a, b, is_copy):
        expected_slice = table1.slice(a, b - a)
        assert table2.equals(expected_slice)
        assert table2.schema == table1.schema
        assert table1.num_columns == table2.num_columns
        for col1, col2 in zip(table1.columns, table2.columns):
            assert col1.num_chunks == col2.num_chunks
            for chunk1, chunk2 in zip(col1.chunks, col2.chunks):
                bufs1 = chunk1.buffers()
                bufs2 = chunk2.buffers()
                expected_offset = 0 if is_copy else a
                assert chunk2.offset == expected_offset
                assert len(chunk2) == b - a
                if is_copy:
                    assert bufs2[1].address != bufs1[1].address
                else:
                    assert bufs2[1].address == bufs1[1].address

    n = 20
    df = pd.DataFrame(
        {"one": list(range(n)), "two": ["a"] * n, "three": [np.nan] + [1.5] * (n - 1)}
    )
    table = pa.Table.from_pandas(df)
    a, b = 5, 10
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    check_for_copy(table, table2, a, b, is_copy=True)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    check_for_copy(table, table2, a, b, is_copy=False)


def test_arrow_block_slice_copy_empty():
    # Test that ArrowBlock slicing properly copies the underlying Arrow
    # table when the table is empty.
    df = pd.DataFrame({"one": []})
    table = pa.Table.from_pandas(df)
    a, b = 0, 0
    expected_slice = table.slice(a, b - a)
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    assert table2.equals(expected_slice)
    assert table2.schema == table.schema
    assert table2.num_rows == 0

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    assert table2.equals(expected_slice)
    assert table2.schema == table.schema
    assert table2.num_rows == 0


def test_convert_to_pyarrow(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(100)
    assert ds.to_dask().sum().compute()[0] == 4950
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
