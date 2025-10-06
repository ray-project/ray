import os
import re
import types
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.air.util.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    _extension_array_concat_supported,
)
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
    _align_struct_fields,
    concat,
    hash_partition,
    shuffle,
    try_combine_chunked_columns,
    unify_schemas,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.extensions import (
    ArrowConversionError,
    ArrowPythonObjectArray,
    ArrowPythonObjectType,
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
    _object_extension_type_allowed,
)


def test_try_defragment_table():
    chunks = np.array_split(np.arange(1000), 10)

    t = pa.Table.from_pydict(
        {
            "id": pa.chunked_array([pa.array(c) for c in chunks]),
        }
    )

    assert len(t["id"].chunks) == 10

    dt = try_combine_chunked_columns(t)

    assert len(dt["id"].chunks) == 1
    assert dt == t


def test_hash_partitioning():
    # Test hash-partitioning of the empty table
    empty_table = pa.Table.from_pydict({"idx": []})

    assert {} == hash_partition(empty_table, hash_cols=["idx"], num_partitions=5)

    # Test hash-partitioning of table into 1 partition (returns table itself)
    t = pa.Table.from_pydict({"idx": list(range(10))})

    assert {0: t} == hash_partition(t, hash_cols=["idx"], num_partitions=1)

    # Test hash-partitioning of proper table
    idx = list(range(100))

    t = pa.Table.from_pydict(
        {
            "idx": pa.array(idx),
            "ints": pa.array(idx),
            "floats": pa.array([float(i) for i in idx]),
            "strings": pa.array([str(i) for i in idx]),
            "structs": pa.array(
                [
                    {
                        "value": i,
                    }
                    for i in idx
                ]
            ),
        }
    )

    single_partition_dict = hash_partition(t, hash_cols=["idx"], num_partitions=1)

    # There's just 1 partition
    assert len(single_partition_dict) == 1
    assert t == single_partition_dict.get(0)

    def _concat_and_sort_partitions(parts: Iterable[pa.Table]) -> pa.Table:
        return pa.concat_tables(parts).sort_by("idx")

    _5_partition_dict = hash_partition(t, hash_cols=["strings"], num_partitions=5)

    assert len(_5_partition_dict) == 5
    assert t == _concat_and_sort_partitions(_5_partition_dict.values())

    # There could be no more partitions than elements
    _structs_partition_dict = hash_partition(
        t, hash_cols=["structs"], num_partitions=101
    )

    assert len(_structs_partition_dict) <= 101
    assert t == _concat_and_sort_partitions(_structs_partition_dict.values())


def test_shuffle():
    t = pa.Table.from_pydict(
        {
            "index": pa.array(list(range(10))),
        }
    )

    shuffled = shuffle(t, seed=0xDEED)

    assert shuffled == pa.Table.from_pydict(
        {"index": pa.array([4, 3, 6, 8, 7, 1, 5, 2, 9, 0])}
    )


def test_arrow_concat_empty(simple_concat_data):
    # Test empty.
    assert concat(simple_concat_data["empty"]) == pa.table([])


def test_arrow_concat_single_block(simple_concat_data):
    # Test single block:
    out = concat([simple_concat_data["single_block"]])
    assert len(out) == 2
    assert out == simple_concat_data["single_block"]


def test_arrow_concat_basic(basic_concat_blocks, basic_concat_expected):
    # Test two basic tables.
    ts = basic_concat_blocks
    out = concat(ts)
    # Check length.
    assert len(out) == basic_concat_expected["length"]
    # Check schema.
    assert out.column_names == basic_concat_expected["column_names"]
    assert out.schema.types == basic_concat_expected["schema_types"]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == basic_concat_expected["chunks"]
    assert out["b"].num_chunks == basic_concat_expected["chunks"]
    # Check content.
    assert out["a"].to_pylist() == basic_concat_expected["content"]["a"]
    assert out["b"].to_pylist() == basic_concat_expected["content"]["b"]
    # Check equivalence.
    expected = pa.concat_tables(ts)
    assert out == expected


def test_arrow_concat_null_promotion(null_promotion_blocks, null_promotion_expected):
    # Test null column --> well-typed column promotion.
    ts = null_promotion_blocks
    out = concat(ts)
    # Check length.
    assert len(out) == null_promotion_expected["length"]
    # Check schema.
    assert out.column_names == null_promotion_expected["column_names"]
    assert out.schema.types == null_promotion_expected["schema_types"]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == null_promotion_expected["chunks"]
    assert out["b"].num_chunks == null_promotion_expected["chunks"]
    # Check content.
    assert out["a"].to_pylist() == null_promotion_expected["content"]["a"]
    assert out["b"].to_pylist() == null_promotion_expected["content"]["b"]
    # Check equivalence.
    expected = pa.concat_tables(ts, promote=True)
    assert out == expected


def test_arrow_concat_tensor_extension_uniform(
    uniform_tensor_blocks, uniform_tensor_expected
):
    # Test tensor column concatenation.
    t1, t2 = uniform_tensor_blocks
    ts = [t1, t2]
    out = concat(ts)

    # Check length.
    assert len(out) == uniform_tensor_expected["length"]

    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema == uniform_tensor_expected["schema"]

    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == uniform_tensor_expected["chunks"]

    # Check content.
    content = uniform_tensor_expected["content"]
    np.testing.assert_array_equal(out["a"].chunk(0).to_numpy(), content[0])
    np.testing.assert_array_equal(out["a"].chunk(1).to_numpy(), content[1])

    # Check equivalence.
    expected = pa.concat_tables(ts, promote=True)
    assert out == expected


def test_arrow_concat_tensor_extension_variable_shaped(
    variable_shaped_tensor_blocks, variable_shaped_tensor_expected
):
    # Test variable_shaped tensor column concatenation.
    t1, t2 = variable_shaped_tensor_blocks
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == variable_shaped_tensor_expected["length"]
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema == variable_shaped_tensor_expected["schema"]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == variable_shaped_tensor_expected["chunks"]
    # Check content.
    content = variable_shaped_tensor_expected["content"]
    for o, e in zip(out["a"].chunk(0).to_numpy(), content[0]):
        np.testing.assert_array_equal(o, e)
    for o, e in zip(out["a"].chunk(1).to_numpy(), content[1]):
        np.testing.assert_array_equal(o, e)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


def test_arrow_concat_tensor_extension_uniform_and_variable_shaped(
    mixed_tensor_blocks, mixed_tensor_expected
):
    # Test concatenating a homogeneous-shaped tensor column with a variable-shaped
    # tensor column.
    t1, t2 = mixed_tensor_blocks
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == mixed_tensor_expected["length"]
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema == mixed_tensor_expected["schema"]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == mixed_tensor_expected["chunks"]
    # Check content.
    content = mixed_tensor_expected["content"]
    for o, e in zip(out["a"].chunk(0).to_numpy(), content[0]):
        np.testing.assert_array_equal(o, e)
    for o, e in zip(out["a"].chunk(1).to_numpy(), content[1]):
        np.testing.assert_array_equal(o, e)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


def test_arrow_concat_tensor_extension_uniform_but_different(
    different_shape_tensor_blocks, different_shape_tensor_expected
):
    # Test concatenating two homogeneous-shaped tensor columns with differing shapes
    # between them.
    t1, t2 = different_shape_tensor_blocks
    ts = [t1, t2]
    out = concat(ts)
    # Check length.
    assert len(out) == different_shape_tensor_expected["length"]
    # Check schema.
    assert out.column_names == ["a"]
    assert out.schema == different_shape_tensor_expected["schema"]
    # Confirm that concatenation is zero-copy (i.e. it didn't trigger chunk
    # consolidation).
    assert out["a"].num_chunks == different_shape_tensor_expected["chunks"]
    # Check content.
    content = different_shape_tensor_expected["content"]
    for o, e in zip(out["a"].chunk(0).to_numpy(), content[0]):
        np.testing.assert_array_equal(o, e)
    for o, e in zip(out["a"].chunk(1).to_numpy(), content[1]):
        np.testing.assert_array_equal(o, e)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


def test_arrow_concat_with_objects(object_concat_blocks, object_concat_expected):
    t3 = concat(object_concat_blocks)
    assert isinstance(t3, pa.Table)
    assert len(t3) == object_concat_expected["length"]
    assert isinstance(t3.schema.field("a").type, object_concat_expected["a_type"])
    assert object_concat_expected["b_type"](t3.schema.field("b").type)
    assert t3.column("a").to_pylist() == object_concat_expected["content"]["a"]
    assert t3.column("b").to_pylist() == object_concat_expected["content"]["b"]


def test_struct_with_different_field_names(
    struct_different_field_names_blocks, struct_different_field_names_expected
):
    # Ensures that when concatenating tables with struct columns having different
    # field names, missing fields in each struct are filled with None in the
    # resulting table.

    # Concatenate tables with different field names in struct
    t3 = concat(struct_different_field_names_blocks)

    assert isinstance(t3, pa.Table)
    assert len(t3) == struct_different_field_names_expected["length"]

    # Check the entire schema
    assert t3.schema == struct_different_field_names_expected["schema"]

    # Check that missing fields are filled with None
    assert (
        t3.column("a").to_pylist()
        == struct_different_field_names_expected["content"]["a"]
    )
    assert (
        t3.column("d").to_pylist()
        == struct_different_field_names_expected["content"]["d"]
    )


def test_nested_structs(nested_structs_blocks, nested_structs_expected):
    # Checks that deeply nested structs (3 levels of nesting) are handled properly
    # during concatenation and the resulting table preserves the correct nesting
    # structure.

    # Concatenate tables with nested structs and missing fields
    t3 = concat(nested_structs_blocks)
    assert isinstance(t3, pa.Table)
    assert len(t3) == nested_structs_expected["length"]

    # Validate the schema of the resulting table
    assert t3.schema == nested_structs_expected["schema"]

    # Validate the data in the concatenated table
    assert t3.column("a").to_pylist() == nested_structs_expected["content"]["a"]
    assert t3.column("d").to_pylist() == nested_structs_expected["content"]["d"]


def test_struct_with_null_values(
    struct_null_values_blocks, struct_null_values_expected
):
    # Ensures that when concatenating tables with struct columns containing null
    # values, the null values are properly handled, and the result reflects the
    # expected structure.

    # Concatenate tables with struct columns containing null values
    t3 = concat(struct_null_values_blocks)
    assert isinstance(t3, pa.Table)
    assert len(t3) == struct_null_values_expected["length"]

    # Validate the schema of the resulting table
    assert (
        t3.schema == struct_null_values_expected["schema"]
    ), f"Expected schema: {struct_null_values_expected['schema']}, but got {t3.schema}"

    # Verify the PyArrow table content
    assert t3.column("a").to_pylist() == struct_null_values_expected["content"]["a"]

    result = t3.column("d").to_pylist()
    expected = struct_null_values_expected["content"]["d"]
    assert result == expected, f"Expected {expected}, but got {result}"


def test_struct_with_mismatched_lengths(
    struct_mismatched_lengths_blocks, struct_mismatched_lengths_expected
):
    # Verifies that when concatenating tables with struct columns of different lengths,
    # the missing values are properly padded with None in the resulting table.

    # Concatenate tables with struct columns of different lengths
    t3 = concat(struct_mismatched_lengths_blocks)
    assert isinstance(t3, pa.Table)
    assert (
        len(t3) == struct_mismatched_lengths_expected["length"]
    )  # Check that the resulting table has the correct number of rows

    # Validate the schema of the resulting table
    assert (
        t3.schema == struct_mismatched_lengths_expected["schema"]
    ), f"Expected schema: {struct_mismatched_lengths_expected['schema']}, but got {t3.schema}"

    # Verify the content of the resulting table
    assert (
        t3.column("a").to_pylist() == struct_mismatched_lengths_expected["content"]["a"]
    )
    result = t3.column("d").to_pylist()
    expected = struct_mismatched_lengths_expected["content"]["d"]

    assert result == expected, f"Expected {expected}, but got {result}"


def test_struct_with_empty_arrays(
    struct_empty_arrays_blocks, struct_empty_arrays_expected
):
    # Checks the behavior when concatenating tables with structs containing empty
    # arrays, verifying that null structs are correctly handled.

    # Concatenate tables with struct columns containing null values
    t3 = concat(struct_empty_arrays_blocks)

    # Verify that the concatenated result is a valid PyArrow Table
    assert isinstance(t3, pa.Table)
    assert (
        len(t3) == struct_empty_arrays_expected["length"]
    )  # Check that the concatenated table has 3 rows

    # Validate the schema of the resulting concatenated table
    assert (
        t3.schema == struct_empty_arrays_expected["schema"]
    ), f"Expected schema: {struct_empty_arrays_expected['schema']}, but got {t3.schema}"

    # Verify the content of the concatenated table
    assert t3.column("a").to_pylist() == struct_empty_arrays_expected["content"]["a"]
    result = t3.column("d").to_pylist()
    expected = struct_empty_arrays_expected["content"]["d"]

    assert result == expected, f"Expected {expected}, but got {result}"


def test_struct_with_arrow_variable_shaped_tensor_type(
    struct_variable_shaped_tensor_blocks, struct_variable_shaped_tensor_expected
):
    # Test concatenating tables with struct columns containing ArrowVariableShapedTensorType
    # fields, ensuring proper handling of variable-shaped tensors within structs.

    # Concatenate tables with struct columns containing variable-shaped tensors
    t3 = concat(struct_variable_shaped_tensor_blocks)
    assert isinstance(t3, pa.Table)
    assert len(t3) == struct_variable_shaped_tensor_expected["length"]

    # Validate the schema of the resulting table
    assert (
        t3.schema == struct_variable_shaped_tensor_expected["schema"]
    ), f"Expected schema: {struct_variable_shaped_tensor_expected['schema']}, but got {t3.schema}"

    # Verify the content of the resulting table
    assert (
        t3.column("id").to_pylist()
        == struct_variable_shaped_tensor_expected["content"]["id"]
    )

    # Check that the struct column contains the expected data
    result_structs = t3.column("struct_with_tensor").to_pylist()
    assert len(result_structs) == 4

    # Verify each struct contains the correct metadata and tensor data
    expected_metadata = ["row1", "row2", "row3", "row4"]
    for i, (struct, expected_meta) in enumerate(zip(result_structs, expected_metadata)):
        assert struct["metadata"] == expected_meta
        assert isinstance(struct["tensor"], np.ndarray)

        # Verify tensor shapes match expectations
        if i == 0:
            assert struct["tensor"].shape == (2, 2)
            np.testing.assert_array_equal(
                struct["tensor"], np.ones((2, 2), dtype=np.float32)
            )
        elif i == 1:
            assert struct["tensor"].shape == (3, 3)
            np.testing.assert_array_equal(
                struct["tensor"], np.zeros((3, 3), dtype=np.float32)
            )
        elif i == 2:
            assert struct["tensor"].shape == (1, 4)
            np.testing.assert_array_equal(
                struct["tensor"], np.ones((1, 4), dtype=np.float32)
            )
        elif i == 3:
            assert struct["tensor"].shape == (2, 1)
            np.testing.assert_array_equal(
                struct["tensor"], np.zeros((2, 1), dtype=np.float32)
            )


def test_arrow_concat_object_with_tensor_fails(object_with_tensor_fails_blocks):
    with pytest.raises(ArrowConversionError) as exc_info:
        concat(object_with_tensor_fails_blocks)
    assert "objects and tensors" in str(exc_info.value.__cause__)


def test_unify_schemas(unify_schemas_basic_schemas, unify_schemas_multicol_schemas):
    # Unifying a schema with the same schema as itself
    schemas = unify_schemas_basic_schemas
    assert (
        unify_schemas([schemas["tensor_arr_1"], schemas["tensor_arr_1"]])
        == schemas["tensor_arr_1"]
    )

    # Single columns with different shapes
    contains_diff_shaped = [schemas["tensor_arr_1"], schemas["tensor_arr_2"]]
    assert unify_schemas(contains_diff_shaped) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )

    # Single columns with same shapes
    contains_diff_types = [schemas["tensor_arr_1"], schemas["tensor_arr_3"]]
    assert unify_schemas(contains_diff_types) == pa.schema(
        [
            ("tensor_arr", ArrowTensorType((3, 5), pa.int32())),
        ]
    )

    # Single columns with a variable shaped tensor, same ndim
    contains_var_shaped = [schemas["tensor_arr_1"], schemas["var_tensor_arr"]]
    assert unify_schemas(contains_var_shaped) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )

    # Single columns with a variable shaped tensor, different ndim
    contains_1d2d = [schemas["tensor_arr_1"], schemas["var_tensor_arr_1d"]]
    assert unify_schemas(contains_1d2d) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )
    contains_2d3d = [schemas["tensor_arr_1"], schemas["var_tensor_arr_3d"]]
    assert unify_schemas(contains_2d3d) == pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 3)),
        ]
    )

    # Multi-column schemas
    multicol = unify_schemas_multicol_schemas
    assert unify_schemas(
        [multicol["multicol_schema_1"], multicol["multicol_schema_2"]]
    ) == pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowTensorType((4, 2), pa.int32())),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )

    assert unify_schemas(
        [multicol["multicol_schema_1"], multicol["multicol_schema_3"]]
    ) == pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowVariableShapedTensorType(pa.int32(), 3)),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )

    # Unifying >2 schemas together
    assert unify_schemas(
        [
            multicol["multicol_schema_1"],
            multicol["multicol_schema_2"],
            multicol["multicol_schema_3"],
        ]
    ) == pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowVariableShapedTensorType(pa.int32(), 3)),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )


def test_unify_schemas_object_types(unify_schemas_object_types_schemas):
    """Test handling of object types (columns_with_objects functionality)."""
    schemas = unify_schemas_object_types_schemas

    # Should convert to ArrowPythonObjectType
    result = unify_schemas([schemas["object_schema"], schemas["int_schema"]])
    assert result == schemas["expected"]

    # Test multiple object types
    result = unify_schemas(
        [schemas["object_schema"], schemas["int_schema"], schemas["float_schema"]]
    )
    assert result == schemas["expected"]


def test_unify_schemas_incompatible_tensor_dtypes(
    unify_schemas_incompatible_tensor_schemas,
):
    """Test error handling for incompatible tensor dtypes."""
    import pyarrow as pa

    with pytest.raises(
        pa.lib.ArrowTypeError,
        match=re.escape(
            "Can't unify tensor types with divergent scalar types: [ArrowTensorType(shape=(2, 2), dtype=int32), ArrowTensorType(shape=(2, 2), dtype=float)]"
        ),
    ):
        unify_schemas(unify_schemas_incompatible_tensor_schemas)


def test_unify_schemas_objects_and_tensors(unify_schemas_objects_and_tensors_schemas):
    """Test error handling for intersection of objects and tensors."""
    with pytest.raises(ValueError, match="Found columns with both objects and tensors"):
        unify_schemas(unify_schemas_objects_and_tensors_schemas)


def test_unify_schemas_missing_tensor_fields(
    unify_schemas_missing_tensor_fields_schemas,
):
    """Test handling of missing tensor fields in structs (has_missing_fields logic)."""
    schemas = unify_schemas_missing_tensor_fields_schemas

    # Should convert tensor to variable-shaped to accommodate missing field
    result = unify_schemas([schemas["with_tensor"], schemas["without_tensor"]])
    assert result == schemas["expected"]


def test_unify_schemas_nested_struct_tensors(
    unify_schemas_nested_struct_tensors_schemas,
):
    """Test handling of nested structs with tensor fields."""
    schemas = unify_schemas_nested_struct_tensors_schemas

    # Should convert nested tensor to variable-shaped
    result = unify_schemas([schemas["with_tensor"], schemas["without_tensor"]])
    assert result == schemas["expected"]


def test_unify_schemas_edge_cases(unify_schemas_edge_cases_data):
    """Test edge cases and robustness."""
    data = unify_schemas_edge_cases_data

    # Empty schema list
    with pytest.raises(Exception):  # Should handle gracefully
        unify_schemas(data["empty_schemas"])

    # Single schema
    assert unify_schemas([data["single_schema"]]) == data["single_schema"]

    # Schemas with no common columns
    result = unify_schemas(
        [data["no_common_columns"]["schema1"], data["no_common_columns"]["schema2"]]
    )
    assert result == data["no_common_columns"]["expected"]

    # All null schemas
    result = unify_schemas(
        [data["all_null_schemas"]["schema1"], data["all_null_schemas"]["schema2"]]
    )
    assert result == data["all_null_schemas"]["schema1"]


def test_unify_schemas_mixed_tensor_types(unify_schemas_mixed_tensor_data):
    """Test handling of mixed tensor types (fixed and variable shaped)."""
    data = unify_schemas_mixed_tensor_data

    # Should result in variable-shaped tensor
    result = unify_schemas([data["fixed_shape"], data["variable_shaped"]])
    assert result == data["expected_variable"]

    # Test with different shapes but same dtype
    result = unify_schemas([data["fixed_shape"], data["different_shape"]])
    assert result == data["expected_variable"]


@pytest.mark.skipif(
    get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION,
    reason="Requires Arrow version of at least 14.0.0",
)
def test_unify_schemas_type_promotion(unify_schemas_type_promotion_data):
    data = unify_schemas_type_promotion_data

    # No type promotion
    assert (
        unify_schemas(
            [data["non_null"], data["nullable"]],
            promote_types=False,
        )
        == data["nullable"]
    )

    # No type promotion
    with pytest.raises(pa.lib.ArrowTypeError) as exc_info:
        unify_schemas(
            [data["int64"], data["float64"]],
            promote_types=False,
        )

    assert "Unable to merge: Field A has incompatible types: int64 vs double" == str(
        exc_info.value
    )

    # Type promoted
    assert (
        unify_schemas(
            [data["int64"], data["float64"]],
            promote_types=True,
        )
        == data["float64"]
    )


def test_arrow_block_select(block_select_data):
    data = block_select_data
    block_accessor = BlockAccessor.for_block(data["table"])

    block = block_accessor.select(data["single_column"]["columns"])
    assert block.schema == data["single_column"]["expected_schema"]
    assert block.to_pandas().equals(data["df"][data["single_column"]["columns"]])

    block = block_accessor.select(data["multiple_columns"]["columns"])
    assert block.schema == data["multiple_columns"]["expected_schema"]
    assert block.to_pandas().equals(data["df"][data["multiple_columns"]["columns"]])

    with pytest.raises(ValueError):
        block = block_accessor.select([lambda x: x % 3, "two"])


def test_arrow_block_slice_copy(block_slice_data):
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

    data = block_slice_data["normal"]
    table = data["table"]
    a, b = data["slice_params"]["a"], data["slice_params"]["b"]
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    check_for_copy(table, table2, a, b, is_copy=True)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    check_for_copy(table, table2, a, b, is_copy=False)


def test_arrow_block_slice_copy_empty(block_slice_data):
    # Test that ArrowBlock slicing properly copies the underlying Arrow
    # table when the table is empty.
    data = block_slice_data["empty"]
    table = data["table"]
    a, b = data["slice_params"]["a"], data["slice_params"]["b"]
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

        assert ds.take_all() == [
            {"id": i, "my_data": data[i]} for i in range(len(data))
        ]


def test_mixed_tensor_types_same_dtype(
    mixed_tensor_types_same_dtype_blocks, mixed_tensor_types_same_dtype_expected
):
    """Test mixed tensor types with same data type but different shapes."""

    t1, t2 = mixed_tensor_types_same_dtype_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == mixed_tensor_types_same_dtype_expected["length"]

    # Verify schema - should have tensor field as variable-shaped
    assert t3.schema == mixed_tensor_types_same_dtype_expected["schema"]
    tensor_field = t3.schema.field("tensor")
    assert isinstance(tensor_field.type, ArrowVariableShapedTensorType)

    # Verify content
    result_tensors = t3.column("tensor").to_pylist()
    assert len(result_tensors) == mixed_tensor_types_same_dtype_expected["length"]

    expected_tensors = mixed_tensor_types_same_dtype_expected["tensor_values"]

    # Verify each tensor
    for i, (result_tensor, expected_tensor) in enumerate(
        zip(result_tensors, expected_tensors)
    ):
        assert isinstance(result_tensor, np.ndarray)
        assert result_tensor.shape == expected_tensor.shape
        assert result_tensor.dtype == expected_tensor.dtype
        np.testing.assert_array_equal(result_tensor, expected_tensor)


def test_mixed_tensor_types_fixed_shape_different(
    mixed_tensor_types_fixed_shape_blocks, mixed_tensor_types_fixed_shape_expected
):
    """Test mixed tensor types with different fixed shapes."""

    t1, t2 = mixed_tensor_types_fixed_shape_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == mixed_tensor_types_fixed_shape_expected["length"]

    # Verify schema - should have tensor field as variable-shaped
    assert t3.schema == mixed_tensor_types_fixed_shape_expected["schema"]
    tensor_field = t3.schema.field("tensor")
    assert isinstance(tensor_field.type, ArrowVariableShapedTensorType)

    # Verify content
    result_tensors = t3.column("tensor").to_pylist()
    assert len(result_tensors) == mixed_tensor_types_fixed_shape_expected["length"]

    expected_tensors = mixed_tensor_types_fixed_shape_expected["tensor_values"]

    # Verify each tensor
    for i, (result_tensor, expected_tensor) in enumerate(
        zip(result_tensors, expected_tensors)
    ):
        assert isinstance(result_tensor, np.ndarray)
        assert result_tensor.shape == expected_tensor.shape
        assert result_tensor.dtype == expected_tensor.dtype
        np.testing.assert_array_equal(result_tensor, expected_tensor)


def test_mixed_tensor_types_variable_shaped(
    mixed_tensor_types_variable_shaped_blocks,
    mixed_tensor_types_variable_shaped_expected,
):
    """Test mixed tensor types with variable-shaped tensors."""

    t1, t2 = mixed_tensor_types_variable_shaped_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == mixed_tensor_types_variable_shaped_expected["length"]

    # Verify schema - should have tensor field as variable-shaped
    assert t3.schema == mixed_tensor_types_variable_shaped_expected["schema"]
    tensor_field = t3.schema.field("tensor")
    assert isinstance(tensor_field.type, ArrowVariableShapedTensorType)

    # Verify content
    result_tensors = t3.column("tensor").to_pylist()
    assert len(result_tensors) == mixed_tensor_types_variable_shaped_expected["length"]

    expected_tensors = mixed_tensor_types_variable_shaped_expected["tensor_values"]

    # Verify each tensor
    for i, (result_tensor, expected_tensor) in enumerate(
        zip(result_tensors, expected_tensors)
    ):
        assert isinstance(result_tensor, np.ndarray)
        assert result_tensor.shape == expected_tensor.shape
        assert result_tensor.dtype == expected_tensor.dtype
        np.testing.assert_array_equal(result_tensor, expected_tensor)


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_mixed_tensor_types_in_struct(
    struct_with_mixed_tensor_types_blocks, struct_with_mixed_tensor_types_expected
):
    """Test that the fix works for mixed tensor types in structs."""

    t1, t2 = struct_with_mixed_tensor_types_blocks

    # This should work with our fix
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == struct_with_mixed_tensor_types_expected["length"]

    # Verify the result has the expected structure
    assert t3.schema == struct_with_mixed_tensor_types_expected["schema"]
    assert "id" in t3.column_names
    assert "struct" in t3.column_names

    # Verify struct field contains both types of tensors
    struct_data = t3.column("struct").to_pylist()
    assert len(struct_data) == struct_with_mixed_tensor_types_expected["length"]

    expected_struct_values = struct_with_mixed_tensor_types_expected["struct_values"]

    # Verify struct values
    for i, (struct_row, expected_values) in enumerate(
        zip(struct_data, expected_struct_values)
    ):
        for key, expected_value in expected_values.items():
            assert struct_row[key] == expected_value


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_nested_struct_with_mixed_tensor_types(
    nested_struct_with_mixed_tensor_types_blocks,
    nested_struct_with_mixed_tensor_types_expected,
):
    """Test nested structs with mixed tensor types at different levels."""

    t1, t2 = nested_struct_with_mixed_tensor_types_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == nested_struct_with_mixed_tensor_types_expected["length"]

    # Verify the result has the expected structure
    assert t3.schema == nested_struct_with_mixed_tensor_types_expected["schema"]
    assert "id" in t3.column_names
    assert "complex_struct" in t3.column_names

    # Verify nested struct field contains both types of tensors
    struct_data = t3.column("complex_struct").to_pylist()
    assert len(struct_data) == nested_struct_with_mixed_tensor_types_expected["length"]

    expected_fields = nested_struct_with_mixed_tensor_types_expected["expected_fields"]

    # Check that nested structures are preserved
    for field in expected_fields:
        if field in ["nested", "outer_tensor", "outer_value"]:
            assert field in struct_data[0]
        elif field in ["inner_tensor", "inner_value"]:
            assert field in struct_data[0]["nested"]


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_multiple_tensor_fields_in_struct(
    multiple_tensor_fields_struct_blocks, multiple_tensor_fields_struct_expected
):
    """Test structs with multiple tensor fields of different types."""

    t1, t2 = multiple_tensor_fields_struct_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == multiple_tensor_fields_struct_expected["length"]

    # Verify the result has the expected structure
    assert t3.schema == multiple_tensor_fields_struct_expected["schema"]
    assert "id" in t3.column_names
    assert "multi_tensor_struct" in t3.column_names

    # Verify struct field contains both types of tensors
    struct_data = t3.column("multi_tensor_struct").to_pylist()
    assert len(struct_data) == multiple_tensor_fields_struct_expected["length"]

    expected_fields = multiple_tensor_fields_struct_expected["expected_fields"]

    # Check that all tensor fields are present
    for row in struct_data:
        for field in expected_fields:
            assert field in row


def test_struct_with_incompatible_tensor_dtypes_fails():
    """Test that concatenating structs with incompatible tensor dtypes fails gracefully."""

    # Block 1: Struct with float32 fixed-shape tensor
    tensor_data1 = np.ones((2, 2), dtype=np.float32)

    # Block 2: Struct with int64 variable-shaped tensor (different dtype)
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.int64),
            np.zeros((1, 4), dtype=np.int64),
        ],
        dtype=object,
    )

    t1, t2 = _create_struct_tensor_blocks(
        tensor_data1, tensor_data2, "fixed", "variable"
    )

    # This should fail because of incompatible tensor dtypes
    with pytest.raises(
        ArrowConversionError,
        match=re.escape(
            "Can't unify tensor types with divergent scalar types: [ArrowTensorTypeV2(shape=(2,), dtype=float), ArrowVariableShapedTensorType(ndim=2, dtype=int64)]"
        ),
    ):
        concat([t1, t2])


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_struct_with_additional_fields(
    struct_with_additional_fields_blocks, struct_with_additional_fields_expected
):
    """Test structs where some blocks have additional fields."""

    t1, t2 = struct_with_additional_fields_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == struct_with_additional_fields_expected["length"]

    # Verify the result has the expected structure
    assert t3.schema == struct_with_additional_fields_expected["schema"]
    assert "id" in t3.column_names
    assert "struct" in t3.column_names

    # Verify struct field contains both types of tensors
    struct_data = t3.column("struct").to_pylist()
    assert len(struct_data) == struct_with_additional_fields_expected["length"]

    field_presence = struct_with_additional_fields_expected["field_presence"]
    extra_values = struct_with_additional_fields_expected["extra_values"]

    # Check field presence and values
    for i, row in enumerate(struct_data):
        for field, should_be_present in field_presence.items():
            assert (field in row) == should_be_present

        # Check extra field values
        if "extra" in row:
            assert row["extra"] == extra_values[i]


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_struct_with_null_tensor_values(
    struct_with_null_tensor_values_blocks, struct_with_null_tensor_values_expected
):
    """Test structs where some fields are missing and get filled with nulls."""

    t1, t2 = struct_with_null_tensor_values_blocks

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == struct_with_null_tensor_values_expected["length"]

    # Validate schema - should have both fields
    assert t3.schema == struct_with_null_tensor_values_expected["schema"]

    # Validate result
    assert t3.column("id").to_pylist() == struct_with_null_tensor_values_expected["ids"]

    # Check the struct column directly to avoid the Arrow tensor extension null bug
    struct_column = t3.column("struct")
    expected_values = struct_with_null_tensor_values_expected["values"]
    expected_tensor_validity = struct_with_null_tensor_values_expected[
        "tensor_validity"
    ]

    # Check each row
    for i, (expected_value, expected_valid) in enumerate(
        zip(expected_values, expected_tensor_validity)
    ):
        assert struct_column[i]["value"].as_py() == expected_value

        if expected_valid:
            assert struct_column[i]["tensor"] is not None
        else:
            # Check that the tensor field is null by checking its validity
            tensor_field = struct_column[i]["tensor"]
            assert tensor_field.is_valid is False


# Test fixtures for _align_struct_fields tests
@pytest.fixture
def simple_struct_blocks():
    """Fixture for simple struct blocks with missing fields."""
    # Block 1: Struct with fields 'a' and 'b'
    struct_data1 = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    # Block 2: Struct with fields 'a' and 'c' (missing 'b', has 'c')
    struct_data2 = [{"a": 3, "c": True}, {"a": 4, "c": False}]

    return _create_basic_struct_blocks(
        struct_data1, struct_data2, id_data1=None, id_data2=None
    )


@pytest.fixture
def simple_struct_schema():
    """Fixture for simple struct schema with all fields."""
    struct_fields = [("a", pa.int64()), ("b", pa.string()), ("c", pa.bool_())]
    return _create_struct_schema(struct_fields, include_id=False)


@pytest.fixture
def nested_struct_blocks():
    """Fixture for nested struct blocks with missing fields."""
    # Block 1: Nested struct with inner fields 'x' and 'y'
    struct_data1 = [{"inner": {"x": 1, "y": "a"}}, {"inner": {"x": 2, "y": "b"}}]

    # Block 2: Nested struct with inner fields 'x' and 'z' (missing 'y', has 'z')
    struct_data2 = [{"inner": {"x": 3, "z": 1.5}}, {"inner": {"x": 4, "z": 2.5}}]

    return _create_basic_struct_blocks(
        struct_data1, struct_data2, column_name="outer", id_data1=None, id_data2=None
    )


@pytest.fixture
def nested_struct_schema():
    """Fixture for nested struct schema with all fields."""
    inner_fields = [("x", pa.int64()), ("y", pa.string()), ("z", pa.float64())]
    struct_fields = [("inner", pa.struct(inner_fields))]
    return _create_struct_schema(
        struct_fields,
        include_id=False,
        other_fields=[("outer", pa.struct(struct_fields))],
    )


@pytest.fixture
def missing_column_blocks():
    """Fixture for blocks where one is missing a struct column entirely."""
    # Block 1: Has struct column
    t1 = pa.table(
        {
            "struct": pa.array([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
            "other": pa.array([10, 20]),
        }
    )

    # Block 2: Missing struct column entirely
    t2 = pa.table({"other": pa.array([30, 40])})

    return t1, t2


@pytest.fixture
def missing_column_schema():
    """Fixture for schema with struct column that may be missing."""
    return pa.schema(
        [
            ("struct", pa.struct([("a", pa.int64()), ("b", pa.string())])),
            ("other", pa.int64()),
        ]
    )


@pytest.fixture
def multiple_struct_blocks():
    """Fixture for blocks with multiple struct columns."""
    # Block 1: Two struct columns with different field sets
    struct1_data1 = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    struct2_data1 = [{"p": 10, "q": True}, {"p": 20, "q": False}]

    # Block 2: Same struct columns but with different/missing fields
    struct1_data2 = [{"a": 3, "c": 1.5}, {"a": 4, "c": 2.5}]  # missing 'b', has 'c'
    struct2_data2 = [
        {"p": 30, "r": "alpha"},
        {"p": 40, "r": "beta"},
    ]  # missing 'q', has 'r'

    t1 = pa.table(
        {
            "struct1": pa.array(struct1_data1),
            "struct2": pa.array(struct2_data1),
        }
    )

    t2 = pa.table(
        {
            "struct1": pa.array(struct1_data2),
            "struct2": pa.array(struct2_data2),
        }
    )

    return t1, t2


@pytest.fixture
def multiple_struct_schema():
    """Fixture for schema with multiple struct columns."""
    struct1_fields = [("a", pa.int64()), ("b", pa.string()), ("c", pa.float64())]
    struct2_fields = [("p", pa.int64()), ("q", pa.bool_()), ("r", pa.string())]

    return pa.schema(
        [
            ("struct1", pa.struct(struct1_fields)),
            ("struct2", pa.struct(struct2_fields)),
        ]
    )


@pytest.fixture
def mixed_column_blocks():
    """Fixture for blocks with mix of struct and non-struct columns."""
    # Block 1: Mix of struct and non-struct columns
    struct_data1 = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    int_col1 = [10, 20]
    string_col1 = ["foo", "bar"]

    # Block 2: Same structure
    struct_data2 = [{"a": 3, "c": True}, {"a": 4, "c": False}]  # missing 'b', has 'c'
    int_col2 = [30, 40]
    string_col2 = ["baz", "qux"]

    t1 = pa.table(
        {
            "struct": pa.array(struct_data1),
            "int_col": pa.array(int_col1),
            "string_col": pa.array(string_col1),
        }
    )

    t2 = pa.table(
        {
            "struct": pa.array(struct_data2),
            "int_col": pa.array(int_col2),
            "string_col": pa.array(string_col2),
        }
    )

    return t1, t2


@pytest.fixture
def mixed_column_schema():
    """Fixture for schema with mix of struct and non-struct columns."""
    struct_fields = [("a", pa.int64()), ("b", pa.string()), ("c", pa.bool_())]

    return pa.schema(
        [
            ("struct", pa.struct(struct_fields)),
            ("int_col", pa.int64()),
            ("string_col", pa.string()),
        ]
    )


@pytest.fixture
def empty_block_blocks():
    """Fixture for blocks where one is empty."""
    # Empty block
    empty_struct_type = pa.struct([("a", pa.int64()), ("b", pa.string())])
    t1 = pa.table({"struct": pa.array([], type=empty_struct_type)})

    # Non-empty block
    struct_data2 = [{"a": 1, "c": True}, {"a": 2, "c": False}]  # missing 'b', has 'c'
    t2 = pa.table({"struct": pa.array(struct_data2)})

    return t1, t2


@pytest.fixture
def empty_block_schema():
    """Fixture for schema used with empty blocks."""
    struct_fields = [("a", pa.int64()), ("b", pa.string()), ("c", pa.bool_())]
    return _create_struct_schema(struct_fields, include_id=False)


@pytest.fixture
def already_aligned_blocks():
    """Fixture for blocks that are already aligned."""
    # Both blocks have identical schemas
    struct_data1 = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    struct_data2 = [{"a": 3, "b": "z"}, {"a": 4, "b": "w"}]

    return _create_basic_struct_blocks(
        struct_data1, struct_data2, id_data1=None, id_data2=None
    )


@pytest.fixture
def already_aligned_schema():
    """Fixture for schema used with already aligned blocks."""
    struct_fields = [("a", pa.int64()), ("b", pa.string())]
    return _create_struct_schema(struct_fields, include_id=False)


@pytest.fixture
def no_struct_blocks():
    """Fixture for blocks with no struct columns."""
    # Blocks with no struct columns
    int_col1 = [1, 2]
    string_col1 = ["a", "b"]
    int_col2 = [3, 4]
    string_col2 = ["c", "d"]

    t1 = pa.table({"int_col": pa.array(int_col1), "string_col": pa.array(string_col1)})
    t2 = pa.table({"int_col": pa.array(int_col2), "string_col": pa.array(string_col2)})

    return t1, t2


@pytest.fixture
def no_struct_schema():
    """Fixture for schema with no struct columns."""
    return pa.schema([("int_col", pa.int64()), ("string_col", pa.string())])


@pytest.fixture
def deep_nesting_blocks():
    """Fixture for blocks with deeply nested structs."""
    # Block 1: Deeply nested struct
    struct_data1 = [
        {"level2": {"level3": {"a": 1, "b": "x"}}},
        {"level2": {"level3": {"a": 2, "b": "y"}}},
    ]

    # Block 2: Same structure but missing some fields
    struct_data2 = [
        {"level2": {"level3": {"a": 3, "c": True}}},  # missing 'b', has 'c'
        {"level2": {"level3": {"a": 4, "c": False}}},
    ]

    return _create_basic_struct_blocks(
        struct_data1, struct_data2, column_name="level1", id_data1=None, id_data2=None
    )


@pytest.fixture
def deep_nesting_schema():
    """Fixture for schema with deeply nested structs."""
    level3_fields = [("a", pa.int64()), ("b", pa.string()), ("c", pa.bool_())]
    level2_fields = [("level3", pa.struct(level3_fields))]
    level1_fields = [("level2", pa.struct(level2_fields))]

    return pa.schema([("level1", pa.struct(level1_fields))])


def test_align_struct_fields_simple(simple_struct_blocks, simple_struct_schema):
    """Test basic struct field alignment with missing fields."""
    t1, t2 = simple_struct_blocks

    aligned_blocks = _align_struct_fields([t1, t2], simple_struct_schema)

    assert len(aligned_blocks) == 2

    # Check first block - should have 'c' field filled with None
    result1 = aligned_blocks[0]
    assert result1.schema == simple_struct_schema
    assert result1["struct"].to_pylist() == [
        {"a": 1, "b": "x", "c": None},
        {"a": 2, "b": "y", "c": None},
    ]

    # Check second block - should have 'b' field filled with None
    result2 = aligned_blocks[1]
    assert result2.schema == simple_struct_schema
    assert result2["struct"].to_pylist() == [
        {"a": 3, "b": None, "c": True},
        {"a": 4, "b": None, "c": False},
    ]


def test_align_struct_fields_nested(nested_struct_blocks, nested_struct_schema):
    """Test nested struct field alignment."""
    t1, t2 = nested_struct_blocks

    aligned_blocks = _align_struct_fields([t1, t2], nested_struct_schema)

    assert len(aligned_blocks) == 2

    # Check first block - should have 'z' field filled with None
    result1 = aligned_blocks[0]
    assert result1.schema == nested_struct_schema
    assert result1["outer"].to_pylist() == [
        {"inner": {"x": 1, "y": "a", "z": None}},
        {"inner": {"x": 2, "y": "b", "z": None}},
    ]

    # Check second block - should have 'y' field filled with None
    result2 = aligned_blocks[1]
    assert result2.schema == nested_struct_schema
    assert result2["outer"].to_pylist() == [
        {"inner": {"x": 3, "y": None, "z": 1.5}},
        {"inner": {"x": 4, "y": None, "z": 2.5}},
    ]


def test_align_struct_fields_missing_column(
    missing_column_blocks, missing_column_schema
):
    """Test alignment when a struct column is missing from some blocks."""
    t1, t2 = missing_column_blocks

    aligned_blocks = _align_struct_fields([t1, t2], missing_column_schema)

    assert len(aligned_blocks) == 2

    # Check first block - should be unchanged
    result1 = aligned_blocks[0]
    assert result1.schema == missing_column_schema
    assert result1["struct"].to_pylist() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    assert result1["other"].to_pylist() == [10, 20]

    # Check second block - should have null struct column
    result2 = aligned_blocks[1]
    assert result2.schema == missing_column_schema
    assert result2["struct"].to_pylist() == [None, None]
    assert result2["other"].to_pylist() == [30, 40]


def test_align_struct_fields_multiple_structs(
    multiple_struct_blocks, multiple_struct_schema
):
    """Test alignment with multiple struct columns."""
    t1, t2 = multiple_struct_blocks

    aligned_blocks = _align_struct_fields([t1, t2], multiple_struct_schema)

    assert len(aligned_blocks) == 2

    # Check first block
    result1 = aligned_blocks[0]
    assert result1.schema == multiple_struct_schema
    assert result1["struct1"].to_pylist() == [
        {"a": 1, "b": "x", "c": None},
        {"a": 2, "b": "y", "c": None},
    ]
    assert result1["struct2"].to_pylist() == [
        {"p": 10, "q": True, "r": None},
        {"p": 20, "q": False, "r": None},
    ]

    # Check second block
    result2 = aligned_blocks[1]
    assert result2.schema == multiple_struct_schema
    assert result2["struct1"].to_pylist() == [
        {"a": 3, "b": None, "c": 1.5},
        {"a": 4, "b": None, "c": 2.5},
    ]
    assert result2["struct2"].to_pylist() == [
        {"p": 30, "q": None, "r": "alpha"},
        {"p": 40, "q": None, "r": "beta"},
    ]


def test_align_struct_fields_non_struct_columns(
    mixed_column_blocks, mixed_column_schema
):
    """Test that non-struct columns are left unchanged."""
    t1, t2 = mixed_column_blocks

    aligned_blocks = _align_struct_fields([t1, t2], mixed_column_schema)

    assert len(aligned_blocks) == 2

    # Check that non-struct columns are unchanged
    for i, block in enumerate(aligned_blocks):
        assert block["int_col"].to_pylist() == [10 + i * 20, 20 + i * 20]
        assert (
            block["string_col"].to_pylist() == ["foo", "bar"]
            if i == 0
            else ["baz", "qux"]
        )


def test_align_struct_fields_empty_blocks(empty_block_blocks, empty_block_schema):
    """Test alignment with empty blocks."""
    t1, t2 = empty_block_blocks

    aligned_blocks = _align_struct_fields([t1, t2], empty_block_schema)

    assert len(aligned_blocks) == 2

    # Check empty block
    result1 = aligned_blocks[0]
    assert result1.schema == empty_block_schema
    assert len(result1) == 0

    # Check non-empty block
    result2 = aligned_blocks[1]
    assert result2.schema == empty_block_schema
    assert result2["struct"].to_pylist() == [
        {"a": 1, "b": None, "c": True},
        {"a": 2, "b": None, "c": False},
    ]


def test_align_struct_fields_already_aligned(
    already_aligned_blocks, already_aligned_schema
):
    """Test that already aligned blocks are returned unchanged."""
    t1, t2 = already_aligned_blocks

    aligned_blocks = _align_struct_fields([t1, t2], already_aligned_schema)

    # Should return the original blocks unchanged
    assert aligned_blocks == [t1, t2]


def test_align_struct_fields_no_struct_columns(no_struct_blocks, no_struct_schema):
    """Test alignment when there are no struct columns in the schema."""
    t1, t2 = no_struct_blocks

    aligned_blocks = _align_struct_fields([t1, t2], no_struct_schema)

    # Should return the original blocks unchanged
    assert aligned_blocks == [t1, t2]


def test_align_struct_fields_deep_nesting(deep_nesting_blocks, deep_nesting_schema):
    """Test alignment with deeply nested structs."""
    t1, t2 = deep_nesting_blocks

    aligned_blocks = _align_struct_fields([t1, t2], deep_nesting_schema)

    assert len(aligned_blocks) == 2

    # Check first block - should have 'c' field filled with None
    result1 = aligned_blocks[0]
    assert result1.schema == deep_nesting_schema
    assert result1["level1"].to_pylist() == [
        {"level2": {"level3": {"a": 1, "b": "x", "c": None}}},
        {"level2": {"level3": {"a": 2, "b": "y", "c": None}}},
    ]

    # Check second block - should have 'b' field filled with None
    result2 = aligned_blocks[1]
    assert result2.schema == deep_nesting_schema
    assert result2["level1"].to_pylist() == [
        {"level2": {"level3": {"a": 3, "b": None, "c": True}}},
        {"level2": {"level3": {"a": 4, "b": None, "c": False}}},
    ]


# Test fixtures for tensor-related tests
@pytest.fixture
def uniform_tensor_blocks():
    """Fixture for uniform tensor blocks with same shape."""
    # Block 1: Fixed shape tensors (2x2)
    a1 = np.arange(12).reshape((3, 2, 2))
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})

    # Block 2: Fixed shape tensors (2x2)
    a2 = np.arange(12, 24).reshape((3, 2, 2))
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})

    return t1, t2


@pytest.fixture
def uniform_tensor_expected():
    """Fixture for expected results from uniform tensor concatenation."""
    if DataContext.get_current().use_arrow_tensor_v2:
        tensor_type = ArrowTensorTypeV2
    else:
        tensor_type = ArrowTensorType

    expected_schema = pa.schema([("a", tensor_type((2, 2), pa.int64()))])
    expected_length = 6
    expected_chunks = 2

    # Expected content
    a1 = np.arange(12).reshape((3, 2, 2))
    a2 = np.arange(12, 24).reshape((3, 2, 2))

    return {
        "schema": expected_schema,
        "length": expected_length,
        "chunks": expected_chunks,
        "content": [a1, a2],
    }


@pytest.fixture
def variable_shaped_tensor_blocks():
    """Fixture for variable-shaped tensor blocks."""
    # Block 1: Variable shape tensors
    a1 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})

    # Block 2: Variable shape tensors
    a2 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})

    return t1, t2


@pytest.fixture
def variable_shaped_tensor_expected():
    """Fixture for expected results from variable-shaped tensor concatenation."""
    expected_schema = pa.schema([("a", ArrowVariableShapedTensorType(pa.int64(), 2))])
    expected_length = 4
    expected_chunks = 2

    # Expected content
    a1 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    a2 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )

    return {
        "schema": expected_schema,
        "length": expected_length,
        "chunks": expected_chunks,
        "content": [a1, a2],
    }


@pytest.fixture
def mixed_tensor_blocks():
    """Fixture for mixed fixed-shape and variable-shaped tensor blocks."""
    # Block 1: Fixed shape tensors
    a1 = np.arange(12).reshape((3, 2, 2))
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})

    # Block 2: Variable shape tensors
    a2 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})

    return t1, t2


@pytest.fixture
def mixed_tensor_expected():
    """Fixture for expected results from mixed tensor concatenation."""
    expected_schema = pa.schema([("a", ArrowVariableShapedTensorType(pa.int64(), 2))])
    expected_length = 5
    expected_chunks = 2

    # Expected content
    a1 = np.arange(12).reshape((3, 2, 2))
    a2 = np.array(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))], dtype=object
    )

    return {
        "schema": expected_schema,
        "length": expected_length,
        "chunks": expected_chunks,
        "content": [a1, a2],
    }


@pytest.fixture
def different_shape_tensor_blocks():
    """Fixture for tensor blocks with different fixed shapes."""
    # Block 1: Fixed shape tensors (2x2)
    a1 = np.arange(12).reshape((3, 2, 2))
    t1 = pa.table({"a": ArrowTensorArray.from_numpy(a1)})

    # Block 2: Fixed shape tensors (3x3)
    a2 = np.arange(12, 39).reshape((3, 3, 3))
    t2 = pa.table({"a": ArrowTensorArray.from_numpy(a2)})

    return t1, t2


@pytest.fixture
def different_shape_tensor_expected():
    """Fixture for expected results from different shape tensor concatenation."""
    expected_schema = pa.schema([("a", ArrowVariableShapedTensorType(pa.int64(), 2))])
    expected_length = 6
    expected_chunks = 2

    # Expected content
    a1 = np.arange(12).reshape((3, 2, 2))
    a2 = np.arange(12, 39).reshape((3, 3, 3))

    return {
        "schema": expected_schema,
        "length": expected_length,
        "chunks": expected_chunks,
        "content": [a1, a2],
    }


@pytest.fixture
def mixed_tensor_types_same_dtype_blocks():
    """Fixture for mixed tensor types with same dtype but different shapes."""
    # Block 1: Fixed shape tensors with float32
    tensor_data1 = np.ones((2, 2), dtype=np.float32)

    # Block 2: Variable shape tensors with float32
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )

    return _create_tensor_blocks(tensor_data1, tensor_data2, "fixed", "variable")


@pytest.fixture
def mixed_tensor_types_same_dtype_expected():
    """Fixture for expected results from mixed tensor types with same dtype."""
    expected_schema = _create_tensor_schema(struct_name="tensor")
    expected_tensors = [
        # First 2 were converted to var-shaped with their shape expanded
        # with singleton axis: from (2,) to (1, 2)
        np.ones((1, 2), dtype=np.float32),
        np.ones((1, 2), dtype=np.float32),
        # Last 2 were left intact
        np.ones((3, 3), dtype=np.float32),
        np.zeros((1, 4), dtype=np.float32),
    ]

    return _create_expected_result(expected_schema, 4, tensor_values=expected_tensors)


@pytest.fixture
def mixed_tensor_types_fixed_shape_blocks():
    """Fixture for mixed tensor types with different fixed shapes."""
    # Block 1: Fixed shape tensors (2x2)
    tensor_data1 = np.ones((2, 2), dtype=np.float32)

    # Block 2: Fixed shape tensors (3x3)
    tensor_data2 = np.zeros((3, 3), dtype=np.float32)

    return _create_tensor_blocks(
        tensor_data1, tensor_data2, "fixed", "fixed", id_data2=[3, 4, 5]
    )


@pytest.fixture
def mixed_tensor_types_fixed_shape_expected():
    """Fixture for expected results from mixed tensor types with different fixed shapes."""
    expected_schema = _create_tensor_schema(struct_name="tensor", ndim=1)
    expected_tensors = [
        np.ones((2,), dtype=np.float32),  # First 2 converted to variable-shaped
        np.ones((2,), dtype=np.float32),
        np.zeros((3,), dtype=np.float32),  # Last 3 variable-shaped
        np.zeros((3,), dtype=np.float32),
        np.zeros((3,), dtype=np.float32),
    ]

    return _create_expected_result(expected_schema, 5, tensor_values=expected_tensors)


@pytest.fixture
def mixed_tensor_types_variable_shaped_blocks():
    """Fixture for mixed tensor types with variable-shaped tensors."""
    # Block 1: Variable shape tensors
    tensor_data1 = np.array(
        [
            np.ones((2, 2), dtype=np.float32),
            np.zeros((3, 3), dtype=np.float32),
        ],
        dtype=object,
    )

    # Block 2: Variable shape tensors with different shapes
    tensor_data2 = np.array(
        [
            np.ones((1, 4), dtype=np.float32),
            np.zeros((2, 1), dtype=np.float32),
        ],
        dtype=object,
    )

    return _create_tensor_blocks(tensor_data1, tensor_data2, "variable", "variable")


@pytest.fixture
def mixed_tensor_types_variable_shaped_expected():
    """Fixture for expected results from mixed variable-shaped tensor types."""
    expected_schema = _create_tensor_schema(struct_name="tensor")
    expected_tensors = [
        np.ones((2, 2), dtype=np.float32),
        np.zeros((3, 3), dtype=np.float32),
        np.ones((1, 4), dtype=np.float32),
        np.zeros((2, 1), dtype=np.float32),
    ]

    return _create_expected_result(expected_schema, 4, tensor_values=expected_tensors)


@pytest.fixture
def struct_with_mixed_tensor_types_blocks():
    """Fixture for struct blocks with mixed tensor types."""
    # Block 1: Struct with fixed-shape tensor
    tensor_data1 = np.ones((2, 2), dtype=np.float32)

    # Block 2: Struct with variable-shaped tensor
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )

    return _create_struct_tensor_blocks(tensor_data1, tensor_data2, "fixed", "variable")


@pytest.fixture
def struct_with_mixed_tensor_types_expected():
    """Fixture for expected results from struct with mixed tensor types."""
    expected_schema = _create_tensor_schema(struct_name="struct")
    expected_struct_values = [
        {"value": 1},  # First two from fixed-shape tensor struct
        {"value": 2},
        {"value": 3},  # Last two from variable-shaped tensor struct
        {"value": 4},
    ]

    return _create_expected_result(
        expected_schema, 4, struct_values=expected_struct_values
    )


@pytest.fixture
def nested_struct_with_mixed_tensor_types_blocks():
    """Fixture for nested struct blocks with mixed tensor types."""
    # Block 1: Nested struct with fixed-shape tensors
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = _create_tensor_array(tensor_data1, "fixed")
    inner_struct1 = pa.StructArray.from_arrays(
        [tensor_array1, pa.array([10, 20], type=pa.int64())],
        names=["inner_tensor", "inner_value"],
    )
    outer_tensor1 = _create_tensor_array(np.zeros((2, 1), dtype=np.float32), "fixed")
    outer_struct1 = pa.StructArray.from_arrays(
        [inner_struct1, outer_tensor1, pa.array([1, 2], type=pa.int64())],
        names=["nested", "outer_tensor", "outer_value"],
    )
    t1 = pa.table({"id": [1, 2], "complex_struct": outer_struct1})

    # Block 2: Nested struct with variable-shaped tensors
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array2 = _create_tensor_array(tensor_data2, "variable")
    inner_struct2 = pa.StructArray.from_arrays(
        [tensor_array2, pa.array([30, 40], type=pa.int64())],
        names=["inner_tensor", "inner_value"],
    )
    outer_tensor2 = _create_tensor_array(
        np.array(
            [np.ones((2, 2), dtype=np.float32), np.zeros((1, 3), dtype=np.float32)],
            dtype=object,
        ),
        "variable",
    )
    outer_struct2 = pa.StructArray.from_arrays(
        [inner_struct2, outer_tensor2, pa.array([3, 4], type=pa.int64())],
        names=["nested", "outer_tensor", "outer_value"],
    )
    t2 = pa.table({"id": [3, 4], "complex_struct": outer_struct2})

    return t1, t2


@pytest.fixture
def nested_struct_with_mixed_tensor_types_expected():
    """Fixture for expected results from nested struct with mixed tensor types."""
    expected_schema = pa.schema(
        [
            ("id", pa.int64()),
            (
                "complex_struct",
                pa.struct(
                    [
                        (
                            "nested",
                            pa.struct(
                                [
                                    (
                                        "inner_tensor",
                                        ArrowVariableShapedTensorType(pa.float32(), 2),
                                    ),
                                    ("inner_value", pa.int64()),
                                ]
                            ),
                        ),
                        (
                            "outer_tensor",
                            ArrowVariableShapedTensorType(pa.float32(), 2),
                        ),
                        ("outer_value", pa.int64()),
                    ]
                ),
            ),
        ]
    )
    expected_fields = [
        "nested",
        "outer_tensor",
        "outer_value",
        "inner_tensor",
        "inner_value",
    ]

    return _create_expected_result(expected_schema, 4, expected_fields=expected_fields)


@pytest.fixture
def multiple_tensor_fields_struct_blocks():
    """Fixture for struct blocks with multiple tensor fields."""
    # Block 1: Struct with multiple fixed-shape tensors
    tensor1_data = np.ones((2, 2), dtype=np.float32)
    tensor1_array = _create_tensor_array(tensor1_data, "fixed")
    tensor2_data = np.zeros((2, 3), dtype=np.int32)
    tensor2_array = _create_tensor_array(tensor2_data, "fixed")
    struct_array1 = pa.StructArray.from_arrays(
        [tensor1_array, tensor2_array, pa.array([1, 2], type=pa.int64())],
        names=["tensor1", "tensor2", "value"],
    )
    t1 = pa.table({"id": [1, 2], "multi_tensor_struct": struct_array1})

    # Block 2: Struct with multiple variable-shaped tensors
    tensor1_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor1_array2 = _create_tensor_array(tensor1_data2, "variable")
    tensor2_data2 = np.array(
        [
            np.ones((2, 2), dtype=np.int32),
            np.zeros((3, 1), dtype=np.int32),
        ],
        dtype=object,
    )
    tensor2_array2 = _create_tensor_array(tensor2_data2, "variable")
    struct_array2 = pa.StructArray.from_arrays(
        [tensor1_array2, tensor2_array2, pa.array([3, 4], type=pa.int64())],
        names=["tensor1", "tensor2", "value"],
    )
    t2 = pa.table({"id": [3, 4], "multi_tensor_struct": struct_array2})

    return t1, t2


@pytest.fixture
def multiple_tensor_fields_struct_expected():
    """Fixture for expected results from struct with multiple tensor fields."""
    expected_schema = pa.schema(
        [
            ("id", pa.int64()),
            (
                "multi_tensor_struct",
                pa.struct(
                    [
                        ("tensor1", ArrowVariableShapedTensorType(pa.float32(), 2)),
                        ("tensor2", ArrowVariableShapedTensorType(pa.int32(), 2)),
                        ("value", pa.int64()),
                    ]
                ),
            ),
        ]
    )
    expected_fields = ["tensor1", "tensor2", "value"]

    return _create_expected_result(expected_schema, 4, expected_fields=expected_fields)


@pytest.fixture
def struct_with_additional_fields_blocks():
    """Fixture for struct blocks where some have additional fields."""
    # Block 1: Struct with tensor field and basic fields
    tensor_data1 = np.ones((2, 2), dtype=np.float32)

    # Block 2: Struct with tensor field and additional fields
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )

    return _create_struct_tensor_blocks(
        tensor_data1, tensor_data2, "fixed", "variable", extra_data2=["a", "b"]
    )


@pytest.fixture
def struct_with_additional_fields_expected():
    """Fixture for expected results from struct with additional fields."""
    expected_schema = _create_tensor_schema(struct_name="struct", include_extra=True)
    expected_field_presence = {"tensor": True, "value": True, "extra": True}
    expected_extra_values = [None, None, "a", "b"]

    return _create_expected_result(
        expected_schema,
        4,
        field_presence=expected_field_presence,
        extra_values=expected_extra_values,
    )


@pytest.fixture
def struct_with_null_tensor_values_blocks():
    """Fixture for struct blocks where some fields are missing and get filled with nulls."""
    # Block 1: Struct with tensor and value fields
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)
    value_array1 = pa.array([1, 2], type=pa.int64())
    struct_array1 = pa.StructArray.from_arrays(
        [tensor_array1, value_array1], names=["tensor", "value"]
    )
    t1 = pa.table({"id": [1, 2], "struct": struct_array1})

    # Block 2: Struct with only value field (missing tensor field)
    value_array2 = pa.array([3], type=pa.int64())
    struct_array2 = pa.StructArray.from_arrays([value_array2], names=["value"])
    t2 = pa.table({"id": [3], "struct": struct_array2})

    return t1, t2


@pytest.fixture
def struct_with_null_tensor_values_expected():
    """Fixture for expected results from struct with null tensor values."""
    expected_schema = pa.schema(
        [
            ("id", pa.int64()),
            (
                "struct",
                pa.struct(
                    [
                        ("tensor", ArrowTensorTypeV2((2,), pa.float32())),
                        ("value", pa.int64()),
                    ]
                ),
            ),
        ]
    )
    expected_length = 3
    expected_ids = [1, 2, 3]

    # Expected value field values
    expected_values = [1, 2, 3]

    # Expected tensor field validity
    expected_tensor_validity = [True, True, False]

    return {
        "schema": expected_schema,
        "length": expected_length,
        "ids": expected_ids,
        "values": expected_values,
        "tensor_validity": expected_tensor_validity,
    }


@pytest.fixture
def basic_concat_blocks():
    """Fixture for basic concat test data."""
    t1 = pa.table({"a": [1, 2], "b": [5, 6]})
    t2 = pa.table({"a": [3, 4], "b": [7, 8]})
    return [t1, t2]


@pytest.fixture
def basic_concat_expected():
    """Fixture for basic concat expected results."""
    return {
        "length": 4,
        "column_names": ["a", "b"],
        "schema_types": [pa.int64(), pa.int64()],
        "chunks": 2,
        "content": {"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]},
    }


@pytest.fixture
def null_promotion_blocks():
    """Fixture for null promotion test data."""
    t1 = pa.table({"a": [None, None], "b": [5, 6]})
    t2 = pa.table({"a": [3, 4], "b": [None, None]})
    return [t1, t2]


@pytest.fixture
def null_promotion_expected():
    """Fixture for null promotion expected results."""
    return {
        "length": 4,
        "column_names": ["a", "b"],
        "schema_types": [pa.int64(), pa.int64()],
        "chunks": 2,
        "content": {"a": [None, None, 3, 4], "b": [5, 6, None, None]},
    }


@pytest.fixture
def struct_different_field_names_blocks():
    """Fixture for struct with different field names test data."""
    struct_data1 = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]
    struct_data2 = [{"x": 3, "z": "c"}]

    struct_type1 = pa.struct([("x", pa.int32()), ("y", pa.string())])
    struct_type2 = pa.struct([("x", pa.int32()), ("z", pa.string())])

    additional_columns1 = {"a": [1, 2]}
    additional_columns2 = {"a": [3]}

    return _create_struct_blocks_with_columns(
        struct_data1,
        struct_data2,
        struct_type1,
        struct_type2,
        additional_columns1,
        additional_columns2,
    )


@pytest.fixture
def struct_different_field_names_expected():
    """Fixture for struct with different field names expected results."""
    field_names = ["x", "y", "z"]
    field_types = [pa.int32(), pa.string(), pa.string()]
    additional_fields = [("a", pa.int64())]

    schema = _create_simple_struct_schema(field_names, field_types, additional_fields)

    content = {
        "a": [1, 2, 3],
        "d": [
            {"x": 1, "y": "a", "z": None},
            {"x": 2, "y": "b", "z": None},
            {"x": 3, "y": None, "z": "c"},
        ],
    }

    return _create_struct_expected_result(schema, 3, content)


@pytest.fixture
def nested_structs_blocks():
    """Fixture for nested structs test data."""
    t1 = pa.table(
        {
            "a": [1],
            "d": pa.array(
                [
                    {
                        "x": {
                            "y": {"p": 1},  # Missing "q"
                            "z": {"m": 3},  # Missing "n"
                        },
                        "w": 5,
                    }
                ],
                type=pa.struct(
                    [
                        (
                            "x",
                            pa.struct(
                                [
                                    (
                                        "y",
                                        pa.struct([("p", pa.int32())]),  # Only "p"
                                    ),
                                    (
                                        "z",
                                        pa.struct([("m", pa.int32())]),  # Only "m"
                                    ),
                                ]
                            ),
                        ),
                        ("w", pa.int32()),
                    ]
                ),
            ),
        }
    )
    t2 = pa.table(
        {
            "a": [2],
            "d": pa.array(
                [
                    {
                        "x": {
                            "y": {"q": 7},  # Missing "p"
                            "z": {"n": 9},  # Missing "m"
                        },
                        "w": 10,
                    }
                ],
                type=pa.struct(
                    [
                        (
                            "x",
                            pa.struct(
                                [
                                    (
                                        "y",
                                        pa.struct([("q", pa.int32())]),  # Only "q"
                                    ),
                                    (
                                        "z",
                                        pa.struct([("n", pa.int32())]),  # Only "n"
                                    ),
                                ]
                            ),
                        ),
                        ("w", pa.int32()),
                    ]
                ),
            ),
        }
    )
    return [t1, t2]


@pytest.fixture
def nested_structs_expected():
    """Fixture for nested structs expected results."""
    return {
        "length": 2,
        "schema": pa.schema(
            [
                ("a", pa.int64()),
                (
                    "d",
                    pa.struct(
                        [
                            (
                                "x",
                                pa.struct(
                                    [
                                        (
                                            "y",
                                            pa.struct(
                                                [("p", pa.int32()), ("q", pa.int32())]
                                            ),
                                        ),
                                        (
                                            "z",
                                            pa.struct(
                                                [("m", pa.int32()), ("n", pa.int32())]
                                            ),
                                        ),
                                    ]
                                ),
                            ),
                            ("w", pa.int32()),
                        ]
                    ),
                ),
            ]
        ),
        "content": {
            "a": [1, 2],
            "d": [
                {
                    "x": {
                        "y": {"p": 1, "q": None},  # Missing "q" filled with None
                        "z": {"m": 3, "n": None},  # Missing "n" filled with None
                    },
                    "w": 5,
                },
                {
                    "x": {
                        "y": {"p": None, "q": 7},  # Missing "p" filled with None
                        "z": {"m": None, "n": 9},  # Missing "m" filled with None
                    },
                    "w": 10,
                },
            ],
        },
    }


@pytest.fixture
def struct_null_values_blocks():
    """Fixture for struct with null values test data."""
    struct_data1 = [{"x": 1, "y": "a"}, None]  # Second row is null
    struct_data2 = [None]  # Entire struct is null

    field_names = ["x", "y"]
    field_types = [pa.int32(), pa.string()]
    additional_columns1 = {"a": [1, 2]}
    additional_columns2 = {"a": [3]}

    return _create_simple_struct_blocks(
        struct_data1,
        struct_data2,
        field_names,
        field_types,
        additional_columns1,
        additional_columns2,
    )


@pytest.fixture
def struct_null_values_expected():
    """Fixture for struct with null values expected results."""
    field_names = ["x", "y"]
    field_types = [pa.int32(), pa.string()]
    additional_fields = [("a", pa.int64())]

    schema = _create_simple_struct_schema(field_names, field_types, additional_fields)

    content = {
        "a": [1, 2, 3],
        "d": [
            {"x": 1, "y": "a"},
            None,  # Entire struct is None, not {"x": None, "y": None}
            None,  # Entire struct is None, not {"x": None, "y": None}
        ],
    }

    return _create_struct_expected_result(schema, 3, content)


@pytest.fixture
def struct_mismatched_lengths_blocks():
    """Fixture for struct with mismatched lengths test data."""
    struct_data1 = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]
    struct_data2 = [{"x": 3, "y": "c"}]

    field_names = ["x", "y"]
    field_types = [pa.int32(), pa.string()]
    additional_columns1 = {"a": [1, 2]}
    additional_columns2 = {"a": [3]}

    return _create_simple_struct_blocks(
        struct_data1,
        struct_data2,
        field_names,
        field_types,
        additional_columns1,
        additional_columns2,
    )


@pytest.fixture
def struct_mismatched_lengths_expected():
    """Fixture for struct with mismatched lengths expected results."""
    field_names = ["x", "y"]
    field_types = [pa.int32(), pa.string()]
    additional_fields = [("a", pa.int64())]

    schema = _create_simple_struct_schema(field_names, field_types, additional_fields)

    content = {
        "a": [1, 2, 3],
        "d": [
            {"x": 1, "y": "a"},
            {"x": 2, "y": "b"},
            {"x": 3, "y": "c"},
        ],
    }

    return _create_struct_expected_result(schema, 3, content)


@pytest.fixture
def struct_empty_arrays_blocks():
    """Fixture for struct with empty arrays test data."""
    struct_data1 = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]

    # Define the second table with null struct value (empty arrays for fields)
    x_array = pa.array([None], type=pa.int32())
    y_array = pa.array([None], type=pa.string())

    # Create a struct array from null field arrays
    null_struct_array = pa.StructArray.from_arrays(
        [x_array, y_array],
        ["x", "y"],
        mask=pa.array([True]),
    )

    t1 = pa.table(
        {
            "a": [1, 2],
            "d": pa.array(
                struct_data1, type=pa.struct([("x", pa.int32()), ("y", pa.string())])
            ),
        }
    )

    t2 = pa.table({"a": [3], "d": null_struct_array})
    return [t1, t2]


@pytest.fixture
def struct_empty_arrays_expected():
    """Fixture for struct with empty arrays expected results."""
    field_names = ["x", "y"]
    field_types = [pa.int32(), pa.string()]
    additional_fields = [("a", pa.int64())]

    schema = _create_simple_struct_schema(field_names, field_types, additional_fields)

    content = {
        "a": [1, 2, 3],
        "d": [
            {"x": 1, "y": "a"},
            {"x": 2, "y": "b"},
            None,  # Entire struct is None, as PyArrow handles it
        ],
    }

    return _create_struct_expected_result(schema, 3, content)


@pytest.fixture
def unify_schemas_basic_schemas():
    """Fixture for basic unify schemas test data."""
    tensor_arr_1 = pa.schema([("tensor_arr", ArrowTensorType((3, 5), pa.int32()))])
    tensor_arr_2 = pa.schema([("tensor_arr", ArrowTensorType((2, 1), pa.int32()))])
    tensor_arr_3 = pa.schema([("tensor_arr", ArrowTensorType((3, 5), pa.int32()))])
    var_tensor_arr = pa.schema(
        [
            ("tensor_arr", ArrowVariableShapedTensorType(pa.int32(), 2)),
        ]
    )
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
    return {
        "tensor_arr_1": tensor_arr_1,
        "tensor_arr_2": tensor_arr_2,
        "tensor_arr_3": tensor_arr_3,
        "var_tensor_arr": var_tensor_arr,
        "var_tensor_arr_1d": var_tensor_arr_1d,
        "var_tensor_arr_3d": var_tensor_arr_3d,
    }


@pytest.fixture
def unify_schemas_multicol_schemas():
    """Fixture for multi-column unify schemas test data."""
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
    multicol_schema_3 = pa.schema(
        [
            ("col_int", pa.int32()),
            ("col_fixed_tensor", ArrowVariableShapedTensorType(pa.int32(), 3)),
            ("col_var_tensor", ArrowVariableShapedTensorType(pa.int16(), 5)),
        ]
    )
    return {
        "multicol_schema_1": multicol_schema_1,
        "multicol_schema_2": multicol_schema_2,
        "multicol_schema_3": multicol_schema_3,
    }


@pytest.fixture
def object_concat_blocks():
    """Fixture for object concat test data."""
    obj = types.SimpleNamespace(a=1, b="test")
    t1 = pa.table({"a": [3, 4], "b": [7, 8]})
    t2 = pa.table({"a": ArrowPythonObjectArray.from_objects([obj, obj]), "b": [0, 1]})
    return [t1, t2]


@pytest.fixture
def object_concat_expected():
    """Fixture for object concat expected results."""
    obj = types.SimpleNamespace(a=1, b="test")
    return {
        "length": 4,
        "a_type": ArrowPythonObjectType,
        "b_type": pa.types.is_integer,
        "content": {"a": [3, 4, obj, obj], "b": [7, 8, 0, 1]},
    }


@pytest.fixture
def struct_variable_shaped_tensor_blocks():
    """Fixture for struct with variable shaped tensor test data."""
    # Create variable-shaped tensor data for the first table
    tensor_data1 = np.array(
        [
            np.ones((2, 2), dtype=np.float32),
            np.zeros((3, 3), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array1 = ArrowVariableShapedTensorArray.from_numpy(tensor_data1)

    # Create struct data with tensor field for the first table
    metadata_array1 = pa.array(["row1", "row2"])
    struct_array1 = pa.StructArray.from_arrays(
        [metadata_array1, tensor_array1], names=["metadata", "tensor"]
    )

    t1 = pa.table({"id": [1, 2], "struct_with_tensor": struct_array1})

    # Create variable-shaped tensor data for the second table
    tensor_data2 = np.array(
        [
            np.ones((1, 4), dtype=np.float32),
            np.zeros((2, 1), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)

    # Create struct data with tensor field for the second table
    metadata_array2 = pa.array(["row3", "row4"])
    struct_array2 = pa.StructArray.from_arrays(
        [metadata_array2, tensor_array2], names=["metadata", "tensor"]
    )

    t2 = pa.table({"id": [3, 4], "struct_with_tensor": struct_array2})
    return [t1, t2]


@pytest.fixture
def struct_variable_shaped_tensor_expected():
    """Fixture for struct with variable shaped tensor expected results."""
    return {
        "length": 4,
        "schema": pa.schema(
            [
                ("id", pa.int64()),
                (
                    "struct_with_tensor",
                    pa.struct(
                        [
                            ("metadata", pa.string()),
                            ("tensor", ArrowVariableShapedTensorType(pa.float32(), 2)),
                        ]
                    ),
                ),
            ]
        ),
        "content": {"id": [1, 2, 3, 4]},
    }


@pytest.fixture
def unify_schemas_object_types_schemas():
    """Fixture for object types unify schemas test data."""
    from ray.air.util.object_extensions.arrow import ArrowPythonObjectType

    schema1 = pa.schema([("obj_col", ArrowPythonObjectType())])
    schema2 = pa.schema([("obj_col", pa.int32())])
    schema3 = pa.schema([("obj_col", pa.float64())])
    expected = pa.schema([("obj_col", ArrowPythonObjectType())])

    return {
        "object_schema": schema1,
        "int_schema": schema2,
        "float_schema": schema3,
        "expected": expected,
    }


@pytest.fixture
def unify_schemas_incompatible_tensor_schemas():
    """Fixture for incompatible tensor dtypes unify schemas test data."""
    schema1 = pa.schema([("tensor", ArrowTensorType((2, 2), pa.int32()))])
    schema2 = pa.schema([("tensor", ArrowTensorType((2, 2), pa.float32()))])
    return [schema1, schema2]


@pytest.fixture
def unify_schemas_objects_and_tensors_schemas():
    """Fixture for objects and tensors unify schemas test data."""
    from ray.air.util.object_extensions.arrow import ArrowPythonObjectType

    schema1 = pa.schema([("col", ArrowPythonObjectType())])
    schema2 = pa.schema([("col", ArrowTensorType((2, 2), pa.int32()))])
    return [schema1, schema2]


@pytest.fixture
def unify_schemas_missing_tensor_fields_schemas():
    """Fixture for missing tensor fields unify schemas test data."""
    schema1 = pa.schema(
        [
            (
                "struct",
                pa.struct(
                    [
                        ("tensor", ArrowTensorType((2, 2), pa.int32())),
                        ("value", pa.int64()),
                    ]
                ),
            )
        ]
    )
    schema2 = pa.schema(
        [("struct", pa.struct([("value", pa.int64())]))]  # Missing tensor field
    )
    expected = pa.schema(
        [
            (
                "struct",
                pa.struct(
                    [
                        ("tensor", ArrowTensorType((2, 2), pa.int32())),
                        ("value", pa.int64()),
                    ]
                ),
            )
        ]
    )
    return {"with_tensor": schema1, "without_tensor": schema2, "expected": expected}


@pytest.fixture
def unify_schemas_nested_struct_tensors_schemas():
    """Fixture for nested struct tensors unify schemas test data."""
    schema1 = pa.schema(
        [
            (
                "outer",
                pa.struct(
                    [
                        (
                            "inner",
                            pa.struct(
                                [
                                    ("tensor", ArrowTensorType((3, 3), pa.float32())),
                                    ("data", pa.string()),
                                ]
                            ),
                        ),
                        ("id", pa.int64()),
                    ]
                ),
            )
        ]
    )
    schema2 = pa.schema(
        [
            (
                "outer",
                pa.struct(
                    [
                        (
                            "inner",
                            pa.struct([("data", pa.string())]),  # Missing tensor field
                        ),
                        ("id", pa.int64()),
                    ]
                ),
            )
        ]
    )
    expected = pa.schema(
        [
            (
                "outer",
                pa.struct(
                    [
                        (
                            "inner",
                            pa.struct(
                                [
                                    (
                                        "tensor",
                                        ArrowTensorType((3, 3), pa.float32()),
                                    ),
                                    ("data", pa.string()),
                                ]
                            ),
                        ),
                        ("id", pa.int64()),
                    ]
                ),
            )
        ]
    )
    return {"with_tensor": schema1, "without_tensor": schema2, "expected": expected}


@pytest.fixture
def object_with_tensor_fails_blocks():
    """Blocks that should fail when concatenating objects with tensors."""
    obj = types.SimpleNamespace(a=1, b="test")
    t1 = pa.table({"a": ArrowPythonObjectArray.from_objects([obj, obj])})
    # Create tensor array with proper extension type
    tensor_array = ArrowTensorArray.from_numpy(np.array([[1, 2], [3, 4]]))
    t2 = pa.table({"a": tensor_array})
    return [t1, t2]


@pytest.fixture
def simple_concat_data():
    """Test data for simple concat operations."""
    return {"empty": [], "single_block": pa.table({"a": [1, 2]})}


# Helper function for creating tensor arrays
def _create_tensor_array(data, tensor_type="fixed"):
    """Helper function to create tensor arrays with consistent patterns."""
    if tensor_type == "fixed":
        return ArrowTensorArray.from_numpy(data)
    elif tensor_type == "variable":
        return ArrowVariableShapedTensorArray.from_numpy(data)
    else:
        raise ValueError(f"Unknown tensor type: {tensor_type}")


# Helper function for creating expected results
def _create_expected_result(schema, length, **kwargs):
    """Helper function to create expected result dictionaries."""
    result = {"schema": schema, "length": length}
    result.update(kwargs)
    return result


# Helper function for creating tensor blocks
def _create_tensor_blocks(
    tensor_data1,
    tensor_data2,
    tensor_type1="fixed",
    tensor_type2="variable",
    id_data1=None,
    id_data2=None,
    column_name="tensor",
):
    """Helper function to create tensor blocks with consistent patterns."""
    if id_data1 is None:
        id_data1 = [1, 2]
    if id_data2 is None:
        id_data2 = [3, 4]

    tensor_array1 = _create_tensor_array(tensor_data1, tensor_type1)
    tensor_array2 = _create_tensor_array(tensor_data2, tensor_type2)

    t1 = pa.table({"id": id_data1, column_name: tensor_array1})
    t2 = pa.table({"id": id_data2, column_name: tensor_array2})

    return t1, t2


# Helper function for creating struct blocks with tensors
def _create_struct_tensor_blocks(
    tensor_data1,
    tensor_data2,
    tensor_type1="fixed",
    tensor_type2="variable",
    value_data1=None,
    value_data2=None,
    extra_data2=None,
    struct_name="struct",
    id_data1=None,
    id_data2=None,
):
    """Helper function to create struct blocks with tensor fields."""
    if value_data1 is None:
        value_data1 = [1, 2]
    if value_data2 is None:
        value_data2 = [3, 4]
    if id_data1 is None:
        id_data1 = [1, 2]
    if id_data2 is None:
        id_data2 = [3, 4]

    tensor_array1 = _create_tensor_array(tensor_data1, tensor_type1)
    tensor_array2 = _create_tensor_array(tensor_data2, tensor_type2)

    value_array1 = pa.array(value_data1, type=pa.int64())
    value_array2 = pa.array(value_data2, type=pa.int64())

    if extra_data2 is not None:
        extra_array2 = pa.array(extra_data2, type=pa.string())
        struct_array1 = pa.StructArray.from_arrays(
            [tensor_array1, value_array1], names=["tensor", "value"]
        )
        struct_array2 = pa.StructArray.from_arrays(
            [tensor_array2, value_array2, extra_array2],
            names=["tensor", "value", "extra"],
        )
    else:
        struct_array1 = pa.StructArray.from_arrays(
            [tensor_array1, value_array1], names=["tensor", "value"]
        )
        struct_array2 = pa.StructArray.from_arrays(
            [tensor_array2, value_array2], names=["tensor", "value"]
        )

    t1 = pa.table({"id": id_data1, struct_name: struct_array1})
    t2 = pa.table({"id": id_data2, struct_name: struct_array2})

    return t1, t2


# Helper function for creating expected tensor schemas
def _create_tensor_schema(
    tensor_type=ArrowVariableShapedTensorType,
    dtype=pa.float32(),
    ndim=2,
    include_id=True,
    struct_name="struct",
    include_extra=False,
):
    """Helper function to create expected tensor schemas."""
    fields = []
    if include_id:
        fields.append(("id", pa.int64()))

    if struct_name == "struct":
        struct_fields = [
            ("tensor", tensor_type(dtype, ndim)),
            ("value", pa.int64()),
        ]
        if include_extra:
            struct_fields.append(("extra", pa.string()))
        fields.append((struct_name, pa.struct(struct_fields)))
    else:
        fields.append(("tensor", tensor_type(dtype, ndim)))

    return pa.schema(fields)


# Helper function for creating basic struct blocks
def _create_basic_struct_blocks(
    struct_data1,
    struct_data2,
    column_name="struct",
    id_data1=None,
    id_data2=None,
    other_columns=None,
):
    """Helper function to create basic struct blocks."""
    struct_array1 = pa.array(struct_data1)
    struct_array2 = pa.array(struct_data2)

    t1_data = {column_name: struct_array1}
    t2_data = {column_name: struct_array2}

    # Only add id columns if they are provided
    if id_data1 is not None:
        t1_data["id"] = id_data1
    if id_data2 is not None:
        t2_data["id"] = id_data2

    if other_columns:
        t1_data.update(other_columns.get("t1", {}))
        t2_data.update(other_columns.get("t2", {}))

    t1 = pa.table(t1_data)
    t2 = pa.table(t2_data)

    return t1, t2


# Helper function for creating struct schemas
def _create_struct_schema(struct_fields, include_id=True, other_fields=None):
    """Helper function to create struct schemas."""
    fields = []
    if include_id:
        fields.append(("id", pa.int64()))

    fields.append(("struct", pa.struct(struct_fields)))

    if other_fields:
        fields.extend(other_fields)

    return pa.schema(fields)


# Helper function for creating struct blocks with additional columns
def _create_struct_blocks_with_columns(
    struct_data1,
    struct_data2,
    struct_type1,
    struct_type2,
    additional_columns1=None,
    additional_columns2=None,
    struct_column="d",
):
    """Helper function to create struct blocks with additional columns."""
    t1_data = {}
    t2_data = {}

    # Add additional columns first to maintain expected order
    if additional_columns1:
        t1_data.update(additional_columns1)
    if additional_columns2:
        t2_data.update(additional_columns2)

    # Add struct column
    t1_data[struct_column] = pa.array(struct_data1, type=struct_type1)
    t2_data[struct_column] = pa.array(struct_data2, type=struct_type2)

    t1 = pa.table(t1_data)
    t2 = pa.table(t2_data)

    return t1, t2


# Helper function for creating expected results for struct tests
def _create_struct_expected_result(schema, length, content):
    """Helper function to create expected results for struct tests."""
    return {
        "length": length,
        "schema": schema,
        "content": content,
    }


# Helper function for creating struct blocks with simple field patterns
def _create_simple_struct_blocks(
    struct_data1,
    struct_data2,
    field_names,
    field_types,
    additional_columns1=None,
    additional_columns2=None,
    struct_column="d",
):
    """Helper function to create struct blocks with simple field patterns."""
    struct_type = pa.struct(list(zip(field_names, field_types)))

    return _create_struct_blocks_with_columns(
        struct_data1,
        struct_data2,
        struct_type,
        struct_type,
        additional_columns1,
        additional_columns2,
        struct_column,
    )


# Helper function for creating simple struct schemas
def _create_simple_struct_schema(field_names, field_types, additional_fields=None):
    """Helper function to create simple struct schemas."""
    struct_fields = list(zip(field_names, field_types))

    fields = []
    if additional_fields:
        fields.extend(additional_fields)
    fields.append(("d", pa.struct(struct_fields)))

    return pa.schema(fields)


@pytest.fixture
def unify_schemas_edge_cases_data():
    """Test data for unify schemas edge cases."""
    return {
        "empty_schemas": [],
        "single_schema": pa.schema([("col", pa.int32())]),
        "no_common_columns": {
            "schema1": pa.schema([("col1", pa.int32())]),
            "schema2": pa.schema([("col2", pa.string())]),
            "expected": pa.schema([("col1", pa.int32()), ("col2", pa.string())]),
        },
        "all_null_schemas": {
            "schema1": pa.schema([("col", pa.null())]),
            "schema2": pa.schema([("col", pa.null())]),
        },
    }


@pytest.fixture
def unify_schemas_mixed_tensor_data():
    """Test data for mixed tensor types in unify schemas."""
    return {
        "fixed_shape": pa.schema([("tensor", ArrowTensorType((2, 2), pa.int32()))]),
        "variable_shaped": pa.schema(
            [("tensor", ArrowVariableShapedTensorType(pa.int32(), 2))]
        ),
        "different_shape": pa.schema([("tensor", ArrowTensorType((3, 3), pa.int32()))]),
        "expected_variable": pa.schema(
            [("tensor", ArrowVariableShapedTensorType(pa.int32(), 2))]
        ),
    }


@pytest.fixture
def unify_schemas_type_promotion_data():
    """Test data for type promotion scenarios."""
    return {
        "non_null": pa.schema([pa.field("A", pa.int32())]),
        "nullable": pa.schema([pa.field("A", pa.int32(), nullable=True)]),
        "int64": pa.schema([pa.field("A", pa.int64())]),
        "float64": pa.schema([pa.field("A", pa.float64())]),
    }


@pytest.fixture
def block_select_data():
    """Test data for block select operations."""
    df = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13], "three": [14, 15, 16]})
    table = pa.Table.from_pandas(df)
    return {
        "table": table,
        "df": df,
        "single_column": {
            "columns": ["two"],
            "expected_schema": pa.schema([("two", pa.int64())]),
        },
        "multiple_columns": {
            "columns": ["two", "one"],
            "expected_schema": pa.schema([("two", pa.int64()), ("one", pa.int64())]),
        },
    }


@pytest.fixture
def block_slice_data():
    """Test data for block slice operations."""
    n = 20
    df = pd.DataFrame(
        {"one": list(range(n)), "two": ["a"] * n, "three": [np.nan] + [1.5] * (n - 1)}
    )
    table = pa.Table.from_pandas(df)
    empty_df = pd.DataFrame({"one": []})
    empty_table = pa.Table.from_pandas(empty_df)
    return {
        "normal": {"table": table, "df": df, "slice_params": {"a": 5, "b": 10}},
        "empty": {"table": empty_table, "slice_params": {"a": 0, "b": 0}},
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
