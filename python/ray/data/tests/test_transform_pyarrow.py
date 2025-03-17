import os
import types

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version
from ray._private.arrow_utils import get_pyarrow_version

import ray
from ray.air.util.tensor_extensions.arrow import ArrowTensorTypeV2
from ray.data import DataContext
from ray.data._internal.arrow_ops.transform_pyarrow import (
    concat,
    try_combine_chunked_columns,
    unify_schemas,
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
    shuffle,
)
from ray.data.block import BlockAccessor
from ray.data.extensions import (
    ArrowConversionError,
    ArrowPythonObjectArray,
    ArrowPythonObjectType,
    ArrowTensorArray,
    ArrowTensorType,
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
    if DataContext.get_current().use_arrow_tensor_v2:
        tensor_type = ArrowTensorTypeV2
    else:
        tensor_type = ArrowTensorType

    assert out.column_names == ["a"]
    assert out.schema.types == [tensor_type((2, 2), pa.int64())]

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


def test_arrow_concat_with_objects():
    obj = types.SimpleNamespace(a=1, b="test")
    t1 = pa.table({"a": [3, 4], "b": [7, 8]})
    t2 = pa.table({"a": ArrowPythonObjectArray.from_objects([obj, obj]), "b": [0, 1]})
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4
    assert isinstance(t3.schema.field("a").type, ArrowPythonObjectType)
    assert pa.types.is_integer(t3.schema.field("b").type)
    assert t3.column("a").to_pylist() == [3, 4, obj, obj]
    assert t3.column("b").to_pylist() == [7, 8, 0, 1]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("17.0.0"),
    reason="Requires PyArrow version 17 or higher",
)
def test_struct_with_different_field_names():
    # Ensures that when concatenating tables with struct columns having different
    # field names, missing fields in each struct are filled with None in the
    # resulting table.

    t1 = pa.table(
        {
            "a": [1, 2],
            "d": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )

    t2 = pa.table(
        {
            "a": [3],
            "d": pa.array(
                [{"x": 3, "z": "c"}],
                type=pa.struct([("x", pa.int32()), ("z", pa.string())]),
            ),
        }
    )

    # Concatenate tables with different field names in struct
    t3 = concat([t1, t2])

    assert isinstance(t3, pa.Table)
    assert len(t3) == 3

    # Check the entire schema
    expected_schema = pa.schema(
        [
            ("a", pa.int64()),
            (
                "d",
                pa.struct(
                    [
                        ("x", pa.int32()),
                        ("y", pa.string()),
                        ("z", pa.string()),
                    ]
                ),
            ),
        ]
    )
    assert t3.schema == expected_schema

    # Check that missing fields are filled with None
    assert t3.column("a").to_pylist() == [1, 2, 3]
    assert t3.column("d").to_pylist() == [
        {"x": 1, "y": "a", "z": None},
        {"x": 2, "y": "b", "z": None},
        {"x": 3, "y": None, "z": "c"},
    ]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("17.0.0"),
    reason="Requires PyArrow version 17 or higher",
)
def test_nested_structs():
    # Checks that deeply nested structs (3 levels of nesting) are handled properly
    # during concatenation and the resulting table preserves the correct nesting
    # structure.

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

    # Concatenate tables with nested structs and missing fields
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 2

    # Validate the schema of the resulting table
    expected_schema = pa.schema(
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
    )
    assert t3.schema == expected_schema

    # Validate the data in the concatenated table
    assert t3.column("a").to_pylist() == [1, 2]
    assert t3.column("d").to_pylist() == [
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
    ]


def test_struct_with_null_values():
    # Ensures that when concatenating tables with struct columns containing null
    # values, the null values are properly handled, and the result reflects the
    # expected structure.

    # Define the first table with struct containing null values
    t1 = pa.table(
        {
            "a": [1, 2],
            "d": pa.array(
                [{"x": 1, "y": "a"}, None],  # Second row is null
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )

    # Define the second table with struct containing a null value
    t2 = pa.table(
        {
            "a": [3],
            "d": pa.array(
                [None],  # Entire struct is null
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )

    # Concatenate tables with struct columns containing null values
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 3

    # Validate the schema of the resulting table
    expected_schema = pa.schema(
        [
            ("a", pa.int64()),
            ("d", pa.struct([("x", pa.int32()), ("y", pa.string())])),
        ]
    )
    assert (
        t3.schema == expected_schema
    ), f"Expected schema: {expected_schema}, but got {t3.schema}"

    # Verify the PyArrow table content
    assert t3.column("a").to_pylist() == [1, 2, 3]

    # Adjust expected to match the format of the actual result
    expected = [
        {"x": 1, "y": "a"},
        None,  # Entire struct is None, not {"x": None, "y": None}
        None,  # Entire struct is None, not {"x": None, "y": None}
    ]

    result = t3.column("d").to_pylist()
    assert result == expected, f"Expected {expected}, but got {result}"


def test_struct_with_mismatched_lengths():
    # Verifies that when concatenating tables with struct columns of different lengths,
    # the missing values are properly padded with None in the resulting table.
    # Define the first table with 2 rows and a struct column
    t1 = pa.table(
        {
            "a": [1, 2],
            "d": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )

    # Define the second table with 1 row and a struct column
    t2 = pa.table(
        {
            "a": [3],
            "d": pa.array(
                [{"x": 3, "y": "c"}],
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )

    # Concatenate tables with struct columns of different lengths
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 3  # Check that the resulting table has the correct number of rows

    # Validate the schema of the resulting table
    expected_schema = pa.schema(
        [
            ("a", pa.int64()),
            ("d", pa.struct([("x", pa.int32()), ("y", pa.string())])),
        ]
    )
    assert (
        t3.schema == expected_schema
    ), f"Expected schema: {expected_schema}, but got {t3.schema}"

    # Verify the content of the resulting table
    assert t3.column("a").to_pylist() == [1, 2, 3]
    expected = [
        {"x": 1, "y": "a"},
        {"x": 2, "y": "b"},
        {"x": 3, "y": "c"},
    ]
    result = t3.column("d").to_pylist()

    assert result == expected, f"Expected {expected}, but got {result}"


def test_struct_with_empty_arrays():
    # Checks the behavior when concatenating tables with structs containing empty
    # arrays, verifying that null structs are correctly handled.

    # Define the first table with valid struct data
    t1 = pa.table(
        {
            "a": [1, 2],
            "d": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
                type=pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )

    # Define the second table with null struct value (empty arrays for fields)
    x_array = pa.array([None], type=pa.int32())
    y_array = pa.array([None], type=pa.string())

    # Create a struct array from null field arrays
    null_struct_array = pa.StructArray.from_arrays(
        [x_array, y_array],
        ["x", "y"],
        mask=pa.array([True]),
    )

    t2 = pa.table({"a": [3], "d": null_struct_array})

    # Concatenate tables with struct columns containing null values
    t3 = concat([t1, t2])

    # Verify that the concatenated result is a valid PyArrow Table
    assert isinstance(t3, pa.Table)
    assert len(t3) == 3  # Check that the concatenated table has 3 rows

    # Validate the schema of the resulting concatenated table
    expected_schema = pa.schema(
        [
            ("a", pa.int64()),  # Assuming 'a' is an integer column
            (
                "d",
                pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),  # Struct column 'd'
        ]
    )
    assert (
        t3.schema == expected_schema
    ), f"Expected schema: {expected_schema}, but got {t3.schema}"

    # Verify the content of the concatenated table
    assert t3.column("a").to_pylist() == [1, 2, 3]
    expected = [
        {"x": 1, "y": "a"},
        {"x": 2, "y": "b"},
        None,  # Entire struct is None, as PyArrow handles it
    ]
    result = t3.column("d").to_pylist()

    assert result == expected, f"Expected {expected}, but got {result}"


def test_arrow_concat_object_with_tensor_fails():
    obj = types.SimpleNamespace(a=1, b="test")
    t1 = pa.table({"a": ArrowPythonObjectArray.from_objects([obj, obj]), "b": [0, 1]})
    t2 = pa.table(
        {"a": ArrowTensorArray.from_numpy([np.zeros((10, 10))] * 2), "b": [7, 8]}
    )
    with pytest.raises(ArrowConversionError) as exc_info:
        concat([t1, t2])
    assert "objects and tensors" in str(exc_info.value.__cause__)


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


@pytest.mark.skipif(
    get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION,
    reason="Requires Arrow version of at least 14.0.0",
)
def test_unify_schemas_type_promotion():
    s_non_null = pa.schema(
        [
            pa.field("A", pa.int32()),
        ]
    )

    s_nullable = pa.schema(
        [
            pa.field("A", pa.int32(), nullable=True),
        ]
    )

    # No type promotion
    assert (
        unify_schemas(
            [s_non_null, s_nullable],
            promote_types=False,
        )
        == s_nullable
    )

    s1 = pa.schema(
        [
            pa.field("A", pa.int64()),
        ]
    )

    s2 = pa.schema(
        [
            pa.field("A", pa.float64()),
        ]
    )

    # No type promotion
    with pytest.raises(pa.lib.ArrowTypeError) as exc_info:
        unify_schemas(
            [s1, s2],
            promote_types=False,
        )

    assert "Unable to merge: Field A has incompatible types: int64 vs double" == str(
        exc_info.value
    )

    # Type promoted
    assert (
        unify_schemas(
            [s1, s2],
            promote_types=True,
        )
        == s2
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
    #
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
