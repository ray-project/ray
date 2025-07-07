import os
import types
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.air.util.tensor_extensions.arrow import ArrowTensorTypeV2
from ray.data import DataContext
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
    concat,
    hash_partition,
    shuffle,
    try_combine_chunked_columns,
    unify_schemas,
)
from ray.data.block import BlockAccessor
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

    assert len(_structs_partition_dict) == 34
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


def test_arrow_concat_empty():
    # Test empty.
    assert concat([]) == pa.table([])


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


def test_struct_with_arrow_variable_shaped_tensor_type():
    # Test concatenating tables with struct columns containing ArrowVariableShapedTensorType
    # fields, ensuring proper handling of variable-shaped tensors within structs.

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

    # Concatenate tables with struct columns containing variable-shaped tensors
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Validate the schema of the resulting table
    expected_schema = pa.schema(
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
    )
    assert (
        t3.schema == expected_schema
    ), f"Expected schema: {expected_schema}, but got {t3.schema}"

    # Verify the content of the resulting table
    assert t3.column("id").to_pylist() == [1, 2, 3, 4]

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


def test_mixed_tensor_types_same_dtype():
    """Test mixed tensor types with same data type but different shapes."""

    # Block 1: Fixed shape tensors with float32
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)

    t1 = pa.table({"id": [1, 2], "tensor": tensor_array1})

    # Block 2: Variable shape tensors with float32
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)

    t2 = pa.table({"id": [3, 4], "tensor": tensor_array2})

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Verify schema - should have tensor field as variable-shaped
    tensor_field = t3.schema.field("tensor")
    assert isinstance(tensor_field.type, ArrowVariableShapedTensorType)

    # Verify content
    result_tensors = t3.column("tensor").to_pylist()
    assert len(result_tensors) == 4

    # First 2 should be converted to variable-shaped tensors
    assert isinstance(result_tensors[0], np.ndarray)
    assert result_tensors[0].shape == (2,)
    assert result_tensors[0].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[0], np.ones((2,), dtype=np.float32))

    assert isinstance(result_tensors[1], np.ndarray)
    assert result_tensors[1].shape == (2,)
    assert result_tensors[1].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[1], np.ones((2,), dtype=np.float32))

    # Last 2 should be variable-shaped tensors
    assert isinstance(result_tensors[2], np.ndarray)
    assert result_tensors[2].shape == (3, 3)
    assert result_tensors[2].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[2], np.ones((3, 3), dtype=np.float32))

    assert isinstance(result_tensors[3], np.ndarray)
    assert result_tensors[3].shape == (1, 4)
    assert result_tensors[3].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[3], np.zeros((1, 4), dtype=np.float32))


def test_mixed_tensor_types_fixed_shape_different():
    """Test mixed tensor types with different fixed shapes."""

    # Block 1: Fixed shape tensors (2x2)
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)

    t1 = pa.table({"id": [1, 2], "tensor": tensor_array1})

    # Block 2: Fixed shape tensors (3x3)
    tensor_data2 = np.zeros((3, 3), dtype=np.float32)
    tensor_array2 = ArrowTensorArray.from_numpy(tensor_data2)

    t2 = pa.table(
        {"id": [3, 4, 5], "tensor": tensor_array2}  # Match length of tensor_array2
    )

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 5

    # Verify schema - should have tensor field as variable-shaped
    tensor_field = t3.schema.field("tensor")
    assert isinstance(tensor_field.type, ArrowVariableShapedTensorType)

    # Verify content
    result_tensors = t3.column("tensor").to_pylist()
    assert len(result_tensors) == 5

    # First 2 should be converted to variable-shaped tensors
    assert isinstance(result_tensors[0], np.ndarray)
    assert result_tensors[0].shape == (2,)
    assert result_tensors[0].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[0], np.ones((2,), dtype=np.float32))

    assert isinstance(result_tensors[1], np.ndarray)
    assert result_tensors[1].shape == (2,)
    assert result_tensors[1].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[1], np.ones((2,), dtype=np.float32))

    # Last 3 should be variable-shaped tensors
    assert isinstance(result_tensors[2], np.ndarray)
    assert result_tensors[2].shape == (3,)
    assert result_tensors[2].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[2], np.zeros((3,), dtype=np.float32))

    assert isinstance(result_tensors[3], np.ndarray)
    assert result_tensors[3].shape == (3,)
    assert result_tensors[3].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[3], np.zeros((3,), dtype=np.float32))

    assert isinstance(result_tensors[4], np.ndarray)
    assert result_tensors[4].shape == (3,)
    assert result_tensors[4].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[4], np.zeros((3,), dtype=np.float32))


def test_mixed_tensor_types_variable_shaped():
    """Test mixed tensor types with variable-shaped tensors."""

    # Block 1: Variable shape tensors
    tensor_data1 = np.array(
        [
            np.ones((2, 2), dtype=np.float32),
            np.zeros((3, 3), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array1 = ArrowVariableShapedTensorArray.from_numpy(tensor_data1)

    t1 = pa.table({"id": [1, 2], "tensor": tensor_array1})

    # Block 2: Variable shape tensors with different shapes
    tensor_data2 = np.array(
        [
            np.ones((1, 4), dtype=np.float32),
            np.zeros((2, 1), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)

    t2 = pa.table({"id": [3, 4], "tensor": tensor_array2})

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Verify schema - should have tensor field as variable-shaped
    tensor_field = t3.schema.field("tensor")
    assert isinstance(tensor_field.type, ArrowVariableShapedTensorType)

    # Verify content
    result_tensors = t3.column("tensor").to_pylist()
    assert len(result_tensors) == 4

    # All should be variable-shaped tensors
    assert isinstance(result_tensors[0], np.ndarray)
    assert result_tensors[0].shape == (2, 2)
    assert result_tensors[0].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[0], np.ones((2, 2), dtype=np.float32))

    assert isinstance(result_tensors[1], np.ndarray)
    assert result_tensors[1].shape == (3, 3)
    assert result_tensors[1].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[1], np.zeros((3, 3), dtype=np.float32))

    assert isinstance(result_tensors[2], np.ndarray)
    assert result_tensors[2].shape == (1, 4)
    assert result_tensors[2].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[2], np.ones((1, 4), dtype=np.float32))

    assert isinstance(result_tensors[3], np.ndarray)
    assert result_tensors[3].shape == (2, 1)
    assert result_tensors[3].dtype == np.float32
    np.testing.assert_array_equal(result_tensors[3], np.zeros((2, 1), dtype=np.float32))


def test_mixed_tensor_types_in_struct():
    """Test that the fix works for mixed tensor types in structs."""
    import numpy as np
    import pyarrow as pa

    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    # Block 1: Struct with fixed-shape tensor
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)
    value_array1 = pa.array([1, 2], type=pa.int64())

    struct_array1 = pa.StructArray.from_arrays(
        [tensor_array1, value_array1], names=["tensor", "value"]
    )

    t1 = pa.table({"id": [1, 2], "struct": struct_array1})

    # Block 2: Struct with variable-shaped tensor
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)
    value_array2 = pa.array([3, 4], type=pa.int64())

    struct_array2 = pa.StructArray.from_arrays(
        [tensor_array2, value_array2], names=["tensor", "value"]
    )

    t2 = pa.table({"id": [3, 4], "struct": struct_array2})

    # This should work with our fix
    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Verify the result has the expected structure
    assert "id" in t3.column_names
    assert "struct" in t3.column_names

    # Verify struct field contains both types of tensors
    struct_data = t3.column("struct").to_pylist()
    assert len(struct_data) == 4

    # First two should be from the fixed-shape tensor struct
    assert struct_data[0]["value"] == 1
    assert struct_data[1]["value"] == 2

    # Last two should be from the variable-shaped tensor struct
    assert struct_data[2]["value"] == 3
    assert struct_data[3]["value"] == 4


def test_nested_struct_with_mixed_tensor_types():
    """Test nested structs with mixed tensor types at different levels."""
    import numpy as np
    import pyarrow as pa

    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    # Block 1: Nested struct with fixed-shape tensors
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)

    # Inner struct with fixed-shape tensor
    inner_struct1 = pa.StructArray.from_arrays(
        [tensor_array1, pa.array([10, 20], type=pa.int64())],
        names=["inner_tensor", "inner_value"],
    )

    # Outer struct with nested struct and fixed-shape tensor
    outer_tensor1 = ArrowTensorArray.from_numpy(np.zeros((2, 1), dtype=np.float32))
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
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)

    # Inner struct with variable-shaped tensor
    inner_struct2 = pa.StructArray.from_arrays(
        [tensor_array2, pa.array([30, 40], type=pa.int64())],
        names=["inner_tensor", "inner_value"],
    )

    # Outer struct with nested struct and variable-shaped tensor
    outer_tensor2 = ArrowVariableShapedTensorArray.from_numpy(
        np.array(
            [np.ones((2, 2), dtype=np.float32), np.zeros((1, 3), dtype=np.float32)],
            dtype=object,
        )
    )
    outer_struct2 = pa.StructArray.from_arrays(
        [inner_struct2, outer_tensor2, pa.array([3, 4], type=pa.int64())],
        names=["nested", "outer_tensor", "outer_value"],
    )

    t2 = pa.table({"id": [3, 4], "complex_struct": outer_struct2})

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Verify the result has the expected structure
    assert "id" in t3.column_names
    assert "complex_struct" in t3.column_names

    # Verify nested struct field contains both types of tensors
    struct_data = t3.column("complex_struct").to_pylist()
    assert len(struct_data) == 4

    # Check that nested structures are preserved
    assert "nested" in struct_data[0]
    assert "outer_tensor" in struct_data[0]
    assert "outer_value" in struct_data[0]
    assert "inner_tensor" in struct_data[0]["nested"]
    assert "inner_value" in struct_data[0]["nested"]


def test_multiple_tensor_fields_in_struct():
    """Test structs with multiple tensor fields of different types."""
    import numpy as np
    import pyarrow as pa

    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    # Block 1: Struct with multiple fixed-shape tensors
    tensor1_data = np.ones((2, 2), dtype=np.float32)
    tensor1_array = ArrowTensorArray.from_numpy(tensor1_data)

    tensor2_data = np.zeros((2, 3), dtype=np.int32)
    tensor2_array = ArrowTensorArray.from_numpy(tensor2_data)

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
    tensor1_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor1_data2)

    tensor2_data2 = np.array(
        [
            np.ones((2, 2), dtype=np.int32),
            np.zeros((3, 1), dtype=np.int32),
        ],
        dtype=object,
    )
    tensor2_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor2_data2)

    struct_array2 = pa.StructArray.from_arrays(
        [tensor1_array2, tensor2_array2, pa.array([3, 4], type=pa.int64())],
        names=["tensor1", "tensor2", "value"],
    )

    t2 = pa.table({"id": [3, 4], "multi_tensor_struct": struct_array2})

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Verify the result has the expected structure
    assert "id" in t3.column_names
    assert "multi_tensor_struct" in t3.column_names

    # Verify struct field contains both types of tensors
    struct_data = t3.column("multi_tensor_struct").to_pylist()
    assert len(struct_data) == 4

    # Check that all tensor fields are present
    for row in struct_data:
        assert "tensor1" in row
        assert "tensor2" in row
        assert "value" in row


def test_struct_with_incompatible_tensor_dtypes_fails():
    """Test that concatenating structs with incompatible tensor dtypes fails gracefully."""
    import numpy as np
    import pyarrow as pa
    import pytest

    from ray.air.util.tensor_extensions.arrow import ArrowConversionError
    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    # Block 1: Struct with float32 fixed-shape tensor
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)
    value_array1 = pa.array([1, 2], type=pa.int64())

    struct_array1 = pa.StructArray.from_arrays(
        [tensor_array1, value_array1], names=["tensor", "value"]
    )

    t1 = pa.table({"id": [1, 2], "struct": struct_array1})

    # Block 2: Struct with int64 variable-shaped tensor (different dtype)
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.int64),
            np.zeros((1, 4), dtype=np.int64),
        ],
        dtype=object,
    )
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)
    value_array2 = pa.array([3, 4], type=pa.int64())

    struct_array2 = pa.StructArray.from_arrays(
        [tensor_array2, value_array2], names=["tensor", "value"]
    )

    t2 = pa.table({"id": [3, 4], "struct": struct_array2})

    # This should fail because of incompatible tensor dtypes
    with pytest.raises(ArrowConversionError):
        concat([t1, t2])


def test_struct_with_additional_fields():
    """Test structs where some blocks have additional fields."""
    import numpy as np
    import pyarrow as pa

    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    # Block 1: Struct with tensor field and basic fields
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)
    value_array1 = pa.array([1, 2], type=pa.int64())

    struct_array1 = pa.StructArray.from_arrays(
        [tensor_array1, value_array1], names=["tensor", "value"]
    )

    t1 = pa.table({"id": [1, 2], "struct": struct_array1})

    # Block 2: Struct with tensor field and additional fields
    tensor_data2 = np.array(
        [
            np.ones((3, 3), dtype=np.float32),
            np.zeros((1, 4), dtype=np.float32),
        ],
        dtype=object,
    )
    tensor_array2 = ArrowVariableShapedTensorArray.from_numpy(tensor_data2)
    value_array2 = pa.array([3, 4], type=pa.int64())
    extra_array2 = pa.array(["a", "b"], type=pa.string())

    struct_array2 = pa.StructArray.from_arrays(
        [tensor_array2, value_array2, extra_array2], names=["tensor", "value", "extra"]
    )

    t2 = pa.table({"id": [3, 4], "struct": struct_array2})

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 4

    # Verify the result has the expected structure
    assert "id" in t3.column_names
    assert "struct" in t3.column_names

    # Verify struct field contains both types of tensors
    struct_data = t3.column("struct").to_pylist()
    assert len(struct_data) == 4

    # First two should have tensor field and extra field filled with None
    assert "tensor" in struct_data[0]
    assert "tensor" in struct_data[1]
    assert "value" in struct_data[0]
    assert "value" in struct_data[1]
    assert "extra" in struct_data[0]
    assert "extra" in struct_data[1]
    assert struct_data[0]["extra"] is None
    assert struct_data[1]["extra"] is None

    # Last two should have tensor field and extra field
    assert "tensor" in struct_data[2]
    assert "tensor" in struct_data[3]
    assert "value" in struct_data[2]
    assert "value" in struct_data[3]
    assert "extra" in struct_data[2]
    assert "extra" in struct_data[3]
    assert struct_data[2]["extra"] == "a"
    assert struct_data[3]["extra"] == "b"


def test_struct_with_null_tensor_values():
    """Test structs where some fields are missing and get filled with nulls."""
    import numpy as np
    import pyarrow as pa

    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorType

    # Block 1: Struct with tensor and value fields
    tensor_data1 = np.ones((2, 2), dtype=np.float32)
    tensor_array1 = ArrowTensorArray.from_numpy(tensor_data1)
    value_array1 = pa.array([1, 2], type=pa.int64())
    struct_array1 = pa.StructArray.from_arrays(
        [tensor_array1, value_array1], names=["tensor", "value"]
    )
    t1 = pa.table({"id": [1, 2], "struct": struct_array1})

    # Block 2: Struct with only tensor field (missing value field)
    tensor_data2 = np.ones((1, 2, 2), dtype=np.float32)
    tensor_array2 = ArrowTensorArray.from_numpy(tensor_data2)
    # Note: no value field in this struct
    struct_array2 = pa.StructArray.from_arrays([tensor_array2], names=["tensor"])
    t2 = pa.table({"id": [3], "struct": struct_array2})

    t3 = concat([t1, t2])
    assert isinstance(t3, pa.Table)
    assert len(t3) == 3

    # Validate schema - should have both fields
    expected_schema = pa.schema(
        [
            ("id", pa.int64()),
            (
                "struct",
                pa.struct(
                    [
                        ("tensor", ArrowVariableShapedTensorType(pa.float32(), 2)),
                        ("value", pa.int64()),
                    ]
                ),
            ),
        ]
    )
    assert (
        t3.schema == expected_schema
    ), f"Expected schema: {expected_schema}, got: {t3.schema}"

    # Validate result
    assert t3.column("id").to_pylist() == [1, 2, 3]
    struct_data = t3.column("struct").to_pylist()

    # First two should have both fields
    assert struct_data[0]["value"] == 1
    assert struct_data[1]["value"] == 2
    assert isinstance(struct_data[0]["tensor"], np.ndarray)
    assert isinstance(struct_data[1]["tensor"], np.ndarray)

    # Third should have tensor field but value field should be null
    assert struct_data[2]["value"] is None
    assert isinstance(struct_data[2]["tensor"], np.ndarray)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
