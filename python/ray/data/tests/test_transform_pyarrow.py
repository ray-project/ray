import numpy as np
import pyarrow as pa
import pytest

from ray.data.extensions import (
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorType,
)
from ray.data._internal.arrow_ops.transform_pyarrow import concat


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
    np.testing.assert_array_equal(out["a"].chunk(0).to_numpy(), a1)
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
    np.testing.assert_array_equal(out["a"].chunk(0).to_numpy(), a1)
    np.testing.assert_array_equal(out["a"].chunk(1).to_numpy(), a2)
    # NOTE: We don't check equivalence with pyarrow.concat_tables since it currently
    # fails for this case.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
