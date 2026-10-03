import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.tensor_extensions import chunked_tensor_take
from ray.data.extensions import (
    ArrowPythonObjectArray,
    ArrowTensorType,
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorArray,
    take_table,
)


def _array(kind, rows=12, width=2):
    if kind == "ordinary":
        return pa.array([None if i % 3 == 0 else str(i) for i in range(rows)])
    if kind == "object":
        return ArrowPythonObjectArray.from_objects([{i, i + 1} for i in range(rows)])
    if kind == "variable":
        return ArrowVariableShapedTensorArray.from_numpy(
            [np.full((width + i % 2, 2), i, dtype=np.float32) for i in range(rows)]
        )
    tensor_cls = ArrowTensorType if kind == "fixed_v1" else ArrowTensorTypeV2
    tensor_type = tensor_cls((width,), pa.float32())
    values = np.arange(rows * width, dtype=np.float32).reshape(rows, width)
    return tensor_type.wrap_array(
        pa.array(values.tolist(), type=tensor_type.storage_type)
    )


def _table(array):
    midpoint = len(array) // 2
    column = pa.chunked_array(
        [array.slice(0, 0), array.slice(0, midpoint), array.slice(midpoint)]
    )
    schema = pa.schema(
        [
            pa.field("value", array.type, metadata={b"field": b"value"}),
            pa.field("id", pa.int64()),
        ],
        metadata={b"table": b"value"},
    )
    return pa.Table.from_arrays([column, pa.array(range(len(array)))], schema=schema)


def _serialize(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


@pytest.mark.parametrize(
    "kind", ["ordinary", "fixed_v1", "fixed_v2", "variable", "object"]
)
@pytest.mark.parametrize(
    "index_format,indices",
    [("list", [11, 0, 6, 11, 5])]
    + [
        (index_format, indices)
        for index_format in ["numpy", "arrow", "chunked_arrow"]
        for indices in [[11, 0, 6, 11, 5], []]
    ],
)
def test_public_take_table_contract(kind, indices, index_format):
    array = _array(kind)
    table = _table(array)
    original = _serialize(table)
    expected = pa.Table.from_arrays(
        [
            array.take(pa.array(indices, type=pa.int64())),
            pa.array(indices, type=pa.int64()),
        ],
        schema=table.schema,
    )
    if index_format == "numpy":
        indices = np.asarray(indices, dtype=np.int64)
    elif index_format == "arrow":
        indices = pa.array(indices, type=pa.int32())
    elif index_format == "chunked_arrow":
        indices = pa.chunked_array([indices[:2], [], indices[2:]], type=pa.int64())

    result = take_table(table, indices)

    result.validate(full=True)
    assert result.equals(expected, check_metadata=True)
    assert _serialize(table).equals(original)


@pytest.mark.parametrize("kind", ["fixed_v1", "fixed_v2", "variable"])
def test_public_take_table_uses_existing_tensor_paths(kind, monkeypatch):
    # Exceed the production size gates without changing them.
    array = _array(kind, rows=128, width=16384 if kind == "variable" else 4096)
    table = _table(array)
    indices = [127, 0, 64, 127]
    expected = pa.Table.from_arrays(
        [array.take(indices), pa.array(indices, type=pa.int64())], schema=table.schema
    )
    plan_cls = (
        chunked_tensor_take.PreparedVariableShapedTensorTake
        if kind == "variable"
        else chunked_tensor_take.PreparedFixedShapedTensorTake
    )
    calls = []
    original_take = plan_cls.take

    def record_take(plan, normalized_indices):
        calls.append(normalized_indices.copy())
        return original_take(plan, normalized_indices)

    monkeypatch.setattr(plan_cls, "take", record_take)
    result = take_table(table, indices)
    assert len(calls) == 1
    np.testing.assert_array_equal(calls[0], indices)
    assert result.equals(expected, check_metadata=True)

    monkeypatch.setattr(chunked_tensor_take, "ENABLE_CHUNKED_TENSOR_TAKE", False)
    fallback = take_table(table, indices)
    assert len(calls) == 1
    assert fallback.equals(expected, check_metadata=True)


@pytest.mark.parametrize("indices", [[], [None, 2], [-1], [3], [1.5]])
def test_public_take_table_ordinary_arrow_validation(indices):
    table = pa.table({"value": [10, 20, 30]})
    try:
        expected = table.take(indices)
    except (pa.ArrowException, TypeError, ValueError) as error:
        with pytest.raises(type(error)):
            take_table(table, indices)
    else:
        assert take_table(table, indices).equals(expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
