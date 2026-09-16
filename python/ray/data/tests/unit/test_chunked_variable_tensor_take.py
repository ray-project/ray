import gc

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal import batcher as batcher_module
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.tensor_extensions import chunked_tensor_take as take_module
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorArray,
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
)


def _column(rows=96, chunks=4, dtype=np.float32, width=4096):
    arrays = []
    for ids in np.array_split(np.arange(rows), chunks):
        values = [
            np.full((width * (1 + int(i) % 2), 4), int(i) % 100, dtype=dtype)
            for i in ids
        ]
        arrays.append(ArrowVariableShapedTensorArray.from_numpy(values))
    return pa.chunked_array(arrays)


def _expected(column, indices):
    if isinstance(indices, pa.ChunkedArray):
        indices = indices.combine_chunks()
    elif isinstance(indices, list):
        indices = pa.array(indices, type=pa.int64())
    return column.type.wrap_array(
        pa.concat_arrays([c.storage for c in column.chunks]).take(indices)
    )


@pytest.fixture
def take_calls(monkeypatch):
    calls = []
    original = take_module.PreparedVariableShapedTensorTake.take

    def record(plan, indices):
        result = original(plan, indices)
        calls.append(len(indices))
        return result

    monkeypatch.setattr(take_module.PreparedVariableShapedTensorTake, "take", record)
    return calls


@pytest.mark.parametrize(
    "dtype", [np.int8, np.uint16, np.int64, np.float16, np.float32, np.float64]
)
@pytest.mark.parametrize("indices", [[], [0], [95, 0, 95, 40, 7], list(range(96))])
def test_variable_take(dtype, indices, take_calls):
    column = _column(dtype=dtype, width=16384 // np.dtype(dtype).itemsize)
    expected = _expected(column, indices)
    plan = take_module.try_prepare_chunked_tensor_take(
        column, max_output_rows=len(indices)
    )
    assert isinstance(plan, take_module.PreparedVariableShapedTensorTake)
    output = plan.take(np.array(indices, dtype=np.int64))
    output.validate(full=True)
    assert output.type == expected.type
    assert output.storage.equals(expected.storage)
    take_calls.clear()
    schema = pa.schema(
        [pa.field("tensor", column.type, metadata={b"field": b"value"})],
        metadata={b"table": b"value"},
    )
    table = pa.Table.from_arrays([column], schema=schema)
    result = transform_pyarrow.take_table(table, np.asarray(indices, dtype=np.int64))
    assert take_calls == [len(indices)]
    assert result.schema.equals(schema, check_metadata=True)
    assert result.column(0).chunk(0).storage.equals(expected.storage)


@pytest.mark.parametrize("kind", ["list", "int32", "uint64", "arrow", "chunked"])
def test_variable_take_normalizes_once(kind, take_calls):
    indices = [95, 1, 1, 12]
    if kind in ("int32", "uint64"):
        indices = np.array(indices, dtype=kind)
    elif kind == "arrow":
        indices = pa.array(indices)
    elif kind == "chunked":
        indices = pa.chunked_array([indices[:2], indices[2:]])
    column = _column()
    result = transform_pyarrow.take_table(pa.table({"tensor": column}), indices)
    assert take_calls == [4]
    assert result.column(0).chunk(0).storage.equals(_expected(column, indices).storage)


def test_variable_take_slices_empty_chunks_zero_rows_and_lifetime(take_calls):
    arrays = []
    for start in (0, 12):
        tensors = [
            np.full((0 if i % 3 == 0 else 65536, 4), i, dtype=np.float32)
            for i in range(start, start + 12)
        ]
        arr = ArrowVariableShapedTensorArray.from_numpy(tensors)
        arrays.extend([arr.slice(0, 0), arr.slice(1, 10), arr.slice(0, 0)])
    column = pa.chunked_array(arrays)
    indices = [19, 2, 0, 2, 5]
    expected = _expected(column, indices)
    plan = take_module.try_prepare_chunked_tensor_take(column, max_output_rows=5)
    assert plan is not None
    # Source views survive releasing Arrow wrappers.
    del column, arrays, arr, tensors
    gc.collect()
    output = plan.take(np.array(indices, dtype=np.int64))
    del plan
    gc.collect()
    assert take_calls == [5]
    assert output.storage.equals(expected.storage)


@pytest.mark.parametrize(
    "failure",
    [
        "parent_null",
        "list_null",
        "shape_null",
        "value_null",
        "dimension_null",
        "negative_dimension",
        "wrong_shape",
        "wrong_rank",
        "descending_offsets",
    ],
)
def test_variable_take_rejects_unsafe_storage(failure, caplog, monkeypatch):
    column = _column()
    chunk = column.chunk(0)
    data, shape = chunk.storage.field("data"), chunk.storage.field("shape")
    mask = pa.array([True] + [False] * (len(chunk) - 1))
    if failure == "list_null":
        data = pa.LargeListArray.from_arrays(data.offsets, data.values, mask=mask)
    elif failure == "shape_null":
        shape = pa.ListArray.from_arrays(shape.offsets, shape.values, mask=mask)
    elif failure == "value_null":
        values = data.values.to_numpy()
        value_mask = np.zeros(len(values), dtype=bool)
        value_mask[0] = True
        data = pa.LargeListArray.from_arrays(
            data.offsets, pa.array(values, mask=value_mask)
        )
    elif failure in (
        "negative_dimension",
        "wrong_shape",
        "dimension_null",
        "wrong_rank",
    ):
        shapes = shape.to_pylist()
        if failure == "wrong_rank":
            shapes[0] = shapes[0] + [1]
        else:
            shapes[0][0] = {
                "negative_dimension": -1,
                "wrong_shape": 7,
                "dimension_null": None,
            }[failure]
        shape = pa.array(shapes, type=shape.type)
    elif failure == "descending_offsets":
        offsets = data.offsets.to_numpy().copy()
        offsets[1] = offsets[2] + 1
        data = pa.LargeListArray.from_arrays(pa.array(offsets), data.values)
    storage = pa.StructArray.from_arrays(
        [data, shape],
        names=["data", "shape"],
        mask=mask if failure == "parent_null" else None,
    )
    column = pa.chunked_array([column.type.wrap_array(storage), *column.chunks[1:]])
    monkeypatch.setattr(take_module.logger, "handlers", [caplog.handler])
    with caplog.at_level("DEBUG", logger=take_module.__name__):
        assert (
            take_module.try_prepare_chunked_tensor_take(column, max_output_rows=1)
            is None
        )
    reason = (
        take_module._TakeFallbackReason.CONTAINS_NULLS
        if failure == "parent_null"
        else take_module._TakeFallbackReason.UNSAFE_CHUNK_STORAGE
    )
    assert reason.value in caplog.text


@pytest.mark.parametrize("child_offset", [0, 3])
def test_variable_take_honors_logical_and_child_offsets(child_offset):
    typ = ArrowVariableShapedTensorType(pa.float32(), 2)
    values = pa.array(np.arange(1600003, dtype=np.float32)).slice(child_offset)
    offsets = pa.array([10, 400010, 1200010], type=pa.int64())
    data = pa.LargeListArray.from_arrays(offsets, values)
    shape = pa.array([[100000, 4], [200000, 4]], type=pa.list_(pa.int64()))
    arr = typ.wrap_array(
        pa.StructArray.from_arrays([data, shape], names=["data", "shape"])
    )
    column = pa.chunked_array([arr, arr])
    plan = take_module.try_prepare_chunked_tensor_take(column, max_output_rows=3)
    assert plan is not None
    output = plan.take(np.array([3, 0, 1], dtype=np.int64))
    assert output.storage.equals(_expected(column, [3, 0, 1]).storage)


@pytest.mark.parametrize("capacity", ["shape", "payload"])
def test_variable_take_checks_both_offset_capacities(capacity, caplog, monkeypatch):
    column = _column()
    max_rows = 2**30
    if capacity == "payload":
        # Lower the allocation capacity without constructing an enormous array.
        original = np.iinfo

        def limits(dtype):
            return original(np.dtype("int16") if dtype == np.dtype(np.intp) else dtype)

        monkeypatch.setattr(take_module.np, "iinfo", limits)
        max_rows = 96
    monkeypatch.setattr(take_module.logger, "handlers", [caplog.handler])
    with caplog.at_level("DEBUG", logger=take_module.__name__):
        assert (
            take_module.try_prepare_chunked_tensor_take(
                column, max_output_rows=max_rows
            )
            is None
        )
    assert take_module._TakeFallbackReason.OUTPUT_OFFSET_OVERFLOW.value in caplog.text


@pytest.mark.parametrize(
    "indices",
    [pa.array([0, None], type=pa.int64()), np.ma.array([0, 1], mask=[False, True])],
)
def test_variable_take_null_indices_use_standard_path(indices, take_calls):
    column = _column()
    result = transform_pyarrow.take_table(pa.table({"tensor": column}), indices)
    assert not take_calls
    assert result.column(0).chunk(0).storage.equals(_expected(column, indices).storage)


def test_variable_take_disabled_and_production_builder(monkeypatch, take_calls):
    builder = ArrowBlockBuilder()
    for chunk in _column().chunks:
        builder.add_block(
            pa.table({"tensor": ArrowTensorArray.from_numpy(chunk.to_numpy())})
        )
    table = builder.build()
    assert isinstance(table, pa.Table)
    indices = [95, 0, 3]
    enabled = transform_pyarrow.take_table(table, indices)
    assert take_calls == [3]
    take_calls.clear()
    monkeypatch.setattr(take_module, "ENABLE_CHUNKED_TENSOR_TAKE", False)
    disabled = transform_pyarrow.take_table(table, indices)
    assert not take_calls
    assert enabled.equals(disabled)


@pytest.mark.parametrize("stage", ["prepare", "take"])
def test_variable_take_failure_recovers(stage, monkeypatch, caplog):
    table = pa.table({"tensor": _column()})
    expected = _expected(table.column(0), [0, 95, 0])

    def fail(*args, **kwargs):
        raise RuntimeError("injected variable take failure")

    if stage == "prepare":
        monkeypatch.setattr(take_module, "_prepare_variable_chunk", fail)
    else:
        monkeypatch.setattr(take_module.PreparedVariableShapedTensorTake, "take", fail)
    monkeypatch.setattr(transform_pyarrow.logger, "handlers", [caplog.handler])
    result = transform_pyarrow.take_table(table, [0, 95, 0])
    assert result.column(0).chunk(0).storage.equals(expected.storage)
    assert any(r.levelname == "WARNING" and r.exc_info for r in caplog.records)


def test_variable_shuffle_reuses_plan_and_disables_failed_column(
    monkeypatch, take_calls
):
    column = _column()
    table, plans = batcher_module._prepare_local_shuffle_arrow_table(
        pa.table({"tensor": column, "id": np.arange(len(column))})
    )
    assert isinstance(plans[0], take_module.PreparedVariableShapedTensorTake)
    state = batcher_module._ShuffleBufferState(
        table, np.arange(len(column), dtype=np.int64)[::-1], plans
    )
    first = state.take_next(16)
    assert take_calls == [16]
    assert isinstance(first, pa.Table)
    assert (
        first.column(0)
        .chunk(0)
        .storage.equals(_expected(column, list(range(95, 79, -1))).storage)
    )

    def fail(*args, **kwargs):
        raise RuntimeError("injected")

    monkeypatch.setattr(take_module.PreparedVariableShapedTensorTake, "take", fail)
    second = state.take_next(16)
    assert not plans
    assert isinstance(second, pa.Table)
    assert isinstance(state.block, pa.Table)
    assert state.block.column(0).num_chunks == 1
    assert (
        second.column(0)
        .chunk(0)
        .storage.equals(_expected(column, list(range(79, 63, -1))).storage)
    )
    state.materialize_remaining()


@pytest.mark.parametrize(
    "rows,row_bytes,chunks,output_rows,eligible",
    [
        (2048, 8188, 4, 1, False),  # Below average row gate, even with a large source.
        # Even row count keeps the average at 8 KiB; the full request satisfies
        # the output-per-chunk gate, isolating the 8 MiB source-payload gate.
        (1022, 8192, 4, 1022, False),
        (1024, 8192, 4, 1, True),  # Source and per-chunk gates exactly meet the bound.
        (1024, 8192, 16, 1023, False),
        (1024, 8192, 16, 1024, True),  # Estimated output per chunk reaches 512 KiB.
    ],
)
def test_variable_take_size_gate_boundaries(
    rows, row_bytes, chunks, output_rows, eligible
):
    values = [
        np.zeros(row_bytes // 4 + (1 if i % 2 else -1), dtype=np.float32)
        for i in range(rows)
    ]
    array = ArrowVariableShapedTensorArray.from_numpy(values)
    parts = np.array_split(np.arange(rows), chunks)
    column = pa.chunked_array([array.slice(int(p[0]), len(p)) for p in parts])
    plan = take_module.try_prepare_chunked_tensor_take(
        column, max_output_rows=output_rows
    )
    assert (plan is not None) == eligible


def test_variable_take_counts_empty_chunks_in_cost_gate():
    # Exactly 2 MiB of source payload per physical chunk.
    values = [
        np.zeros(2048 + (1 if i % 2 else -1), dtype=np.float32) for i in range(1024)
    ]
    array = ArrowVariableShapedTensorArray.from_numpy(values)
    column = pa.chunked_array([array.slice(i, 256) for i in range(0, 1024, 256)])
    assert (
        take_module.try_prepare_chunked_tensor_take(column, max_output_rows=1)
        is not None
    )
    many = pa.chunked_array([*column.chunks, column.chunk(0).slice(0, 0)])
    assert take_module.try_prepare_chunked_tensor_take(many, max_output_rows=1) is None
    assert (
        take_module.try_prepare_chunked_tensor_take(many, max_output_rows=1024)
        is not None
    )


@pytest.mark.parametrize("small_row_values", [0, 1, 2048])
def test_variable_take_oversampling_cost_gate(small_row_values, take_calls):
    rows = [np.ones(small_row_values, dtype=np.float32)] * 511 + [
        np.ones(4 * 1024**2, dtype=np.float32)
    ]
    array = ArrowVariableShapedTensorArray.from_numpy(rows)
    column = pa.chunked_array([array.slice(i, 128) for i in range(0, 512, 128)])
    # Above this count, the avoided source copy cannot cover 8 KiB per output
    # row. The request could repeat only the tiny rows despite a large average.
    source_budget_rows = sum(row.nbytes for row in rows) // (8 * 1024)
    assert (
        take_module.try_prepare_chunked_tensor_take(
            column, max_output_rows=source_budget_rows
        )
        is not None
    )
    indices = np.zeros(source_budget_rows + 1, dtype=np.int64)
    plan = take_module.try_prepare_chunked_tensor_take(
        column, max_output_rows=len(indices)
    )
    # Oversampling still uses the fast path when even the smallest possible
    # selected row meets the row-size gate.
    assert (plan is not None) == (small_row_values == 2048)
    result = transform_pyarrow.take_table(pa.table({"tensor": column}), indices)
    assert take_calls == ([len(indices)] if small_row_values == 2048 else [])
    assert result.column(0).chunk(0).storage.equals(_expected(column, indices).storage)


@pytest.mark.parametrize("null_data_buffer", [False, True])
@pytest.mark.parametrize("indices", [[2, 0, 2], [0, 1, 0]])
def test_variable_take_large_repeated_row_and_zero_chunk(
    null_data_buffer, indices, take_calls
):
    long = np.arange(3 * 1024**2, dtype=np.float32)
    empty = np.empty(0, dtype=np.float32)
    a = ArrowVariableShapedTensorArray.from_numpy([empty, empty])
    if null_data_buffer:
        # A zero-length numeric child may legally omit its data buffer.
        data = pa.LargeListArray.from_arrays(
            a.storage.field("data").offsets,
            pa.Array.from_buffers(pa.float32(), 0, [None, None]),
        )
        a = a.type.wrap_array(
            pa.StructArray.from_arrays(
                [data, a.storage.field("shape")], names=["data", "shape"]
            )
        )
    b = ArrowVariableShapedTensorArray.from_numpy([long, long[:1024]])
    column = pa.chunked_array([a, b])
    column.validate(full=True)
    # Each selected long row exceeds the fixed-shape 8 MiB scratch cap. Copies
    # go directly to the final output, with no per-value gather index array.
    result = transform_pyarrow.take_table(pa.table({"tensor": column}), indices)
    assert take_calls == [3]
    assert result.column(0).chunk(0).storage.equals(_expected(column, indices).storage)


@pytest.mark.parametrize("dtype", [np.float32, np.float64, np.uint64])
def test_variable_take_preserves_bits_with_multiaxis_shapes_and_child_offsets(
    dtype, take_calls
):
    # Include signed zero, infinities, NaN payloads, and integers that cannot be
    # represented exactly in float64. Compare bytes rather than NaN equality.
    if dtype == np.float32:
        bits = np.array(
            [0, 0x80000000, 0x7F800000, 0xFF800000, 0x7FC00001, 0x7F800001],
            dtype=np.uint32,
        )
    else:
        bits = np.array(
            [0, 2**63, 0x7FF0000000000000, 0x7FF8000000000001, 2**64 - 1],
            dtype=np.uint64,
        )
    values = bits.view(dtype)
    shapes = [(32, 32, 64), (16, 128, 64), (64, 32, 32), (32, 0, 64)]
    tensors = [np.resize(values, shapes[i % 4]) for i in range(48)]
    array = ArrowVariableShapedTensorArray.from_numpy(tensors)
    chunks = []
    for start in range(0, 48, 12):
        storage = array.slice(start, 12).storage
        data, shape = storage.field("data"), storage.field("shape")
        # Both list children have a nonzero array offset, independently of the
        # logical offsets retained by the parent slice.
        data_values = pa.array(
            np.concatenate([values[:3], data.values.to_numpy()]),
            from_pandas=False,
        ).slice(3)
        shape_values = pa.array(
            np.concatenate([np.full(3, -1, dtype=np.int64), shape.values.to_numpy()])
        ).slice(3)
        chunks.append(
            array.type.wrap_array(
                pa.StructArray.from_arrays(
                    [
                        pa.LargeListArray.from_arrays(data.offsets, data_values),
                        pa.ListArray.from_arrays(shape.offsets, shape_values),
                    ],
                    names=["data", "shape"],
                )
            )
        )
    column = pa.chunked_array(chunks)
    indices = [47, 0, 1, 2, 3, 11, 1]
    result = transform_pyarrow.take_table(pa.table({"tensor": column}), indices)
    assert take_calls == [len(indices)]
    actual = result.column(0).chunk(0)
    actual.validate(full=True)
    expected = _expected(column, indices)
    assert actual.type.ndim == 3
    assert actual.storage.field("shape").equals(expected.storage.field("shape"))
    assert (
        actual.storage.field("data").values.to_numpy().tobytes()
        == expected.storage.field("data").values.to_numpy().tobytes()
    )


def test_variable_take_shape_product_cannot_wrap():
    column = _column()
    chunk = column.chunk(0)
    data = chunk.storage.field("data")
    shapes = chunk.storage.field("shape").to_pylist()
    # The product wraps to the real row length in int64 arithmetic.
    shapes[0] = [2**62 + 4096, 4]
    storage = pa.StructArray.from_arrays(
        [data, pa.array(shapes, type=pa.list_(pa.int64()))], names=["data", "shape"]
    )
    malformed = pa.chunked_array([column.type.wrap_array(storage), *column.chunks[1:]])
    assert (
        take_module.try_prepare_chunked_tensor_take(malformed, max_output_rows=1)
        is None
    )


def test_variable_plan_retains_zero_copy_source_views():
    column = _column()
    plan = take_module.try_prepare_chunked_tensor_take(column, max_output_rows=8)
    assert isinstance(plan, take_module.PreparedVariableShapedTensorTake)
    for source, prepared in zip(column.chunks, plan.chunks):
        data = source.storage.field("data")
        shape = source.storage.field("shape")
        assert np.shares_memory(prepared.values, data.values.to_numpy())
        assert np.shares_memory(prepared.offsets, data.offsets.to_numpy())
        assert np.shares_memory(prepared.shapes, shape.values.to_numpy())


def test_hash_partition_preserves_variable_tensors(monkeypatch, take_calls):
    column = _column()
    table = pa.table(
        {
            "key": np.arange(len(column)) % 11,
            "row_id": np.arange(len(column)),
            "tensor": column,
        }
    )
    partitions = transform_pyarrow.hash_partition(
        table, hash_cols=["key"], num_partitions=7
    )
    assert take_calls == [len(column)]
    seen = []
    for partition in partitions.values():
        row_ids = partition.column("row_id").to_numpy()
        assert np.all(row_ids[1:] > row_ids[:-1])
        assert partition.schema == table.schema
        assert (
            partition.column("tensor")
            .chunk(0)
            .storage.equals(_expected(column, row_ids).storage)
        )
        seen.extend(row_ids)
    np.testing.assert_array_equal(np.sort(seen), np.arange(len(column)))

    take_calls.clear()
    monkeypatch.setattr(take_module, "ENABLE_CHUNKED_TENSOR_TAKE", False)
    expected = transform_pyarrow.hash_partition(
        table, hash_cols=["key"], num_partitions=7
    )
    assert not take_calls
    assert partitions.keys() == expected.keys()
    assert all(partition.equals(expected[key]) for key, partition in partitions.items())


@pytest.mark.parametrize("dtype", [np.bool_, "datetime64[ns]"])
def test_variable_take_rejects_non_numeric_scalars(dtype, take_calls):
    arrays = [np.arange(n).astype(dtype) for n in (2, 3)]
    array = ArrowVariableShapedTensorArray.from_numpy(arrays)
    column = pa.chunked_array([array, array])
    assert (
        take_module.try_prepare_chunked_tensor_take(column, max_output_rows=3) is None
    )
    result = transform_pyarrow.take_table(pa.table({"tensor": column}), [3, 0, 3])
    assert not take_calls
    assert (
        result.column(0).chunk(0).storage.equals(_expected(column, [3, 0, 3]).storage)
    )


@pytest.mark.parametrize("fail_variable", [False, True])
def test_mixed_tensor_columns_share_indices_and_keep_independent_fallbacks(
    fail_variable, monkeypatch, take_calls
):
    variable = _column()
    fixed = ArrowTensorArray.from_numpy(np.arange(96 * 4096).reshape(96, 4096))
    table = pa.table(
        {
            "variable": variable,
            "fixed": pa.chunked_array([fixed.slice(0, 48), fixed.slice(48)]),
            "id": np.arange(96),
        }
    )
    indices = np.array([95, 0, 17, 0], dtype=np.int32)
    normalized = []
    original_normalize = transform_pyarrow._try_normalize_take_indices
    original_fixed = take_module.PreparedChunkedTensorTake.take
    fixed_calls = []

    def normalize(indices, rows):
        result = original_normalize(indices, rows)
        normalized.append(result)
        return result

    def take_fixed(plan, indices):
        fixed_calls.append(indices)
        return original_fixed(plan, indices)

    def fail(*args, **kwargs):
        raise RuntimeError("injected variable take failure")

    monkeypatch.setattr(transform_pyarrow, "_try_normalize_take_indices", normalize)
    monkeypatch.setattr(take_module.PreparedChunkedTensorTake, "take", take_fixed)
    if fail_variable:
        monkeypatch.setattr(take_module.PreparedVariableShapedTensorTake, "take", fail)
    result = transform_pyarrow.take_table(table, indices)
    assert len(normalized) == len(fixed_calls) == 1
    assert normalized[0] is fixed_calls[0]
    assert take_calls == ([] if fail_variable else [4])
    assert (
        result.column("variable")
        .chunk(0)
        .storage.equals(_expected(variable, indices).storage)
    )
    assert result.column("fixed").chunk(0).storage.equals(fixed.take(indices).storage)
    assert result.column("id").equals(
        pa.chunked_array([pa.array(indices.astype(np.int64))])
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
