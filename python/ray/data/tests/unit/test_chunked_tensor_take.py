import gc
import math

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal import batcher as batcher_module
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.arrow_ops.transform_pyarrow import (
    _try_normalize_take_indices,
    hash_partition,
)
from ray.data._internal.batcher import (
    ShufflingBatcher,
    _prepare_local_shuffle_arrow_table,
    _take_prepared_arrow_table,
)
from ray.data._internal.tensor_extensions import chunked_tensor_take
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorType,
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorType,
)
from ray.data._internal.tensor_extensions.chunked_tensor_take import (
    TENSOR_TAKE_SCRATCH_CAP_BYTES,
    try_prepare_chunked_tensor_take,
    try_take_chunked_tensor,
)
from ray.data.extensions import take_table


def _tensor_array(tensor_type, values):
    values = np.ascontiguousarray(values)
    flat_values = values.reshape(-1)
    scalar_type = tensor_type.storage_type.value_type
    data = pa.Array.from_buffers(
        scalar_type,
        flat_values.size,
        [None, pa.py_buffer(flat_values)],
    )
    values_per_row = math.prod(tensor_type.shape)
    offsets = np.arange(
        0,
        (len(values) + 1) * values_per_row,
        values_per_row,
        dtype=np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype()),
    )
    storage = pa.Array.from_buffers(
        tensor_type.storage_type,
        len(values),
        [None, pa.py_buffer(offsets)],
        children=[data],
    )
    return tensor_type.wrap_array(storage)


def _chunked_tensor(rows, width, chunks, tensor_cls=ArrowTensorTypeV2):
    tensor_type = tensor_cls((width,), pa.float32())
    values = np.arange(rows * width, dtype=np.float32).reshape(rows, width)
    array = _tensor_array(tensor_type, values)
    boundaries = np.linspace(0, rows, chunks + 1, dtype=np.int64)
    arrays = [
        array.slice(
            int(boundaries[index]), int(boundaries[index + 1] - boundaries[index])
        )
        for index in range(chunks)
    ]
    arrays.insert(0, array.slice(0, 0))
    arrays.insert(len(arrays) // 2, array.slice(rows // 2, 0))
    arrays.append(array.slice(rows, 0))
    return pa.chunked_array(arrays, type=tensor_type), values


def _tensor_table(rows, width, chunks, *, start=0):
    tensor_type = ArrowTensorTypeV2((width,), pa.float32())
    row_ids = np.arange(start, start + rows, dtype=np.int64)
    values = np.zeros((rows, width), dtype=np.float32)
    values[:, 0] = row_ids.astype(np.float32)
    array = _tensor_array(tensor_type, values)
    boundaries = np.linspace(0, rows, chunks + 1, dtype=np.int64)
    tensor_chunks = [
        array.slice(
            int(boundaries[index]), int(boundaries[index + 1] - boundaries[index])
        )
        for index in range(chunks)
    ]
    return pa.table(
        {
            "row_id": row_ids,
            "tensor": pa.chunked_array(tensor_chunks, type=tensor_type),
        }
    )


def _tensor_with_null(*, child_null):
    rows = 40
    width = 64
    tensor_type = ArrowTensorTypeV2((width,), pa.float32())
    values = np.arange(rows * width, dtype=np.float32)
    validity = np.full((max(rows, values.size) + 7) // 8, 0xFF, dtype=np.uint8)
    null_index = width + 1 if child_null else 5
    validity[null_index // 8] &= np.uint8(~(1 << (null_index % 8)) & 0xFF)
    data = pa.Array.from_buffers(
        pa.float32(),
        values.size,
        [pa.py_buffer(validity) if child_null else None, pa.py_buffer(values)],
        null_count=1 if child_null else 0,
    )
    offsets = np.arange(
        0,
        (rows + 1) * width,
        width,
        dtype=np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype()),
    )
    storage = pa.Array.from_buffers(
        tensor_type.storage_type,
        rows,
        [pa.py_buffer(validity) if not child_null else None, pa.py_buffer(offsets)],
        null_count=1 if not child_null else 0,
        children=[data],
    )
    array = tensor_type.wrap_array(storage)
    return pa.chunked_array([array.slice(0, 20), array.slice(20)], type=tensor_type)


def _tensor_with_child_offset():
    width = 64
    tensor_type = ArrowTensorTypeV2((width,), pa.float32())
    base = pa.array(np.arange(200, dtype=np.float32))
    child = base.slice(10, 2 * width)
    offsets = pa.array([0, width, 2 * width], type=tensor_type.OFFSET_DTYPE)
    storage = pa.LargeListArray.from_arrays(offsets, child)
    array = tensor_type.wrap_array(storage)
    return pa.chunked_array([array.slice(0, 1), array.slice(1, 1)], type=tensor_type)


def _tensor_with_raw_offsets(offsets, child):
    width = 64
    tensor_type = ArrowTensorTypeV2((width,), child.type)
    raw_offsets = np.asarray(
        offsets,
        dtype=np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype()),
    )
    storage = pa.Array.from_buffers(
        tensor_type.storage_type,
        len(raw_offsets) - 1,
        [None, pa.py_buffer(raw_offsets)],
        children=[child],
    )
    array = tensor_type.wrap_array(storage)
    return (
        pa.chunked_array(
            [array.slice(index, 1) for index in range(len(array))],
            type=tensor_type,
        ),
        storage,
    )


def _zero_shape_chunked_tensor(rows, shape, tensor_cls):
    tensor_type = tensor_cls(shape, pa.float32())
    offsets = np.zeros(
        rows + 1,
        dtype=np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype()),
    )
    storage = pa.Array.from_buffers(
        tensor_type.storage_type,
        rows,
        [None, pa.py_buffer(offsets)],
        children=[pa.array([], type=pa.float32())],
    )
    array = tensor_type.wrap_array(storage)
    split = rows // 2
    return pa.chunked_array(
        [array.slice(0, split), array.slice(split)],
        type=tensor_type,
    )


@pytest.mark.parametrize("tensor_cls", [ArrowTensorType, ArrowTensorTypeV2])
@pytest.mark.parametrize("chunks", [2, 17, 257])
def test_chunked_tensor_take(tensor_cls, chunks):
    rows = 514 if chunks == 257 else 64
    column, values = _chunked_tensor(rows, 64, chunks, tensor_cls)
    indices = np.array([rows - 1, 0, rows // 2, rows // 2, 3], dtype=np.int64)

    plan = try_prepare_chunked_tensor_take(column)

    assert plan is not None
    row_bytes = math.prod(column.type.shape) * np.dtype(np.float32).itemsize
    assert (
        plan.subbatch_rows
        * (row_bytes + chunked_tensor_take._INDEX_SCRATCH_BYTES_PER_ROW)
        <= TENSOR_TAKE_SCRATCH_CAP_BYTES
    )
    output = plan.try_take(indices)
    assert output is not None
    np.testing.assert_array_equal(output.to_numpy(), values[indices])
    assert output.type == column.type

    larger_indices = np.arange(10, dtype=np.int64)
    larger_output = plan.try_take(larger_indices)
    assert larger_output is not None
    np.testing.assert_array_equal(larger_output.to_numpy(), values[larger_indices])

    limited_plan = plan._replace(max_supported_output_rows=2)
    assert limited_plan.try_take(np.array([0, 1, 2], dtype=np.int64)) is None


@pytest.mark.parametrize(
    "chunks,indices,strategy",
    [
        pytest.param(
            4,
            [0, 1, 16, 17, 48, 49],
            "_gather_monotonic_chunk_ids",
            id="monotonic-contiguous",
        ),
        pytest.param(
            4,
            [0, 3, 16, 20, 48, 55],
            "_gather_monotonic_chunk_ids",
            id="monotonic-noncontiguous",
        ),
        pytest.param(
            4,
            [63, 0, 32, 1],
            "_gather_by_chunk_masks",
            id="unordered-few-chunks",
        ),
        pytest.param(
            17,
            [63, 0, 32, 1],
            "_gather_by_sorted_chunk_ids",
            id="unordered-many-chunks",
        ),
    ],
)
def test_chunked_tensor_take_gather_strategy(monkeypatch, chunks, indices, strategy):
    column, values = _chunked_tensor(64, 64, chunks)
    indices = np.asarray(indices, dtype=np.int64)
    original = getattr(chunked_tensor_take, strategy)
    calls = 0

    def record_strategy(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(chunked_tensor_take, strategy, record_strategy)
    plan = try_prepare_chunked_tensor_take(column)
    assert plan is not None
    output = plan.try_take(indices)

    assert calls == 1
    assert output is not None
    np.testing.assert_array_equal(output.to_numpy(), values[indices])


def test_chunked_tensor_take_across_scratch_subbatches():
    rows = 1000
    column, values = _chunked_tensor(rows, 3000, 17)
    indices = np.random.default_rng(42).permutation(rows).astype(np.int64)
    plan = try_prepare_chunked_tensor_take(column)

    assert plan is not None
    assert 0 < plan.subbatch_rows < rows
    output = plan.try_take(indices)
    assert output is not None
    np.testing.assert_array_equal(output.to_numpy(), values[indices])


def test_chunked_tensor_take_allows_one_row_to_exceed_scratch_cap():
    shape = (2000, 1697)
    column, _ = _chunked_tensor(2, math.prod(shape), 2)
    tensor_type = ArrowTensorTypeV2(shape, pa.float32())
    column = pa.chunked_array(
        [tensor_type.wrap_array(chunk.storage) for chunk in column.chunks],
        type=tensor_type,
    )

    plan = try_prepare_chunked_tensor_take(column, expected_output_rows=1)

    assert plan is not None
    assert plan.subbatch_rows == 1
    assert (
        plan.values_per_row * plan.value_dtype.itemsize > TENSOR_TAKE_SCRATCH_CAP_BYTES
    )
    output = plan.try_take(np.array([1], dtype=np.int64))
    assert output is not None
    assert output.storage.values[0].as_py() == pytest.approx(math.prod(shape))


def test_chunked_tensor_take_output_owns_source_lifetime():
    column, values = _chunked_tensor(4096, 64, 5)
    expected = values[[4095, 1, 2048]].copy()

    output = try_take_chunked_tensor(
        column,
        np.array([4095, 1, 2048], dtype=np.int64),
    )

    assert output is not None
    del column
    del values
    gc.collect()
    np.testing.assert_array_equal(output.to_numpy(), expected)


@pytest.mark.parametrize("index_dtype", [np.int32, np.uint32, np.int64, np.uint64])
def test_chunked_tensor_take_accepts_integer_index_dtypes(index_dtype):
    column, values = _chunked_tensor(40, 64, 4)
    indices = np.array([39, 1, 20], dtype=index_dtype)

    output = take_table(pa.table({"tensor": column}), indices).column("tensor").chunk(0)

    np.testing.assert_array_equal(output.to_numpy(), values[indices])


def test_prepared_chunked_tensor_take_owns_source_lifetime():
    column, values = _chunked_tensor(40, 64, 5)
    indices = np.array([39, 1, 20], dtype=np.int64)
    expected = values[indices].copy()
    plan = try_prepare_chunked_tensor_take(column)

    assert plan is not None
    del column
    del values
    gc.collect()

    output = plan.try_take(indices)
    assert output is not None
    np.testing.assert_array_equal(output.to_numpy(), expected)


@pytest.mark.parametrize("rows", [64, 1024])
def test_small_narrow_chunked_tensor_take_falls_back(rows):
    narrow, _ = _chunked_tensor(rows, 8, 4)
    assert (
        try_prepare_chunked_tensor_take(
            narrow,
            expected_output_rows=min(rows, 128),
        )
        is None
    )


def test_chunked_tensor_take_eligibility_uses_source_and_output_work():
    narrow_large_source, _ = _chunked_tensor(100_000, 8, 32)
    assert (
        try_prepare_chunked_tensor_take(
            narrow_large_source,
            expected_output_rows=128,
        )
        is None
    )

    small_wide_source, _ = _chunked_tensor(1024, 64, 4)
    assert (
        try_prepare_chunked_tensor_take(
            small_wide_source,
            expected_output_rows=128,
        )
        is None
    )

    wide_large_source, _ = _chunked_tensor(100_000, 64, 32)
    assert (
        try_prepare_chunked_tensor_take(
            wide_large_source,
            expected_output_rows=128,
        )
        is not None
    )
    assert (
        try_prepare_chunked_tensor_take(
            wide_large_source,
            expected_output_rows=100_000,
        )
        is None
    )


def test_chunked_tensor_take_can_be_disabled(monkeypatch):
    column, values = _chunked_tensor(40, 64, 4)
    indices = np.array([39, 1, 20], dtype=np.int64)
    monkeypatch.setattr(chunked_tensor_take, "ENABLE_CHUNKED_TENSOR_TAKE", False)

    # Column preparation is a feature-flag-independent capability check.
    assert try_prepare_chunked_tensor_take(column) is not None

    def fail_fast_path(*args, **kwargs):
        raise AssertionError("Disabled chunked tensor fast path was entered")

    monkeypatch.setattr(
        transform_pyarrow,
        "_try_normalize_take_indices",
        fail_fast_path,
    )
    monkeypatch.setattr(
        batcher_module,
        "try_prepare_chunked_tensor_take",
        fail_fast_path,
    )

    table = pa.table({"tensor": column})
    output = take_table(table, indices)
    np.testing.assert_array_equal(
        output.column("tensor").combine_chunks().to_numpy(),
        values[indices],
    )

    prepared_table, prepared_takes = _prepare_local_shuffle_arrow_table(table)
    assert not prepared_takes
    assert prepared_table.column("tensor").num_chunks == 1


def test_ineligible_chunked_tensor_skips_index_normalization(monkeypatch):
    column, values = _chunked_tensor(40, 64, 4)
    indices = np.array([39, 1, 20], dtype=np.int64)

    def fail_normalization(*args, **kwargs):
        raise AssertionError("Ineligible tensor take normalized indices")

    monkeypatch.setattr(
        transform_pyarrow,
        "_try_normalize_take_indices",
        fail_normalization,
    )
    output = take_table(pa.table({"tensor": column}), indices)

    np.testing.assert_array_equal(
        output.column("tensor").combine_chunks().to_numpy(),
        values[indices],
    )


def test_chunked_tensor_take_fallbacks():
    single = pa.chunked_array([_chunked_tensor(20, 64, 2)[0].combine_chunks()])
    assert try_prepare_chunked_tensor_take(single) is None

    nullable = _tensor_with_null(child_null=False)
    assert nullable.null_count == 1
    assert try_prepare_chunked_tensor_take(nullable) is None

    child_nullable = _tensor_with_null(child_null=True)
    assert child_nullable.null_count == 0
    assert child_nullable.chunk(0).storage.values.null_count == 1
    assert try_prepare_chunked_tensor_take(child_nullable) is None

    variable_type = ArrowVariableShapedTensorType(pa.float32(), ndim=1)
    variable_storage = pa.array(
        [
            {"data": [1.0, 2.0], "shape": [2]},
            {"data": [3.0], "shape": [1]},
        ],
        type=variable_type.storage_type,
    )
    variable = variable_type.wrap_array(variable_storage)
    variable_chunked = pa.chunked_array(
        [variable.slice(0, 1), variable.slice(1)], type=variable_type
    )
    assert try_prepare_chunked_tensor_take(variable_chunked) is None

    child_offset = _tensor_with_child_offset()
    assert child_offset.chunk(0).storage.values.offset == 10
    assert try_prepare_chunked_tensor_take(child_offset) is None
    child_offset_output = (
        take_table(pa.table({"tensor": child_offset}), np.array([0, 1], dtype=np.int64))
        .column("tensor")
        .chunk(0)
    )
    assert child_offset_output.storage.to_pylist() == [
        list(np.arange(10, 74, dtype=np.float32)),
        list(np.arange(74, 138, dtype=np.float32)),
    ]

    if hasattr(pa, "FixedShapeTensorArray"):
        native = pa.FixedShapeTensorArray.from_numpy_ndarray(
            np.arange(40 * 64, dtype=np.float32).reshape(40, 64)
        )
        native_chunked = pa.chunked_array([native.slice(0, 20), native.slice(20)])
        assert try_prepare_chunked_tensor_take(native_chunked) is None


def test_chunked_tensor_take_respects_nonzero_logical_offsets():
    width = 64
    column, storage = _tensor_with_raw_offsets(
        [width, 2 * width, 3 * width],
        pa.array(np.arange(3 * width, dtype=np.int64)),
    )

    plan = try_prepare_chunked_tensor_take(column)

    assert plan is not None
    output = plan.try_take(np.array([0, 1], dtype=np.int64))
    assert output is not None
    assert output.storage.to_pylist() == storage.to_pylist()


def test_chunked_tensor_take_rejects_invalid_logical_offsets():
    width = 64
    irregular, _ = _tensor_with_raw_offsets(
        [0, width, 2 * width + 1],
        pa.array(np.arange(2 * width + 1, dtype=np.int64)),
    )
    assert try_prepare_chunked_tensor_take(irregular) is None

    column, _ = _chunked_tensor(40, width, 4)
    chunk = next(chunk for chunk in column.chunks if len(chunk) > 0)

    class PastEndStorage:
        def __init__(self, storage):
            self._storage = storage
            self.values = storage.values
            start = len(self.values) - width
            self.offsets = pa.array(
                start + np.arange(len(chunk) + 1) * width,
                type=column.type.OFFSET_DTYPE,
            )

        def __getattr__(self, name):
            return getattr(self._storage, name)

    class PastEndChunk:
        def __init__(self, array):
            self._array = array
            self.storage = PastEndStorage(array.storage)

        def __getattr__(self, name):
            return getattr(self._array, name)

        def __len__(self):
            return len(self._array)

    assert (
        chunked_tensor_take._try_get_zero_copy_chunk_view(
            PastEndChunk(chunk),
            column.type,
            width,
            np.dtype(np.float32),
        )
        is None
    )


@pytest.mark.parametrize("tensor_cls", [ArrowTensorType, ArrowTensorTypeV2])
def test_hash_partition_preserves_multichunk_tensor(tensor_cls):
    rows = 97
    tensor, values = _chunked_tensor(rows, 64, 17, tensor_cls)
    table = pa.table(
        {
            "key": np.arange(rows, dtype=np.int64) % 11,
            "row_id": np.arange(rows, dtype=np.int64),
            "tensor": tensor,
        }
    )

    partitions = hash_partition(table, hash_cols=["key"], num_partitions=7)
    combined = pa.concat_tables(list(partitions.values()))
    row_ids = combined.column("row_id").combine_chunks().to_numpy()
    order = np.argsort(row_ids)
    actual_tensor = combined.column("tensor").combine_chunks().to_numpy()

    assert combined.schema == table.schema
    np.testing.assert_array_equal(row_ids[order], np.arange(rows, dtype=np.int64))
    np.testing.assert_array_equal(actual_tensor[order], values)


@pytest.mark.parametrize("tensor_cls", [ArrowTensorType, ArrowTensorTypeV2])
@pytest.mark.parametrize("rows,shape", [(3, (0,)), (3, (0, 2)), (0, (0,))])
def test_zero_shape_tensor_falls_back(tensor_cls, rows, shape):
    tensor = _zero_shape_chunked_tensor(rows, shape, tensor_cls)
    table = pa.table({"tensor": tensor})
    reference = pa.table({"tensor": tensor.combine_chunks()})
    indices = np.arange(rows, dtype=np.int64)

    assert try_prepare_chunked_tensor_take(tensor) is None
    actual = take_table(table, indices)
    expected = take_table(reference, indices)

    assert actual.schema == expected.schema
    assert actual.num_rows == expected.num_rows == rows
    assert (
        actual.column("tensor").combine_chunks().storage.to_pylist()
        == expected.column("tensor").combine_chunks().storage.to_pylist()
    )


@pytest.mark.parametrize(
    "indices",
    [
        [4, 0, 4, 2],
        np.array([4, 0, 4, 2], dtype=np.int32),
        np.array([4, 0, 4, 2], dtype=np.int64),
        np.array([4, 0, 4, 2], dtype=np.uint64),
        pa.array([4, 0, 4, 2], type=pa.int64()),
        pa.chunked_array([[4, 0], [4, 2]], type=pa.int32()),
        np.ma.array([0, 1], mask=[False, True]),
        pa.array([0, None], type=pa.int64()),
    ],
)
def test_take_table_matches_single_chunk_tensor(indices):
    table = _tensor_table(5, 64, 2)
    reference = pa.table(
        {
            "row_id": table.column("row_id").combine_chunks(),
            "tensor": table.column("tensor").combine_chunks(),
        }
    )

    actual = take_table(table, indices)
    expected = take_table(reference, indices)

    assert (
        actual.column("row_id").combine_chunks().to_pylist()
        == expected.column("row_id").combine_chunks().to_pylist()
    )
    assert (
        actual.column("tensor").combine_chunks().storage.to_pylist()
        == expected.column("tensor").combine_chunks().storage.to_pylist()
    )
    assert actual.schema == expected.schema


@pytest.mark.parametrize(
    "indices,expected",
    [
        ([4, 0, 2], [4, 0, 2]),
        (np.array([4, 0, 2], dtype=np.int32), [4, 0, 2]),
        (np.array([4, 0, 2], dtype=np.uint64), [4, 0, 2]),
        (pa.array([4, 0, 2], type=pa.int16()), [4, 0, 2]),
        (pa.chunked_array([[4, 0], [2]], type=pa.int32()), [4, 0, 2]),
    ],
)
def test_normalize_take_indices(indices, expected):
    normalized = _try_normalize_take_indices(indices, row_count=5)

    assert normalized is not None
    assert normalized.dtype == np.dtype(np.int64)
    assert normalized.dtype.isnative
    np.testing.assert_array_equal(normalized, expected)


@pytest.mark.parametrize(
    "indices",
    [
        np.ma.array([0, 1], mask=[False, True]),
        pa.array([0, None], type=pa.int64()),
        pa.chunked_array([[0], [None]], type=pa.int64()),
        pa.chunked_array([[0.0], [1.0]], type=pa.float64()),
        np.array([-1], dtype=np.int64),
        np.array([5], dtype=np.int64),
        np.array([1.0], dtype=np.float64),
        np.array([True], dtype=np.bool_),
        np.array([[0, 1]], dtype=np.int64),
        np.array([0, 1], dtype=">i8"),
        [],
        [0, None],
        [True, 1],
    ],
)
def test_normalize_take_indices_rejects_fallback_inputs(indices):
    assert _try_normalize_take_indices(indices, row_count=5) is None


@pytest.mark.parametrize(
    "indices",
    [
        np.array([3, -1], dtype=np.int64),
        np.array([5], dtype=np.int64),
        np.array([1.0, 2.0]),
        np.array([[0, 1]], dtype=np.int64),
        np.array([4, 0, 4, 2], dtype=">i8"),
    ],
)
def test_take_table_preserves_fallback_exceptions(indices):
    table = _tensor_table(5, 64, 2)
    reference = pa.table(
        {
            "row_id": table.column("row_id").combine_chunks(),
            "tensor": table.column("tensor").combine_chunks(),
        }
    )

    with pytest.raises(Exception) as expected_error:
        take_table(reference, indices)
    with pytest.raises(type(expected_error.value)):
        take_table(table, indices)


@pytest.mark.parametrize("indices", [[1, True], [True, 1], []])
def test_take_table_tensor_only_preserves_list_exceptions(indices):
    table = _tensor_table(5, 64, 2).select(["tensor"])
    reference = pa.table({"tensor": table.column("tensor").combine_chunks()})

    with pytest.raises(Exception) as expected_error:
        take_table(reference, indices)
    with pytest.raises(type(expected_error.value)):
        take_table(table, indices)


def _consume(table, *, batch_size, buffer_rows, source_rows, seed):
    batcher = ShufflingBatcher(
        batch_size=batch_size,
        shuffle_buffer_min_size=buffer_rows,
        shuffle_seed=seed,
    )
    batches = []

    def consume_ready():
        while batcher.has_batch():
            batches.append(batcher.next_batch())

    for start in range(0, len(table), source_rows):
        batcher.add(table.slice(start, min(source_rows, len(table) - start)))
        consume_ready()
    batcher.done_adding()
    consume_ready()
    if batcher.has_any():
        batches.append(batcher.next_batch())
    return batches, batcher


def test_shuffling_batcher_reuses_prepared_chunked_tensor_take(monkeypatch):
    monkeypatch.setattr(
        batcher_module, "get_total_obj_store_mem_on_node", lambda: 1 << 60
    )
    monkeypatch.setattr(
        chunked_tensor_take,
        "_is_fast_take_worthwhile",
        lambda **kwargs: True,
    )
    tensor_table = _tensor_table(79, 64, 20)
    plain_table = tensor_table.select(["row_id"])

    tensor_batches, batcher = _consume(
        tensor_table, batch_size=9, buffer_rows=27, source_rows=4, seed=51
    )
    plain_batches, _ = _consume(
        plain_table, batch_size=9, buffer_rows=27, source_rows=4, seed=51
    )

    tensor_ids = [
        batch.column("row_id").combine_chunks().to_numpy() for batch in tensor_batches
    ]
    plain_ids = [
        batch.column("row_id").combine_chunks().to_numpy() for batch in plain_batches
    ]
    assert len(tensor_ids) == len(plain_ids)
    for actual, expected in zip(tensor_ids, plain_ids):
        np.testing.assert_array_equal(actual, expected)
    for batch, ids in zip(tensor_batches, tensor_ids):
        tensor = batch.column("tensor").chunk(0).to_numpy()
        np.testing.assert_array_equal(tensor[:, 0], ids.astype(np.float32))

    assert sum(len(ids) for ids in tensor_ids) == len(tensor_table)
    assert batcher._batch_head == len(batcher._shuffled_indices)
    assert batcher._prepared_tensor_takes
    assert not hasattr(batcher, "_prefetched_block")
    for prepared in batcher._prepared_tensor_takes.values():
        assert prepared.max_supported_output_rows >= 9


def test_unexpected_chunked_tensor_take_errors_propagate(monkeypatch):
    column, _ = _chunked_tensor(40, 64, 4)

    def raise_prepare(*args, **kwargs):
        raise RuntimeError("injected prepare failure")

    monkeypatch.setattr(
        chunked_tensor_take, "_try_get_zero_copy_chunk_view", raise_prepare
    )
    with pytest.raises(RuntimeError, match="injected prepare failure"):
        try_prepare_chunked_tensor_take(column)


def test_chunked_tensor_take_falls_back_for_truncated_buffers():
    column, _ = _chunked_tensor(40, 64, 4)
    chunk = next(chunk for chunk in column.chunks if len(chunk) > 0)

    class TruncatedBuffersValues:
        def __init__(self, values):
            self._values = values

        def __getattr__(self, name):
            return getattr(self._values, name)

        def buffers(self):
            return self._values.buffers()[:1]

    class TruncatedBuffersStorage:
        def __init__(self, storage):
            self.values = TruncatedBuffersValues(storage.values)

    class TruncatedBuffersChunk:
        def __init__(self, array):
            self._array = array
            self.storage = TruncatedBuffersStorage(array.storage)

        def __getattr__(self, name):
            return getattr(self._array, name)

        def __len__(self):
            return len(self._array)

    assert (
        chunked_tensor_take._try_get_zero_copy_chunk_view(
            TruncatedBuffersChunk(chunk),
            column.type,
            64,
            np.dtype(np.float32),
        )
        is None
    )


def test_chunked_tensor_take_checks_view_pointer_range():
    column, _ = _chunked_tensor(40, 64, 4)
    chunk = next(chunk for chunk in reversed(column.chunks) if len(chunk) > 0)

    class ShortDataBufferValues:
        def __init__(self, values):
            self._values = values

        def __getattr__(self, name):
            return getattr(self._values, name)

        def __len__(self):
            return len(self._values)

        def buffers(self):
            validity, data = self._values.buffers()
            return validity, data.slice(0, data.size - 1)

    class ShortDataBufferStorage:
        def __init__(self, storage):
            self._storage = storage
            self.values = ShortDataBufferValues(storage.values)

        def __getattr__(self, name):
            return getattr(self._storage, name)

    class ShortDataBufferChunk:
        def __init__(self, array):
            self._array = array
            self.storage = ShortDataBufferStorage(array.storage)

        def __getattr__(self, name):
            return getattr(self._array, name)

        def __len__(self):
            return len(self._array)

    assert (
        chunked_tensor_take._try_get_zero_copy_chunk_view(
            ShortDataBufferChunk(chunk),
            column.type,
            64,
            np.dtype(np.float32),
        )
        is None
    )


def test_local_shuffle_tensor_fallbacks_and_prepared_take_errors(monkeypatch):
    zero_column_table = pa.table({"value": [1, 2]}).select([])
    prepared_empty, empty_takes = _prepare_local_shuffle_arrow_table(zero_column_table)
    assert prepared_empty.num_rows == zero_column_table.num_rows
    assert not empty_takes

    single_chunk = pa.table({"value": [1, 2]})
    prepared_single, single_takes = _prepare_local_shuffle_arrow_table(single_chunk)
    assert prepared_single is single_chunk
    assert not single_takes

    plain = pa.table({"value": pa.chunked_array([pa.array([1, 2]), pa.array([3, 4])])})

    def fail_plain_tensor_prepare(*args, **kwargs):
        raise AssertionError("Plain Arrow fast path called tensor prepare")

    with monkeypatch.context() as plain_context:
        plain_context.setattr(
            batcher_module,
            "try_prepare_chunked_tensor_take",
            fail_plain_tensor_prepare,
        )
        prepared_plain, plain_takes = _prepare_local_shuffle_arrow_table(plain)
    assert not plain_takes
    assert prepared_plain.column("value").num_chunks == 1

    narrow, _ = _chunked_tensor(1024, 8, 4)
    prepared_narrow, narrow_takes = _prepare_local_shuffle_arrow_table(
        pa.table({"tensor": narrow}),
        expected_output_rows=128,
    )
    assert not narrow_takes
    assert prepared_narrow.column("tensor").num_chunks == 1

    nullable = _tensor_with_null(child_null=False)
    fallback_table, fallback_takes = _prepare_local_shuffle_arrow_table(
        pa.table({"tensor": nullable})
    )
    assert not fallback_takes
    assert fallback_table.column("tensor").num_chunks == 1

    table = _tensor_table(40, 64, 4)
    prepared_table, prepared_takes = _prepare_local_shuffle_arrow_table(table)
    assert prepared_takes
    indices = np.arange(10, dtype=np.int64)

    monkeypatch.setattr(
        chunked_tensor_take.PreparedChunkedTensorTake,
        "try_take",
        lambda *args, **kwargs: None,
    )
    assert _take_prepared_arrow_table(prepared_table, indices, prepared_takes) is None

    def raise_take(*args, **kwargs):
        raise RuntimeError("injected take failure")

    monkeypatch.setattr(
        chunked_tensor_take.PreparedChunkedTensorTake,
        "try_take",
        raise_take,
    )
    with pytest.raises(RuntimeError, match="injected take failure"):
        _take_prepared_arrow_table(prepared_table, indices, prepared_takes)


def test_local_shuffle_stops_retrying_invalid_prepared_take(monkeypatch):
    monkeypatch.setattr(
        batcher_module, "get_total_obj_store_mem_on_node", lambda: 1 << 60
    )
    monkeypatch.setattr(
        chunked_tensor_take,
        "_is_fast_take_worthwhile",
        lambda **kwargs: True,
    )
    original_combine = batcher_module.transform_pyarrow.combine_chunked_array
    combine_calls = 0

    def record_combine(column):
        nonlocal combine_calls
        combine_calls += 1
        return original_combine(column)

    monkeypatch.setattr(
        batcher_module.transform_pyarrow,
        "combine_chunked_array",
        record_combine,
    )
    batcher = ShufflingBatcher(
        batch_size=10,
        shuffle_buffer_min_size=10,
        shuffle_seed=0,
    )
    batcher.add(_tensor_table(40, 64, 4))
    batcher.done_adding()

    calls = 0

    def fail_prepared_table_take(*args, **kwargs):
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(
        batcher_module,
        "_take_prepared_arrow_table",
        fail_prepared_table_take,
    )

    assert len(batcher.next_batch()) == 10
    assert calls == 1
    assert not batcher._prepared_tensor_takes
    assert batcher._shuffle_buffer.column("tensor").num_chunks == 1
    assert combine_calls == 1

    assert len(batcher.next_batch()) == 10
    assert calls == 1
    assert combine_calls == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
