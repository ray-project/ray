import math
from itertools import chain
from typing import Any, NamedTuple, Optional

import numpy as np
import pyarrow as pa

from ray._common.utils import env_bool
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorType,
    ArrowTensorTypeV2,
)

ENABLE_CHUNKED_TENSOR_TAKE = env_bool(
    "RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE",
    True,
)

# Cap for simultaneously allocated per-subbatch NumPy gather/index buffers.
# The final output, Arrow offsets, and zero-copy chunk-view metadata are excluded.
TENSOR_TAKE_SCRATCH_CAP_BYTES = 8 * 1024 * 1024
# Narrow columns stay on Ray's native path because grouping can cost more than
# the full-column copy it avoids.
_MIN_FAST_ROW_BYTES = 256
# Boolean-mask grouping is cheaper for a few chunks; sorting scales better once
# the number of chunks makes repeated full-subbatch masks expensive.
_MAX_MASK_GROUP_CHUNKS = 16
_INDEX_SCRATCH_BYTES_PER_ROW = 64
_OFFSET_VALIDATION_BLOCK_BYTES = 8 * 1024 * 1024


class _ChunkedTensorTakePlan(NamedTuple):
    """Runtime plan for one chunked tensor take."""

    values_per_row: int
    value_dtype: np.dtype
    subbatch_rows: int
    scratch_bytes: int


class PreparedChunkedTensorTakePlan(NamedTuple):
    """Validated source metadata reusable across takes from one immutable column."""

    tensor_type: Any
    input_rows: int
    max_output_rows: int
    values_per_row: int
    row_bytes: int
    value_dtype: np.dtype
    subbatch_rows: int
    chunk_views: tuple[np.ndarray, ...]
    chunk_starts: np.ndarray
    chunk_ends: np.ndarray


def _try_get_chunked_tensor_take_plan(
    tensor_type: Any,
    *,
    input_rows: int,
    output_rows: int,
    chunk_count: int,
) -> Optional[_ChunkedTensorTakePlan]:
    """Plan an eligible take using the same policy as the runtime fast path.

    Args:
        tensor_type: Ray Arrow tensor extension type to inspect.
        input_rows: Number of rows in the source column.
        output_rows: Maximum number of rows in one take.
        chunk_count: Number of source chunks.

    Returns:
        A runtime plan when the input is eligible. Otherwise, ``None``. The
        scratch estimate covers only simultaneously allocated per-subbatch
        gather and index buffers; it excludes final output, Arrow offsets, and
        chunk-view metadata.
    """
    layout = _try_get_tensor_layout(tensor_type)
    if layout is None or input_rows < 0 or output_rows < 0 or chunk_count <= 1:
        return None

    values_per_row, row_bytes, value_dtype = layout
    if row_bytes < _MIN_FAST_ROW_BYTES:
        return None
    if not _offsets_can_represent_output(tensor_type, output_rows, values_per_row):
        return None

    subbatch_rows = _tensor_take_subbatch_rows(output_rows, row_bytes)
    if output_rows > 0 and subbatch_rows == 0:
        return None
    return _ChunkedTensorTakePlan(
        values_per_row=values_per_row,
        value_dtype=value_dtype,
        subbatch_rows=subbatch_rows,
        scratch_bytes=subbatch_rows * (row_bytes + _INDEX_SCRATCH_BYTES_PER_ROW),
    )


def try_prepare_chunked_tensor_take(
    column: pa.ChunkedArray,
    *,
    max_output_rows: int,
) -> Optional[PreparedChunkedTensorTakePlan]:
    """Prepare an immutable chunked tensor source for repeated row takes.

    Args:
        column: Source chunked tensor column.
        max_output_rows: Maximum number of rows in any subsequent take.

    Returns:
        Validated source metadata when the fast path supports the column.
        Otherwise, ``None`` so the caller can use the standard Arrow fallback.
    """
    if not ENABLE_CHUNKED_TENSOR_TAKE:
        return None

    tensor_type = column.type
    if column.null_count > 0:
        return None

    runtime_plan = _try_get_chunked_tensor_take_plan(
        tensor_type,
        input_rows=len(column),
        output_rows=max_output_rows,
        chunk_count=column.num_chunks,
    )
    if runtime_plan is None:
        return None

    layout = _try_get_tensor_layout(tensor_type)
    assert layout is not None
    values_per_row, row_bytes, value_dtype = layout
    chunk_views = []
    chunk_starts = []
    chunk_ends = []
    row_offset = 0
    for chunk in column.chunks:
        view = _try_get_zero_copy_chunk_view(
            chunk, tensor_type, values_per_row, value_dtype
        )
        if view is None:
            return None
        if len(chunk) == 0:
            continue
        chunk_views.append(view)
        chunk_starts.append(row_offset)
        row_offset += len(chunk)
        chunk_ends.append(row_offset)

    if row_offset != len(column):
        return None

    return PreparedChunkedTensorTakePlan(
        tensor_type=tensor_type,
        input_rows=len(column),
        max_output_rows=max_output_rows,
        values_per_row=values_per_row,
        row_bytes=row_bytes,
        value_dtype=value_dtype,
        subbatch_rows=runtime_plan.subbatch_rows,
        chunk_views=tuple(chunk_views),
        chunk_starts=np.asarray(chunk_starts, dtype=np.int64),
        chunk_ends=np.asarray(chunk_ends, dtype=np.int64),
    )


def try_take_prepared_chunked_tensor(
    plan: PreparedChunkedTensorTakePlan,
    normalized_indices: np.ndarray,
) -> Optional[pa.Array]:
    """Take rows using source validation cached in a prepared plan.

    Args:
        plan: Previously validated source metadata.
        normalized_indices: One-dimensional, native-endian ``np.int64`` row
            indices.

    Returns:
        A single tensor extension array, or ``None`` when the indices violate
        the prepared plan's contract.
    """
    if (
        not _is_normalized_indices(normalized_indices)
        or len(normalized_indices) > plan.max_output_rows
    ):
        return None
    if len(normalized_indices) > 0 and (
        normalized_indices.min() < 0 or normalized_indices.max() >= plan.input_rows
    ):
        return None

    output = np.empty(
        (len(normalized_indices), *plan.tensor_type.shape),
        dtype=plan.value_dtype,
    )
    if len(normalized_indices) > 0:
        if not plan.chunk_views:
            return None
        _gather_into_output(
            output,
            normalized_indices,
            plan.chunk_views,
            plan.chunk_starts,
            plan.chunk_ends,
            plan.subbatch_rows,
        )

    return _wrap_tensor_output(output, plan.tensor_type, plan.values_per_row)


def try_take_chunked_tensor(
    column: pa.ChunkedArray,
    normalized_indices: np.ndarray,
) -> Optional[pa.Array]:
    """Take rows from an eligible Ray tensor column without concatenating chunks.

    Args:
        column: Source chunked tensor column.
        normalized_indices: One-dimensional, native-endian ``np.int64`` row
            indices.

    Returns:
        A single tensor extension array when the fast path supports the input.
        Otherwise, ``None`` so the caller can use Ray's standard column path.

    The fast path supports non-null, numeric, fixed-shape Ray V1/V2 tensor
    columns. It gathers from validated zero-copy chunk views into one output
    buffer in bounded subbatches. Unexpected allocation and internal errors
    propagate.
    """
    if not _is_normalized_indices(normalized_indices):
        return None
    plan = try_prepare_chunked_tensor_take(
        column,
        max_output_rows=len(normalized_indices),
    )
    return (
        try_take_prepared_chunked_tensor(plan, normalized_indices)
        if plan is not None
        else None
    )


def _tensor_take_subbatch_rows(total_rows: int, tensor_row_bytes: int) -> int:
    """Choose the largest gather subbatch that fits the scratch-byte budget.

    Args:
        total_rows: Number of output rows.
        tensor_row_bytes: Tensor payload bytes per row.

    Returns:
        The maximum rows to gather at once. The estimate covers the selected
        payload and temporary index/grouping arrays, but excludes final output
        and Arrow offsets.

    Raises:
        ValueError: If ``tensor_row_bytes`` is not positive for nonempty output.
    """
    if total_rows <= 0:
        return 0
    if tensor_row_bytes <= 0:
        raise ValueError("tensor_row_bytes must be positive")

    bytes_per_row = tensor_row_bytes + _INDEX_SCRATCH_BYTES_PER_ROW
    return min(total_rows, TENSOR_TAKE_SCRATCH_CAP_BYTES // bytes_per_row)


def _try_get_tensor_layout(tensor_type):
    """Return fixed numeric tensor layout metadata, or ``None`` if unsupported.

    The returned tuple is ``(values_per_row, row_bytes, numpy_dtype)``. Rejecting
    unsupported scalar types or shapes keeps the fast path independent of object
    conversion and variable-shape tensor semantics.
    """
    if not isinstance(tensor_type, (ArrowTensorType, ArrowTensorTypeV2)):
        return None

    scalar_type = tensor_type.storage_type.value_type
    if not (pa.types.is_integer(scalar_type) or pa.types.is_floating(scalar_type)):
        return None
    if scalar_type.bit_width % 8 != 0:
        return None

    shape = tensor_type.shape
    if any(not isinstance(dimension, int) or dimension < 0 for dimension in shape):
        return None
    values_per_row = math.prod(shape)
    if values_per_row <= 0:
        return None

    try:
        value_dtype = np.dtype(scalar_type.to_pandas_dtype())
    except (NotImplementedError, TypeError, ValueError):
        return None
    if value_dtype.hasobject or value_dtype.itemsize * 8 != scalar_type.bit_width:
        return None

    return values_per_row, values_per_row * value_dtype.itemsize, value_dtype


def _is_normalized_indices(indices) -> bool:
    """Check the internal index contract established by the table wrapper."""
    return (
        isinstance(indices, np.ndarray)
        and indices.ndim == 1
        and indices.dtype == np.dtype(np.int64)
    )


def _offsets_can_represent_output(
    tensor_type, output_rows: int, values_per_row: int
) -> bool:
    """Check whether the original V1/V2 offset dtype can encode the output."""
    offset_dtype = np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype())
    return output_rows <= np.iinfo(offset_dtype).max // values_per_row


def _try_get_zero_copy_chunk_view(chunk, tensor_type, values_per_row, value_dtype):
    """Return a safe zero-copy tensor view, or ``None`` for column fallback.

    Validation covers extension type identity, row/scalar nulls, sliced-array offset
    origins, fixed-width row offsets, NumPy shape/dtype/contiguity, buffer ownership,
    and the final pointer range. These checks prevent a view from silently copying or
    addressing bytes outside the Arrow value buffer.
    """
    if chunk.type != tensor_type or chunk.null_count > 0:
        return None

    storage = chunk.storage
    values = storage.values
    if values.offset != 0:
        return None
    offsets = storage.offsets
    if offsets.null_count > 0:
        return None
    try:
        offset_values = offsets.to_numpy(zero_copy_only=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return None

    if len(offset_values) != len(chunk) + 1:
        return None
    expected_start = chunk.offset * values_per_row
    if int(offset_values[0]) != expected_start:
        return None
    if int(offset_values[-1]) - expected_start != len(chunk) * values_per_row:
        return None
    if not _has_fixed_width_offsets(offset_values, values_per_row):
        return None

    value_start = int(offset_values[0])
    value_count = int(offset_values[-1]) - value_start
    if values.slice(value_start, value_count).null_count > 0:
        return None

    try:
        view = chunk.to_numpy(zero_copy_only=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return None

    if (
        view.shape != (len(chunk), *tensor_type.shape)
        or view.dtype != value_dtype
        or not view.flags.c_contiguous
        or view.flags.owndata
    ):
        return None

    buffers = chunk.buffers()
    if len(buffers) < 4 or buffers[3] is None:
        return None
    data_buffer = buffers[3]
    data_address = view.__array_interface__["data"][0]
    if (
        data_address < data_buffer.address
        or data_address + view.nbytes > data_buffer.address + data_buffer.size
    ):
        return None
    return view


def _has_fixed_width_offsets(offsets: np.ndarray, values_per_row: int) -> bool:
    """Validate fixed-size row offsets without allocating one full diff array."""
    comparison_rows = max(
        1,
        _OFFSET_VALIDATION_BLOCK_BYTES // (offsets.dtype.itemsize + 1),
    )
    for start in range(0, len(offsets) - 1, comparison_rows):
        stop = min(len(offsets) - 1, start + comparison_rows)
        if not np.all(
            offsets[start + 1 : stop + 1] - offsets[start:stop] == values_per_row
        ):
            return False
    return True


def _gather_into_output(
    output: np.ndarray,
    indices: np.ndarray,
    chunks: tuple[np.ndarray, ...],
    chunk_starts: np.ndarray,
    chunk_ends: np.ndarray,
    subbatch_rows: int,
) -> None:
    """Gather normalized row indices into a preallocated tensor output.

    Each bounded subbatch maps global rows to source chunks once. Monotonic chunk IDs
    preserve output order and use contiguous source slices when possible. Unordered
    takes use mask grouping for at most 16 chunks; larger chunk sets sort output
    positions once and scatter each chunk group back to its original positions.
    """
    for start in range(0, len(indices), subbatch_rows):
        stop = min(len(indices), start + subbatch_rows)
        subbatch_indices = indices[start:stop]
        output_slice = output[start:stop]
        chunk_ids = np.searchsorted(chunk_ends, subbatch_indices, side="right")
        local_indices = subbatch_indices - chunk_starts[chunk_ids]

        # Ordered chunk IDs need no sorting and may collapse to direct slices.
        if np.all(chunk_ids[1:] >= chunk_ids[:-1]):
            boundaries = np.flatnonzero(chunk_ids[1:] != chunk_ids[:-1]) + 1
            group_start = 0
            # Add the final sentinel lazily instead of materializing a Python tuple
            # proportional to the number of chunk groups.
            for group_stop in chain(boundaries, (len(chunk_ids),)):
                chunk_id = chunk_ids[group_start]
                group_indices = local_indices[group_start:group_stop]
                if len(group_indices) <= 1 or np.all(
                    group_indices[1:] == group_indices[:-1] + 1
                ):
                    source_start = int(group_indices[0])
                    source_stop = source_start + len(group_indices)
                    output_slice[group_start:group_stop] = chunks[chunk_id][
                        source_start:source_stop
                    ]
                else:
                    output_slice[group_start:group_stop] = chunks[chunk_id][
                        group_indices
                    ]
                group_start = group_stop
            continue

        # For a few chunks, masks avoid the fixed cost of sorting every row.
        if len(chunks) <= _MAX_MASK_GROUP_CHUNKS:
            present_chunk_ids = np.flatnonzero(
                np.bincount(chunk_ids, minlength=len(chunks))
            )
            for chunk_id in present_chunk_ids:
                positions = np.flatnonzero(chunk_ids == chunk_id)
                output_slice[positions] = chunks[chunk_id][local_indices[positions]]
            continue

        # For many chunks, sort positions once to avoid one full mask per chunk.
        order = np.argsort(chunk_ids, kind="quicksort")
        sorted_chunk_ids = chunk_ids[order]
        boundaries = np.flatnonzero(sorted_chunk_ids[1:] != sorted_chunk_ids[:-1]) + 1
        group_start = 0
        for group_stop in chain(boundaries, (len(order),)):
            positions = order[group_start:group_stop]
            chunk_id = chunk_ids[positions[0]]
            output_slice[positions] = chunks[chunk_id][local_indices[positions]]
            group_start = group_stop


def _wrap_tensor_output(output: np.ndarray, tensor_type, values_per_row: int):
    """Wrap the owned NumPy output buffer with the original Ray tensor type.

    Rebuilding data and offset arrays from buffers avoids another payload copy and
    preserves the caller-visible V1/V2 extension type and table schema.
    """
    scalar_type = tensor_type.storage_type.value_type
    data_array = pa.Array.from_buffers(
        scalar_type,
        output.size,
        [None, pa.py_buffer(output)],
    )
    offset_dtype = np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype())
    offsets = np.arange(
        0,
        (len(output) + 1) * values_per_row,
        values_per_row,
        dtype=offset_dtype,
    )
    storage = pa.Array.from_buffers(
        tensor_type.storage_type,
        len(output),
        [None, pa.py_buffer(offsets)],
        children=[data_array],
    )
    return tensor_type.wrap_array(storage)
