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
# Conservative per-row budget for temporary chunk IDs, local indices, masks,
# ordering positions, and NumPy comparison results used while gathering.
_INDEX_SCRATCH_BYTES_PER_ROW = 64


class PreparedChunkedTensorTake(NamedTuple):
    """Executable tensor take prepared from one immutable chunked column."""

    tensor_type: Any
    max_supported_output_rows: int
    values_per_row: int
    value_dtype: np.dtype
    subbatch_rows: int
    chunk_views: tuple[np.ndarray, ...]
    chunk_starts: np.ndarray

    def try_take(self, indices: np.ndarray) -> Optional[pa.Array]:
        """Take normalized rows, or return ``None`` for a column fallback.

        ``indices`` must be a one-dimensional, native ``np.int64`` array whose
        values are within the source column's bounds. Callers establish this
        invariant once before applying the same indices to multiple columns.
        This method only checks the output-size limit derived from this tensor
        column's Arrow offset type; rescanning indices here would duplicate
        caller work for every prepared column and every take.
        """
        if len(indices) > self.max_supported_output_rows:
            return None

        output = np.empty(
            (len(indices), *self.tensor_type.shape),
            dtype=self.value_dtype,
        )
        if len(indices) > 0:
            _gather_into_output(
                output,
                indices,
                self.chunk_views,
                self.chunk_starts,
                self.subbatch_rows,
            )

        return _wrap_tensor_output(output, self.tensor_type, self.values_per_row)


def try_prepare_chunked_tensor_take(
    column: pa.ChunkedArray,
) -> Optional[PreparedChunkedTensorTake]:
    """Prepare an immutable chunked tensor source for repeated row takes.

    Args:
        column: Source chunked tensor column.

    Returns:
        Validated source metadata when the fast path supports the column.
        Otherwise, ``None`` so the caller can use the standard Arrow fallback.
    """
    if not ENABLE_CHUNKED_TENSOR_TAKE or column.num_chunks <= 1:
        return None

    tensor_type = column.type
    if column.null_count > 0:
        return None

    layout = _try_get_tensor_layout(tensor_type)
    if layout is None:
        return None
    values_per_row, row_bytes, value_dtype = layout
    if row_bytes < _MIN_FAST_ROW_BYTES:
        return None

    subbatch_rows = TENSOR_TAKE_SCRATCH_CAP_BYTES // (
        row_bytes + _INDEX_SCRATCH_BYTES_PER_ROW
    )
    if subbatch_rows == 0:
        return None

    offset_dtype = np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype())
    max_supported_output_rows = np.iinfo(offset_dtype).max // values_per_row

    chunk_views = []
    chunk_starts = []
    row_offset = 0
    for chunk in column.chunks:
        if len(chunk) == 0:
            continue
        view = _try_get_zero_copy_chunk_view(
            chunk,
            tensor_type,
            values_per_row,
            value_dtype,
        )
        if view is None:
            return None
        chunk_views.append(view)
        chunk_starts.append(row_offset)
        row_offset += len(chunk)

    return PreparedChunkedTensorTake(
        tensor_type=tensor_type,
        max_supported_output_rows=max_supported_output_rows,
        values_per_row=values_per_row,
        value_dtype=value_dtype,
        subbatch_rows=subbatch_rows,
        chunk_views=tuple(chunk_views),
        chunk_starts=np.asarray(chunk_starts, dtype=np.int64),
    )


def try_take_chunked_tensor(
    column: pa.ChunkedArray,
    indices: np.ndarray,
) -> Optional[pa.Array]:
    """Take rows from an eligible Ray tensor column without concatenating chunks.

    Args:
        column: Source chunked tensor column.
        indices: One-dimensional, native ``np.int64`` row indices already
            validated to be within the bounds of ``column``.

    Returns:
        A single tensor extension array when the fast path supports the input.
        Otherwise, ``None`` so the caller can use Ray's standard column path.

    Index type, shape, and bounds are caller invariants rather than fallback
    conditions here. The fast path supports non-null, numeric, fixed-shape Ray
    V1/V2 tensor columns. It gathers from validated zero-copy chunk views into
    one output buffer in bounded subbatches. Unexpected allocation and internal
    errors propagate.
    """
    prepared = try_prepare_chunked_tensor_take(column)
    return prepared.try_take(indices) if prepared is not None else None


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


def _try_get_zero_copy_chunk_view(
    chunk,
    tensor_type,
    values_per_row,
    value_dtype,
):
    """Return a safe zero-copy tensor view, or ``None`` for column fallback.

    Constructing the view from the numeric child buffer makes its shape, dtype,
    contiguity, ownership, and pointer bounds explicit. NumPy rejects truncated
    buffers instead of letting an invalid view reach the gather kernel.
    """
    values = chunk.storage.values
    # Preserve the existing fallback for child arrays whose logical data starts
    # inside a larger values array.
    if values.offset != 0 or values.null_count > 0:
        return None

    buffers = values.buffers()
    if len(buffers) < 2 or buffers[1] is None:
        return None
    byte_offset = chunk.offset * values_per_row * value_dtype.itemsize
    try:
        return np.ndarray(
            (len(chunk), *tensor_type.shape),
            dtype=value_dtype,
            buffer=buffers[1],
            offset=byte_offset,
        )
    except (TypeError, ValueError):
        return None


def _gather_into_output(
    output: np.ndarray,
    indices: np.ndarray,
    chunks: tuple[np.ndarray, ...],
    chunk_starts: np.ndarray,
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
        chunk_ids = np.searchsorted(chunk_starts, subbatch_indices, side="right") - 1
        local_indices = subbatch_indices - chunk_starts[chunk_ids]

        if np.all(chunk_ids[1:] >= chunk_ids[:-1]):
            _gather_monotonic_chunk_ids(
                output_slice,
                local_indices,
                chunks,
                chunk_ids,
            )
        elif len(chunks) <= _MAX_MASK_GROUP_CHUNKS:
            _gather_by_chunk_masks(
                output_slice,
                local_indices,
                chunks,
                chunk_ids,
            )
        else:
            _gather_by_sorted_chunk_ids(
                output_slice,
                local_indices,
                chunks,
                chunk_ids,
            )


def _gather_monotonic_chunk_ids(
    output: np.ndarray,
    local_indices: np.ndarray,
    chunks: tuple[np.ndarray, ...],
    chunk_ids: np.ndarray,
) -> None:
    """Gather ordered chunk groups, using source slices when rows are contiguous."""
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
            output[group_start:group_stop] = chunks[chunk_id][source_start:source_stop]
        else:
            output[group_start:group_stop] = chunks[chunk_id][group_indices]
        group_start = group_stop


def _gather_by_chunk_masks(
    output: np.ndarray,
    local_indices: np.ndarray,
    chunks: tuple[np.ndarray, ...],
    chunk_ids: np.ndarray,
) -> None:
    """Gather unordered rows by masking each present chunk."""
    present_chunk_ids = np.flatnonzero(np.bincount(chunk_ids, minlength=len(chunks)))
    for chunk_id in present_chunk_ids:
        positions = np.flatnonzero(chunk_ids == chunk_id)
        output[positions] = chunks[chunk_id][local_indices[positions]]


def _gather_by_sorted_chunk_ids(
    output: np.ndarray,
    local_indices: np.ndarray,
    chunks: tuple[np.ndarray, ...],
    chunk_ids: np.ndarray,
) -> None:
    """Gather unordered rows by sorting output positions into chunk groups."""
    order = np.argsort(chunk_ids, kind="quicksort")
    sorted_chunk_ids = chunk_ids[order]
    boundaries = np.flatnonzero(sorted_chunk_ids[1:] != sorted_chunk_ids[:-1]) + 1
    group_start = 0
    for group_stop in chain(boundaries, (len(order),)):
        positions = order[group_start:group_stop]
        chunk_id = chunk_ids[positions[0]]
        output[positions] = chunks[chunk_id][local_indices[positions]]
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
