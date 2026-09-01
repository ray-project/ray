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

# Soft cap for temporary tensor payload produced by each gather subbatch. The
# final output and index, offset, and zero-copy view metadata are excluded. A
# source row is irreducible, so an oversized row uses a one-row subbatch and may
# exceed the cap.
TENSOR_TAKE_SCRATCH_CAP_BYTES = 8 * 1024 * 1024
# Narrow rows do not copy enough payload per grouped NumPy operation, while a
# small source column does not amortize preparation even when its rows are wide.
# Keep these operational gates independent of the scratch limit: an eligible
# source row may be larger than the soft scratch cap.
_MIN_FAST_ROW_BYTES = 256
_MIN_FAST_SOURCE_BYTES = 1024 * 1024
# Boolean-mask grouping is cheaper for a few chunks; sorting scales better once
# the number of chunks makes repeated full-subbatch masks expensive.
_MAX_MASK_GROUP_CHUNKS = 16


def is_chunked_tensor_take_enabled() -> bool:
    """Return whether callers should use the chunked tensor take fast path."""
    return ENABLE_CHUNKED_TENSOR_TAKE


def _passes_source_size_gates(source_rows: int, row_bytes: int) -> bool:
    """Return whether the source can amortize fast-path setup work."""
    return (
        row_bytes >= _MIN_FAST_ROW_BYTES
        and source_rows * row_bytes >= _MIN_FAST_SOURCE_BYTES
    )


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
            self._gather_into_output(output, indices)

        return self._wrap_tensor_output(output)

    def _gather_into_output(
        self,
        output: np.ndarray,
        indices: np.ndarray,
    ) -> None:
        """Gather normalized row indices into a preallocated tensor output.

        Each bounded subbatch maps every global row index to a source
        ``chunk_id`` and an index local to that chunk. The gather strategy then
        depends only on how those chunk IDs are arranged:

        * Monotonic chunk IDs already form contiguous chunk groups. They can be
          copied in output order and can use source slices for contiguous rows.
        * Unordered IDs over at most 16 chunks are grouped with masks. Repeated
          linear scans are cheaper than allocating and sorting an order array
          when the number of chunks is small.
        * Unordered IDs over more chunks sort output positions by chunk once,
          gather each resulting group, and scatter it back to the original
          positions. The temporary sort changes processing order, never
          caller-visible row order.

        Subbatching bounds the temporary chunk-ID, local-index, mask, and sort
        arrays. All three strategies write into the same preallocated output
        and preserve the order of ``indices``.

        Args:
            output: Destination tensor array.
            indices: Normalized global source-row indices.
        """
        for start in range(0, len(indices), self.subbatch_rows):
            stop = min(len(indices), start + self.subbatch_rows)
            subbatch_indices = indices[start:stop]
            output_slice = output[start:stop]
            chunk_ids = (
                np.searchsorted(self.chunk_starts, subbatch_indices, side="right") - 1
            )
            local_indices = subbatch_indices - self.chunk_starts[chunk_ids]

            if np.all(chunk_ids[1:] >= chunk_ids[:-1]):
                _gather_monotonic_chunk_ids(
                    output_slice,
                    local_indices,
                    self.chunk_views,
                    chunk_ids,
                )
            elif len(self.chunk_views) <= _MAX_MASK_GROUP_CHUNKS:
                _gather_by_chunk_masks(
                    output_slice,
                    local_indices,
                    self.chunk_views,
                    chunk_ids,
                )
            else:
                _gather_by_sorted_chunk_ids(
                    output_slice,
                    local_indices,
                    self.chunk_views,
                    chunk_ids,
                )

    def _wrap_tensor_output(self, output: np.ndarray) -> pa.Array:
        """Wrap the owned output buffer with the original Ray tensor type.

        Rebuilding data and offset arrays from buffers avoids another payload
        copy and preserves the caller-visible V1/V2 extension type and table
        schema.
        """
        scalar_type = self.tensor_type.storage_type.value_type
        data_array = pa.Array.from_buffers(
            scalar_type,
            output.size,
            [None, pa.py_buffer(output)],
        )
        offset_dtype = np.dtype(self.tensor_type.OFFSET_DTYPE.to_pandas_dtype())
        offsets = np.arange(
            0,
            (len(output) + 1) * self.values_per_row,
            self.values_per_row,
            dtype=offset_dtype,
        )
        storage = pa.Array.from_buffers(
            self.tensor_type.storage_type,
            len(output),
            [None, pa.py_buffer(offsets)],
            children=[data_array],
        )
        return self.tensor_type.wrap_array(storage)


def try_prepare_chunked_tensor_take(
    column: pa.ChunkedArray,
    *,
    expected_output_rows: Optional[int],
) -> Optional[PreparedChunkedTensorTake]:
    """Prepare an immutable chunked tensor source for repeated row takes.

    Args:
        column: Source chunked tensor column.
        expected_output_rows: Rows expected in each take. When provided,
            operational source-size gates are applied and Arrow output offset
            capacity is checked. Pass ``None`` explicitly for a capability-only
            plan.

    Returns:
        Validated source metadata when the fast path supports the column.
        Otherwise, ``None`` so the caller can use the standard Arrow fallback.

    This capability check is independent of the operational feature flag.
    Callers check ``is_chunked_tensor_take_enabled`` before performing any
    fast-path-specific index normalization or column preparation.
    """
    if column.num_chunks <= 1 or column.null_count > 0:
        return None

    tensor_type = column.type
    layout = _try_get_tensor_layout(tensor_type)
    if layout is None:
        return None
    values_per_row, row_bytes, value_dtype = layout

    if expected_output_rows is not None and not _passes_source_size_gates(
        len(column), row_bytes
    ):
        return None

    offset_dtype = np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype())
    max_supported_output_rows = np.iinfo(offset_dtype).max // values_per_row
    if (
        expected_output_rows is not None
        and expected_output_rows > max_supported_output_rows
    ):
        return None

    chunks = tuple(chunk for chunk in column.chunks if len(chunk) > 0)
    if len(chunks) <= 1:
        return None

    subbatch_rows = max(
        1,
        TENSOR_TAKE_SCRATCH_CAP_BYTES // row_bytes,
    )

    chunk_views = []
    chunk_starts = []
    row_offset = 0
    for chunk in chunks:
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
    contiguity, ownership, and buffer bounds explicit. The logical list
    offsets are authoritative: a legal array may start after child element 0,
    while malformed or variable-stride offsets cannot represent the declared
    fixed tensor shape and must fall back.
    """
    values = chunk.storage.values
    # Preserve the existing fallback for child arrays whose logical data starts
    # inside a larger values array.
    if values.offset != 0 or values.null_count > 0:
        return None

    storage = chunk.storage
    try:
        offsets = storage.offsets.to_numpy(zero_copy_only=True)
    except (AttributeError, pa.ArrowException, TypeError, ValueError):
        return None
    if offsets.ndim != 1 or len(offsets) != len(chunk) + 1:
        return None

    first_value = int(offsets[0])
    last_value = first_value + len(chunk) * values_per_row
    if first_value < 0 or int(offsets[-1]) != last_value or last_value > len(values):
        return None
    if not np.all(np.diff(offsets.astype(np.int64, copy=False)) == values_per_row):
        return None

    buffers = values.buffers()
    if len(buffers) < 2 or buffers[1] is None:
        return None
    data_buffer = buffers[1]
    byte_offset = first_value * value_dtype.itemsize
    view_nbytes = len(chunk) * values_per_row * value_dtype.itemsize
    try:
        buffer_size = data_buffer.size
    except (AttributeError, TypeError):
        return None
    if byte_offset > buffer_size or view_nbytes > buffer_size - byte_offset:
        return None
    try:
        return np.ndarray(
            (len(chunk), *tensor_type.shape),
            dtype=value_dtype,
            buffer=data_buffer,
            offset=byte_offset,
        )
    except (TypeError, ValueError):
        return None


def _gather_monotonic_chunk_ids(
    output: np.ndarray,
    local_indices: np.ndarray,
    chunks: tuple[np.ndarray, ...],
    chunk_ids: np.ndarray,
) -> None:
    """Gather chunk groups that already occur in nondecreasing chunk order.

    A change in ``chunk_ids`` marks a group boundary. Because every group
    occupies a contiguous output range, it can be written without sorting or
    scattering. Consecutive local row indices use a source slice; other rows
    use NumPy advanced indexing.

    Args:
        output: Destination for this subbatch.
        local_indices: Source-row indices relative to their chunks.
        chunks: Zero-copy NumPy views of the source tensor chunks.
        chunk_ids: Nondecreasing source chunk IDs for each output position.
    """
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
    """Gather unordered rows by scanning positions for each present chunk.

    ``bincount`` first avoids visiting chunks absent from this subbatch. For
    every present chunk, an equality mask finds its original output positions;
    the corresponding local rows are gathered together and written back to
    those positions. This preserves row order without sorting.

    The work is proportional to the number of present chunks times the
    subbatch size, so the caller reserves this strategy for at most 16 chunks.

    Args:
        output: Destination for this subbatch.
        local_indices: Source-row indices relative to their chunks.
        chunks: Zero-copy NumPy views of the source tensor chunks.
        chunk_ids: Potentially unordered source chunk IDs for each output
            position.
    """
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
    """Gather unordered rows after sorting positions into chunk groups.

    ``argsort`` returns output positions ordered by source chunk. Equal chunk
    IDs then form contiguous processing groups, so each source chunk is gathered
    once. Results are scattered through the saved original positions, preserving
    the caller-visible row order. A stable sort is unnecessary because every
    gathered row is written to its own original position.

    Sorting costs ``O(K log K)`` for ``K`` subbatch rows, but avoids one full
    ``chunk_ids`` scan per chunk and therefore scales better for many chunks.

    Args:
        output: Destination for this subbatch.
        local_indices: Source-row indices relative to their chunks.
        chunks: Zero-copy NumPy views of the source tensor chunks.
        chunk_ids: Potentially unordered source chunk IDs for each output
            position.
    """
    order = np.argsort(chunk_ids, kind="quicksort")
    sorted_chunk_ids = chunk_ids[order]
    boundaries = np.flatnonzero(sorted_chunk_ids[1:] != sorted_chunk_ids[:-1]) + 1
    group_start = 0
    for group_stop in chain(boundaries, (len(order),)):
        positions = order[group_start:group_stop]
        chunk_id = chunk_ids[positions[0]]
        output[positions] = chunks[chunk_id][local_indices[positions]]
        group_start = group_stop
