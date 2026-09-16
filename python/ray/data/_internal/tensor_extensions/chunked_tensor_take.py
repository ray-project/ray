import logging
import math
from enum import Enum
from itertools import chain
from typing import Any, NamedTuple, Optional, Tuple, Union

import numpy as np
import pyarrow as pa

from ray._common.utils import env_bool
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorType,
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorType,
)

logger = logging.getLogger(__name__)

ENABLE_CHUNKED_TENSOR_TAKE = env_bool(
    "RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE",
    True,
)

# Soft cap for temporary payload in each fixed-shape gather subbatch. The
# final output and index, offset, and zero-copy view metadata are excluded. A
# source row is irreducible, so an oversized row uses a one-row subbatch and may
# exceed the cap.
FIXED_TENSOR_TAKE_SCRATCH_CAP_BYTES = 8 * 1024 * 1024
# Narrow rows do not copy enough payload per grouped NumPy operation, while a
# small source column does not amortize preparation even when its rows are wide.
# Keep these operational gates independent of the scratch limit: an eligible
# source row may be larger than the soft scratch cap.
_MIN_FIXED_ROW_BYTES = 1024
_MIN_FIXED_PAYLOAD_BYTES = 1024 * 1024
# Preparation also has fixed work per physical source chunk, including the
# empty chunks it must inspect. Require enough source payload per chunk unless
# the requested output itself is large enough to amortize that work.
_MIN_FIXED_SOURCE_BYTES_PER_CHUNK = 128 * 1024
# Variable-shaped rows need a Python slice copy per selected row. Require more
# payload per source row than the vectorized fixed-shape gather to amortize it.
# This is a source average, including zero-length rows, not a minimum row size.
# Oversampling also uses this budget per output row, unless every source row
# meets it independently of which indices the request selects.
_MIN_VARIABLE_ROW_BYTES = 8 * 1024
# Variable preparation also validates shapes and two sets of offsets per chunk.
# Keep small sources on Arrow: avoiding their copy does not repay that setup.
_MIN_VARIABLE_PAYLOAD_BYTES = 8 * 1024 * 1024
# For small requests, require an average of 2 MiB of source payload per physical
# chunk, including empty chunks. A larger estimated output can amortize setup
# at an average of 512 KiB per physical chunk.
# Local shuffle's full-generation bound accounts for reuse across its batches.
_MIN_VARIABLE_SOURCE_BYTES_PER_CHUNK = 2 * 1024 * 1024
_MIN_VARIABLE_OUTPUT_BYTES_PER_CHUNK = 512 * 1024


class _TakeFallbackReason(str, Enum):
    """Reasons column preparation or request normalization declines the fast path.

    Tensor layout covers Ray fixed/variable tensor types and numeric scalars.
    Chunk storage covers child offsets/nulls, logical offsets, and buffer bounds.
    Unsupported indices include invalid values as well as unsupported types.
    """

    FEATURE_DISABLED = "feature_disabled"
    SINGLE_CHUNK = "single_chunk"
    CONTAINS_NULLS = "contains_nulls"
    UNSUPPORTED_TENSOR_LAYOUT = "unsupported_tensor_layout"
    BELOW_SIZE_THRESHOLD = "below_size_threshold"
    OUTPUT_OFFSET_OVERFLOW = "output_offset_overflow"
    FEWER_THAN_TWO_NONEMPTY_CHUNKS = "fewer_than_two_nonempty_chunks"
    UNSAFE_CHUNK_STORAGE = "unsafe_chunk_storage"
    UNSUPPORTED_INDICES = "unsupported_indices"


def try_prepare_chunked_tensor_take(
    column: pa.ChunkedArray,
    *,
    max_output_rows: int,
) -> Optional["PreparedTensorTake"]:
    """Authoritatively select and prepare the chunked tensor take fast path.

    This function owns every column-level and operational eligibility rule:

    * The feature flag must be enabled.
    * The column must be a non-null numeric Ray tensor with at
      least two nonempty chunks.
    * Its row size (averaged for variable shapes) and total source payload must
      be large enough, and either its per-chunk source payload or requested
      output must amortize preparation.
    * Its declared maximum output must fit the tensor type's Arrow offsets.
    * Every chunk must have valid logical offsets and expose a safe zero-copy
      view within the numeric child buffer.

    Callers may identify broad multi-chunk extension candidates as part of
    table-level routing, but that does not establish fast-path eligibility.
    Request-level index handling also stays outside this column preparation:
    ``take_table`` normalizes external indices once after a plan is available,
    while local shuffle already owns a valid native ``int64`` permutation.

    A returned plan may take any valid normalized index array containing at
    most ``max_output_rows`` rows. All eligibility checks happen here: once
    preparation succeeds, the returned plan's ``take`` method never falls
    back to Arrow's standard path.

    Args:
        column: Source chunked tensor column.
        max_output_rows: Maximum number of rows in any take using the returned
            plan. This is the request size for ``take_table`` and the complete
            shuffle-generation size for local shuffle.

    Returns:
        Validated source metadata when the fast path supports the column.
        Otherwise, ``None`` so the caller can use the standard Arrow fallback.
    """
    if not ENABLE_CHUNKED_TENSOR_TAKE:
        return _log_take_fallback(_TakeFallbackReason.FEATURE_DISABLED, column=column)
    if column.num_chunks <= 1:
        return _log_take_fallback(_TakeFallbackReason.SINGLE_CHUNK, column=column)
    if column.null_count > 0:
        return _log_take_fallback(_TakeFallbackReason.CONTAINS_NULLS, column=column)

    tensor_type = column.type
    if isinstance(tensor_type, ArrowVariableShapedTensorType):
        return _try_prepare_variable_tensor_take(column, max_output_rows)
    layout = _prepare_fixed_tensor_layout(tensor_type)
    if layout is None:
        return _log_take_fallback(
            _TakeFallbackReason.UNSUPPORTED_TENSOR_LAYOUT, column=column
        )
    values_per_row, row_bytes, value_dtype = layout

    if not _passes_fixed_size_gates(
        source_rows=len(column),
        row_bytes=row_bytes,
        source_chunks=column.num_chunks,
        max_output_rows=max_output_rows,
    ):
        return _log_take_fallback(
            _TakeFallbackReason.BELOW_SIZE_THRESHOLD, column=column
        )

    offset_dtype = np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype())
    offset_capacity_rows = np.iinfo(offset_dtype).max // values_per_row
    if max_output_rows > offset_capacity_rows:
        return _log_take_fallback(
            _TakeFallbackReason.OUTPUT_OFFSET_OVERFLOW, column=column
        )

    chunks = tuple(chunk for chunk in column.chunks if len(chunk) > 0)
    if len(chunks) <= 1:
        return _log_take_fallback(
            _TakeFallbackReason.FEWER_THAN_TWO_NONEMPTY_CHUNKS, column=column
        )

    subbatch_rows = max(
        1,
        FIXED_TENSOR_TAKE_SCRATCH_CAP_BYTES // row_bytes,
    )

    chunk_views = []
    chunk_starts = []
    row_offset = 0
    for chunk in chunks:
        view = _prepare_fixed_chunk_view(
            chunk,
            tensor_type,
            values_per_row,
            value_dtype,
        )
        if view is None:
            return _log_take_fallback(
                _TakeFallbackReason.UNSAFE_CHUNK_STORAGE, column=column
            )
        chunk_views.append(view)
        chunk_starts.append(row_offset)
        row_offset += len(chunk)

    plan = PreparedFixedShapedTensorTake(
        tensor_type=tensor_type,
        values_per_row=values_per_row,
        value_dtype=value_dtype,
        subbatch_rows=subbatch_rows,
        chunk_views=tuple(chunk_views),
        chunk_starts=np.asarray(chunk_starts, dtype=np.int64),
    )
    logger.debug(
        "Chunked tensor take fast path prepared: rows=%s, chunks=%s, "
        "row_bytes=%s, max_output_rows=%s, subbatch_rows=%s",
        len(column),
        len(chunks),
        row_bytes,
        max_output_rows,
        subbatch_rows,
    )
    return plan


def _log_take_fallback(
    reason: _TakeFallbackReason, *, column: Optional[pa.ChunkedArray] = None
) -> None:
    """Debug-log a column or request rejection and return the fallback value.

    Args:
        reason: Why the fast path declined the column or request.
        column: Rejected source column, when the reason is column-specific.
            Omit for request-level failures such as unsupported indices.
    """
    if column is None:
        logger.debug("Chunked tensor take fast path not used: reason=%s", reason.value)
    else:
        logger.debug(
            "Chunked tensor take fast path not prepared: reason=%s, rows=%s, "
            "chunks=%s, type=%s",
            reason.value,
            len(column),
            column.num_chunks,
            column.type,
        )
    return None


def _passes_fixed_size_gates(
    source_rows: int,
    row_bytes: int,
    source_chunks: int,
    max_output_rows: int,
) -> bool:
    """Return whether a fixed-shape source or output can amortize setup."""
    source_bytes = source_rows * row_bytes
    output_bytes = max_output_rows * row_bytes
    return (
        row_bytes >= _MIN_FIXED_ROW_BYTES
        and source_bytes >= _MIN_FIXED_PAYLOAD_BYTES
        and (
            source_bytes >= source_chunks * _MIN_FIXED_SOURCE_BYTES_PER_CHUNK
            or output_bytes >= _MIN_FIXED_PAYLOAD_BYTES
        )
    )


def _prepare_fixed_tensor_layout(
    tensor_type: Any,
) -> Optional[Tuple[int, int, np.dtype]]:
    """Return validated fixed numeric layout metadata, or ``None``.

    The returned tuple is ``(values_per_row, row_bytes, numpy_dtype)``. Rejecting
    unsupported scalar types or shapes keeps the fast path independent of object
    conversion and variable-shape tensor semantics. Unexpected conversion errors
    propagate to the caller.
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

    value_dtype = np.dtype(scalar_type.to_pandas_dtype())
    if value_dtype.hasobject or value_dtype.itemsize * 8 != scalar_type.bit_width:
        return None

    return values_per_row, values_per_row * value_dtype.itemsize, value_dtype


def _prepare_fixed_chunk_view(
    chunk: Any,
    tensor_type: Any,
    values_per_row: int,
    value_dtype: np.dtype,
) -> Optional[np.ndarray]:
    """Return a validated zero-copy fixed-shape chunk view, or ``None``.

    Constructing the view from the numeric child buffer makes its shape, dtype,
    contiguity, ownership, and buffer bounds explicit. The logical list
    offsets are authoritative: a legal array may start after child element 0,
    while malformed or variable-stride offsets cannot represent the declared
    fixed tensor shape and must be rejected. Unexpected Arrow and NumPy conversion
    errors propagate to the caller.
    """
    values = chunk.storage.values
    # Preserve the existing fallback for child arrays whose logical data starts
    # inside a larger values array.
    if values.offset != 0 or values.null_count > 0:
        return None

    storage = chunk.storage
    offsets = storage.offsets.to_numpy(zero_copy_only=True)
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
    buffer_size = data_buffer.size
    if byte_offset > buffer_size or view_nbytes > buffer_size - byte_offset:
        return None
    return np.ndarray(
        (len(chunk), *tensor_type.shape),
        dtype=value_dtype,
        buffer=data_buffer,
        offset=byte_offset,
    )


class PreparedFixedShapedTensorTake(NamedTuple):
    """Fixed-shape tensor take prepared from an immutable chunked column."""

    tensor_type: Any
    values_per_row: int
    value_dtype: np.dtype
    subbatch_rows: int
    chunk_views: tuple[np.ndarray, ...]
    chunk_starts: np.ndarray

    def take(self, indices: np.ndarray) -> pa.Array:
        """Take normalized rows under the contract established by preparation.

        ``indices`` must be a one-dimensional, native ``np.int64`` array whose
        values are within the source column's bounds. Callers establish this
        invariant once before applying the same indices to multiple columns.
        Preparation also establishes that every take stays within the declared
        output-size bound, so execution performs no eligibility checks.
        """
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
        * Unordered IDs sort output positions by chunk once, gather each
          resulting group, and scatter it back to the original positions. The
          temporary sort changes processing order, never caller-visible row
          order.

        Subbatching bounds the temporary chunk-ID, local-index, and sort arrays.
        Both strategies write into the same preallocated output and preserve
        the order of ``indices``.

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


class _VariableTensorChunk(NamedTuple):
    """Zero-copy payload and validated row metadata from one physical chunk."""

    values: np.ndarray
    offsets: np.ndarray
    shapes: np.ndarray


def _try_prepare_variable_tensor_take(
    column: pa.ChunkedArray, max_output_rows: int
) -> Optional["PreparedVariableShapedTensorTake"]:
    """Prepare non-null numeric variable-shaped tensors after the common gates.

    Size gates use logical payload, not the size of retained parent buffers.
    The row-size gate is an average; the per-chunk gate includes empty chunks.
    Requested output is estimated from that average only for the cost gate.
    Oversampling must amortize per-row work through the source payload or the
    smallest source row, since repeated indices may select only tiny rows.
    Capacity checks instead use the largest row, covering repeated indices and
    every batch/carry-over take up to the declared maximum output row count.
    """
    tensor_type = column.type
    scalar_type = tensor_type.value_type
    if (
        not (pa.types.is_integer(scalar_type) or pa.types.is_floating(scalar_type))
        or not isinstance(tensor_type.ndim, int)
        or tensor_type.ndim < 0
    ):
        return _log_take_fallback(
            _TakeFallbackReason.UNSUPPORTED_TENSOR_LAYOUT, column=column
        )
    value_dtype = np.dtype(scalar_type.to_pandas_dtype())
    storages = [chunk.storage for chunk in column.chunks if len(chunk)]
    if len(storages) < 2:
        return _log_take_fallback(
            _TakeFallbackReason.FEWER_THAN_TWO_NONEMPTY_CHUNKS, column=column
        )

    source_values = 0
    for storage in storages:
        data = storage.field("data")
        first, last = int(data.offsets[0].as_py()), int(data.offsets[-1].as_py())
        if not 0 <= first <= last <= len(data.values):
            return _log_take_fallback(
                _TakeFallbackReason.UNSAFE_CHUNK_STORAGE, column=column
            )
        source_values += last - first
    source_bytes = source_values * value_dtype.itemsize
    if (
        source_bytes < len(column) * _MIN_VARIABLE_ROW_BYTES
        or source_bytes < _MIN_VARIABLE_PAYLOAD_BYTES
        or (
            source_bytes < column.num_chunks * _MIN_VARIABLE_SOURCE_BYTES_PER_CHUNK
            and source_bytes * max_output_rows
            < len(column) * column.num_chunks * _MIN_VARIABLE_OUTPUT_BYTES_PER_CHUNK
        )
    ):
        return _log_take_fallback(
            _TakeFallbackReason.BELOW_SIZE_THRESHOLD, column=column
        )

    # Shape lists have int32 offsets even though data lists use int64 offsets.
    if max_output_rows * tensor_type.ndim > np.iinfo(np.dtype(np.int32)).max:
        return _log_take_fallback(
            _TakeFallbackReason.OUTPUT_OFFSET_OVERFLOW, column=column
        )
    chunks, starts = [], []
    row_start, largest_row = 0, 0
    for storage in storages:
        chunk = _prepare_variable_chunk(storage, tensor_type.ndim, value_dtype)
        if chunk is None:
            return _log_take_fallback(
                _TakeFallbackReason.UNSAFE_CHUNK_STORAGE, column=column
            )
        largest_row = max(largest_row, int(np.max(np.diff(chunk.offsets))))
        chunks.append(chunk)
        starts.append(row_start)
        row_start += len(storage)
    # Bound NumPy allocation bytes as well as Arrow's element offsets. Python
    # integer arithmetic avoids overflow while checking even huge repeat counts.
    if (
        max_output_rows * largest_row
        > np.iinfo(np.dtype(np.intp)).max // value_dtype.itemsize
    ):
        return _log_take_fallback(
            _TakeFallbackReason.OUTPUT_OFFSET_OVERFLOW, column=column
        )
    # Repeated indices can select only tiny rows despite a large source average.
    # Cover the per-output-row cost with either avoided source-copy bytes or a
    # lower bound on every selected row. Shuffle generations already satisfy
    # the source bound because max_output_rows never exceeds the source rows.
    if source_bytes < max_output_rows * _MIN_VARIABLE_ROW_BYTES and any(
        int(np.min(np.diff(chunk.offsets))) * value_dtype.itemsize
        < _MIN_VARIABLE_ROW_BYTES
        for chunk in chunks
    ):
        return _log_take_fallback(
            _TakeFallbackReason.BELOW_SIZE_THRESHOLD, column=column
        )
    logger.debug(
        "Variable tensor take fast path prepared: rows=%s, chunks=%s, "
        "source_bytes=%s, max_output_rows=%s",
        len(column),
        len(chunks),
        source_bytes,
        max_output_rows,
    )
    return PreparedVariableShapedTensorTake(
        tensor_type, value_dtype, tuple(chunks), np.asarray(starts, dtype=np.int64)
    )


def _prepare_variable_chunk(
    storage: pa.StructArray, ndim: int, value_dtype: np.dtype
) -> Optional[_VariableTensorChunk]:
    """Validate logical data/shape offsets and expose numeric child views.

    Division checks shape products without multiplying dimensions in a fixed
    width dtype. This accepts zero-sized dimensions without overflow and rejects
    shapes whose product differs from their row's actual data length.
    """
    data, shape = storage.field("data"), storage.field("shape")
    if any(
        array.null_count for array in (storage, data, data.values, shape, shape.values)
    ):
        return None
    offsets = data.offsets.to_numpy(zero_copy_only=True)
    shape_offsets = shape.offsets.to_numpy(zero_copy_only=True)
    if (
        np.any(offsets[1:] < offsets[:-1])
        or not 0 <= int(offsets[0]) <= int(offsets[-1]) <= len(data.values)
        or not 0 <= int(shape_offsets[0]) <= int(shape_offsets[-1]) <= len(shape.values)
        or np.any(np.diff(shape_offsets.astype(np.int64, copy=False)) != ndim)
    ):
        return None
    data_buffer = data.values.buffers()[1]
    if data_buffer is None:
        # A zero-length numeric child need not have a data buffer.
        if len(data.values):
            return None
    elif (
        data.values.offset + len(data.values)
    ) * value_dtype.itemsize > data_buffer.size:
        return None
    flat_shapes = shape.values.to_numpy(zero_copy_only=True)
    shapes = flat_shapes[int(shape_offsets[0]) : int(shape_offsets[-1])].reshape(
        len(storage), ndim
    )
    if np.any(shapes < 0):
        return None
    remaining = np.diff(offsets)
    zero_rows = np.any(shapes == 0, axis=1)
    for axis in range(ndim):
        factors = np.maximum(shapes[:, axis], 1)
        if np.any(remaining % factors):
            return None
        remaining //= factors
    if np.any(remaining != np.where(zero_rows, 0, 1)):
        return None
    return _VariableTensorChunk(
        data.values.to_numpy(zero_copy_only=True), offsets, shapes
    )


class PreparedVariableShapedTensorTake(NamedTuple):
    """Gather variable rows without concatenation or per-scalar take indices.

    Source slices are copied straight into the final payload: no temporary
    tensor payload is needed, even for a row larger than the fixed-shape scratch
    cap. Row routing and shape/offset metadata consume O(K * ndim) space.
    """

    tensor_type: ArrowVariableShapedTensorType
    value_dtype: np.dtype
    chunks: tuple[_VariableTensorChunk, ...]
    chunk_starts: np.ndarray

    def take(self, indices: np.ndarray) -> pa.Array:
        """Take valid native int64 indices within the prepared output bound.

        Callers establish the same index contract as fixed-shape prepared takes.
        Preparation proves the largest possible output fits the buffer/offset
        dtypes, so even repeated long rows can safely use a cumulative sum here.
        """
        chunk_ids = np.searchsorted(self.chunk_starts, indices, side="right") - 1
        local = indices - self.chunk_starts[chunk_ids]
        lengths = np.empty(len(indices), dtype=np.int64)
        source_offsets = np.empty(len(indices), dtype=np.int64)
        shapes = np.empty((len(indices), self.tensor_type.ndim), dtype=np.int64)
        # Sort once to avoid scanning every requested row for every source chunk.
        order = np.argsort(chunk_ids)
        sorted_ids = chunk_ids[order]
        boundaries = np.flatnonzero(sorted_ids[1:] != sorted_ids[:-1]) + 1
        start = 0
        for stop in chain(boundaries, (len(order),)):
            if start == stop:
                continue
            positions = order[start:stop]
            chunk = self.chunks[int(sorted_ids[start])]
            rows = local[positions]
            source_offsets[positions] = chunk.offsets[rows]
            lengths[positions] = chunk.offsets[rows + 1] - chunk.offsets[rows]
            shapes[positions] = chunk.shapes[rows]
            start = stop
        offsets = np.empty(len(indices) + 1, dtype=np.int64)
        offsets[0] = 0
        np.cumsum(lengths, out=offsets[1:])
        output = np.empty(int(offsets[-1]), dtype=self.value_dtype)
        if output.size:
            for position, chunk_id in enumerate(chunk_ids):
                source_start = int(source_offsets[position])
                source_stop = source_start + int(lengths[position])
                output[offsets[position] : offsets[position + 1]] = self.chunks[
                    int(chunk_id)
                ].values[source_start:source_stop]
        data = pa.LargeListArray.from_arrays(
            pa.array(offsets),
            pa.Array.from_buffers(
                self.tensor_type.value_type, len(output), [None, pa.py_buffer(output)]
            ),
        )
        shape_offsets = (
            np.arange(len(indices) + 1, dtype=np.int64) * self.tensor_type.ndim
        )
        shape = pa.ListArray.from_arrays(
            pa.array(shape_offsets, type=pa.int32()), pa.array(shapes.reshape(-1))
        )
        return self.tensor_type.wrap_array(
            pa.StructArray.from_arrays([data, shape], names=["data", "shape"])
        )


PreparedTensorTake = Union[
    PreparedFixedShapedTensorTake, PreparedVariableShapedTensorTake
]


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
