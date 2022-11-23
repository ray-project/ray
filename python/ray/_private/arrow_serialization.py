# arrow_serialization.py must resides outside of ray.data, otherwise
# it causes circular dependency issues for AsyncActors due to
# ray.data's lazy import.
# see https://github.com/ray-project/ray/issues/30498 for more context.
import logging
import os
import sys
from typing import List, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION"
)
RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION"
)

logger = logging.getLogger(__name__)

# Whether we have already warned the user about bloated fallback serialization.
_serialization_fallback_set = set()

# Whether we're currently running in a test, either local or CI.
_in_test = None


def _is_in_test():
    global _in_test

    if _in_test is None:
        _in_test = any(
            env_var in os.environ
            # These environment variables are always set by pytest and Buildkite,
            # respectively.
            for env_var in ("PYTEST_CURRENT_TEST", "BUILDKITE")
        )
    return _in_test


def _register_custom_datasets_serializers(serialization_context):
    try:
        import pyarrow as pa  # noqa: F401
    except ModuleNotFoundError:
        # No pyarrow installed so not using Arrow, so no need for custom serializers.
        return

    # Register all custom serializers required by Datasets.
    _register_arrow_data_serializer(serialization_context)
    _register_arrow_json_readoptions_serializer(serialization_context)
    _register_arrow_json_parseoptions_serializer(serialization_context)


# Register custom Arrow JSON ReadOptions serializer to workaround it not being picklable
# in Arrow < 8.0.0.
def _register_arrow_json_readoptions_serializer(serialization_context):
    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        return

    import pyarrow.json as pajson

    serialization_context._register_cloudpickle_serializer(
        pajson.ReadOptions,
        custom_serializer=lambda opts: (opts.use_threads, opts.block_size),
        custom_deserializer=lambda args: pajson.ReadOptions(*args),
    )


def _register_arrow_json_parseoptions_serializer(serialization_context):
    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        return

    import pyarrow.json as pajson

    serialization_context._register_cloudpickle_serializer(
        pajson.ParseOptions,
        custom_serializer=lambda opts: (
            opts.explicit_schema,
            opts.newlines_in_values,
            opts.unexpected_field_behavior,
        ),
        custom_deserializer=lambda args: pajson.ParseOptions(*args),
    )


# Register custom Arrow data serializer to work around zero-copy slice pickling bug.
# See https://issues.apache.org/jira/browse/ARROW-10739.
def _register_arrow_data_serializer(serialization_context):
    """Custom reducer for Arrow data that works around a zero-copy slicing pickling
    bug by using the Arrow IPC format for the underlying serialization.

    Background:
        Arrow has both array-level slicing and buffer-level slicing; both are zero-copy,
        but the former has a serialization bug where the entire buffer is serialized
        instead of just the slice, while the latter's serialization works as expected
        and only serializes the slice of the buffer. I.e., array-level slicing doesn't
        propagate the slice down to the buffer when serializing the array.

        We work around this by registering a custom cloudpickle reducers for Arrow
        Tables that delegates serialization to the Arrow IPC format; thankfully, Arrow's
        IPC serialization has fixed this buffer truncation bug.

    See https://issues.apache.org/jira/browse/ARROW-10739.
    """
    if os.environ.get(RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION, "0") == "1":
        return

    import pyarrow as pa

    serialization_context._register_cloudpickle_reducer(pa.Table, _arrow_table_reduce)


def _arrow_table_reduce(t: "pyarrow.Table"):
    """Custom reducer for Arrow Tables that works around a zero-copy slice pickling bug.
    Background:
        Arrow has both array-level slicing and buffer-level slicing; both are zero-copy,
        but the former has a serialization bug where the entire buffer is serialized
        instead of just the slice, while the latter's serialization works as expected
        and only serializes the slice of the buffer. I.e., array-level slicing doesn't
        propagate the slice down to the buffer when serializing the array.
        All that these copy methods do is, at serialization time, take the array-level
        slicing and translate them to buffer-level slicing, so only the buffer slice is
        sent over the wire instead of the entire buffer.
    See https://issues.apache.org/jira/browse/ARROW-10739.
    """
    global _serialization_fallback_set

    # Reduce the ChunkedArray columns.
    reduced_columns = []
    for column_name in t.column_names:
        column = t[column_name]
        try:
            # Delegate to ChunkedArray reducer.
            reduced_column = _arrow_chunked_array_reduce(column)
        except Exception as e:
            if not _is_dense_union(column.type) and _is_in_test():
                # If running in a test and the column is not a dense union array
                # (which we expect to need a fallback), we want to raise the error,
                # not fall back.
                raise e from None
            if type(column.type) not in _serialization_fallback_set:
                logger.warning(
                    "Failed to complete optimized serialization of Arrow Table, "
                    f"serialization of column '{column_name}' of type {column.type} "
                    "failed, so we're falling back to Arrow IPC serialization for the "
                    "table. Note that this may result in slower serialization and more "
                    "worker memory utilization. Serialization error:",
                    exc_info=True,
                )
                _serialization_fallback_set.add(type(column.type))
            # Fall back to Arrow IPC-based workaround for the entire table.
            return _arrow_table_ipc_reduce(t)
        else:
            # Column reducer succeeded, add reduced column to list.
            reduced_columns.append(reduced_column)
    return _reconstruct_table, (reduced_columns, t.schema)


def _reconstruct_table(
    reduced_columns: List[Tuple[List["pyarrow.Array"], "pyarrow.DataType"]],
    schema: "pyarrow.Schema",
) -> "pyarrow.Table":
    """Restore a serialized Arrow Table, reconstructing each reduced column."""
    import pyarrow as pa

    # Reconstruct each reduced column.
    columns = []
    for chunks, type_ in reduced_columns:
        columns.append(_reconstruct_chunked_array(chunks, type_))

    return pa.Table.from_arrays(columns, schema=schema)


def _arrow_chunked_array_reduce(
    ca: "pyarrow.ChunkedArray",
) -> Tuple[List["pyarrow.Array"], "pyarrow.DataType"]:
    """Custom reducer for Arrow ChunkedArrays that works around a zero-copy slice
    pickling bug. This reducer does not return a reconstruction function, since it's
    expected to be reconstructed by the Arrow Table reconstructor.
    """
    truncated_chunks = []
    for chunk in ca.chunks:
        chunk = _copy_array_if_needed(chunk)
        truncated_chunks.append(chunk)
    return truncated_chunks, ca.type


def _reconstruct_chunked_array(
    chunks: List["pyarrow.Array"], type_: "pyarrow.DataType"
) -> "pyarrow.ChunkedArray":
    """Restore a serialized Arrow ChunkedArray from chunks and type."""
    import pyarrow as pa

    return pa.chunked_array(chunks, type_)


def _copy_array_if_needed(a: "pyarrow.Array") -> "pyarrow.Array":
    """Copy the provided Arrow array, if needed.
    This method recursively traverses the array and subarrays, translating array-level
    slices to buffer-level slices, thereby ensuring a copy at pickle time.
    """
    # See the Arrow buffer layouts for each type for information on how this buffer
    # traversal and copying works:
    # https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
    import pyarrow as pa

    if _is_dense_union(a.type):
        # Dense unions are not supported.
        # TODO(Clark): Support dense unions.
        raise NotImplementedError(
            "Custom slice view serialization of dense union arrays is not yet "
            "supported."
        )

    from ray.air.util.tensor_extensions.arrow import (
        ArrowTensorArray,
        ArrowVariableShapedTensorArray,
    )

    if isinstance(a, ArrowTensorArray):
        # Custom path for copying the buffers underlying our tensor column extension
        # array.
        return _copy_tensor_array_if_needed(a)

    if isinstance(a, ArrowVariableShapedTensorArray):
        # Custom path for copying the buffers underlying our variable-shaped tensor
        # column extension array.
        return _copy_variable_shaped_tensor_array_if_needed(a)

    if pa.types.is_dictionary(a.type):
        # Custom path for dictionary arrays.
        dictionary = _copy_array_if_needed(a.dictionary)
        indices = _copy_array_if_needed(a.indices)
        return pa.DictionaryArray.from_arrays(indices, dictionary)

    if pa.types.is_null(a.type):
        # Return NullArray as is.
        return a

    buffers = a.buffers()
    bitmap = buffers[0]
    buf = buffers[1]
    # Let remaining buffers be handled downstream.
    buffers = buffers[2:]
    children = None
    if pa.types.is_struct(a.type) or pa.types.is_union(a.type):
        # Struct and union arrays directly expose children arrays, which are easier
        # to work with than the raw buffers.
        children = [a.field(i) for i in range(a.type.num_fields)]
        buffers = None
    if pa.types.is_map(a.type):
        if isinstance(a, pa.lib.ListArray):
            # Map arrays directly expose the one child array in pyarrow>=7.0.0, which
            # is easier to work with than the raw buffers.
            children = [a.values]
            buffers = None
        else:
            # In pyarrow<7.0.0, the child array is not exposed, so we work with the key
            # and item arrays.
            if bitmap is not None:
                bitmap = _copy_bitpacked_buffer_if_needed(bitmap, a.offset, len(a))
            offset_buf, data_offset, data_length = _copy_offsets_buffer_if_needed(
                buf, a.type, a.offset, len(a)
            )
            offsets = pa.Array.from_buffers(
                pa.int32(), len(a) + 1, [bitmap, offset_buf]
            )
            keys = _copy_array_if_needed(a.keys.slice(data_offset, data_length))
            items = _copy_array_if_needed(a.items.slice(data_offset, data_length))
            return pa.MapArray.from_arrays(offsets, keys, items)
    return _copy_array_buffers_if_needed(
        bitmap,
        buf,
        a.type,
        a.offset,
        len(a),
        a.null_count,
        buffers=buffers,
        children=children,
    )


def _copy_array_buffers_if_needed(
    bitmap: "pyarrow.Buffer",
    buf: "pyarrow.Buffer",
    type_: "pyarrow.DataType",
    offset: int,
    length: int,
    null_count: int,
    *,
    buffers: Optional[List["pyarrow.Buffer"]] = None,
    children: Optional[List["pyarrow.Array"]] = None,
) -> "pyarrow.Array":
    """
    Copy provided array buffers, if needed.
    """
    import pyarrow as pa

    new_buffers = []
    new_children = None

    # Copy bitmap buffer, if needed.
    if bitmap is not None:
        bitmap = _copy_bitpacked_buffer_if_needed(bitmap, offset, length)
    new_buffers.append(bitmap)

    if pa.types.is_list(type_) or pa.types.is_large_list(type_):
        # Dedicated path for ListArrays. These arrays have a nested set of bitmap and
        # offset buffers, eventually bottoming out on a data buffer.
        # However, pyarrow doesn't expose the children arrays in the Python API, so we
        # have to work directly with the underlying buffers.
        # Buffer scheme for nested ListArray:
        # [bitmap, offsets, bitmap, offsets, ..., bitmap, data]
        assert buffers is not None
        assert children is None
        buf, child_offset, child_length = _copy_offsets_buffer_if_needed(
            buf, type_, offset, length
        )
        # Recursively construct child array based on remaining buffers.
        # Assumption: Every ListArray has 2 buffers (bitmap, offsets) and 1 child.
        child = _copy_array_buffers_if_needed(
            bitmap=buffers[0],
            buf=buffers[1],
            type_=type_.value_type,
            offset=child_offset,
            length=child_length,
            # Null count not known without the child arrays exposed in the Python API.
            null_count=-1,
            buffers=buffers[2:],
        )
        new_children = [child]
        new_buffers.append(buf)
    elif pa.types.is_fixed_size_list(type_):
        # Dedicated path for fixed-size lists.
        # Buffer scheme for FixedSizeListArray:
        # [bitmap, values_bitmap, values_data, values_subbuffers...]
        child = _copy_array_buffers_if_needed(
            bitmap=buf,
            buf=buffers[0],
            type_=type_.value_type,
            offset=type_.list_size * offset,
            length=type_.list_size * length,
            # Null count not known.
            null_count=-1,
            buffers=buffers[1:],
        )
        new_children = [child]
    elif pa.types.is_map(type_):
        # Dedicated path for MapArrays.
        # Buffer scheme for MapArrays:
        # [bitmap, offsets, child_struct_array_buffers...]
        buf, child_offset, child_length = _copy_offsets_buffer_if_needed(
            buf, type_, offset, length
        )
        # We copy the children arrays (should be single child struct array).
        assert len(children) == 1
        new_children = []
        for child in children:
            child = child.slice(child_offset, child_length)
            new_children.append(_copy_array_if_needed(child))
        new_buffers.append(buf)
    elif pa.types.is_struct(type_) or pa.types.is_union(type_):
        # Dedicated path for StructArrays and UnionArrays.
        # StructArrays have a top-level bitmap buffer and one or more children arrays.
        # UnionArrays have a top-level bitmap buffer and type code buffer, and one or
        # more children arrays.
        assert children is not None
        assert buffers is None
        if pa.types.is_union(type_):
            # Only sparse unions are supported.
            assert not _is_dense_union(type_)
            assert buf is not None
            buf = _copy_buffer_if_needed(buf, pa.int8(), offset, length)
            new_buffers.append(buf)
        else:
            assert buf is None
        # We copy the children arrays.
        new_children = []
        for child in children:
            new_children.append(_copy_array_if_needed(child))
    elif (
        pa.types.is_string(type_)
        or pa.types.is_large_string(type_)
        or pa.types.is_binary(type_)
        or pa.types.is_large_binary(type_)
    ):
        # Dedicated path for StringArrays.
        assert len(buffers) == 1
        # StringArray buffer scheme: [bitmap, value_offsets, data]
        offset_buf, data_offset, data_length = _copy_offsets_buffer_if_needed(
            buf, type_, offset, length
        )
        data_buf = _copy_buffer_if_needed(buffers[0], None, data_offset, data_length)
        new_buffers.append(offset_buf)
        new_buffers.append(data_buf)
    else:
        # If not a nested Array, buf is a plain data buffer.
        # Copy data buffer, if needed.
        if buf is not None:
            buf = _copy_buffer_if_needed(buf, type_, offset, length)
        new_buffers.append(buf)
    return pa.Array.from_buffers(
        type_, length, buffers=new_buffers, null_count=null_count, children=new_children
    )


def _copy_buffer_if_needed(
    buf: "pyarrow.Buffer",
    type_: Optional["pyarrow.DataType"],
    offset: int,
    length: int,
) -> "pyarrow.Buffer":
    """Copy buffer, if needed."""
    import pyarrow as pa

    if type_ is not None and pa.types.is_boolean(type_):
        # Arrow boolean array buffers are bit-packed, with 8 entries per byte,
        # and are accessed via bit offsets.
        buf = _copy_bitpacked_buffer_if_needed(buf, offset, length)
    else:
        type_bytewidth = type_.bit_width // 8 if type_ is not None else 1
        buf = _copy_normal_buffer_if_needed(buf, type_bytewidth, offset, length)
    return buf


def _copy_normal_buffer_if_needed(
    buf: "pyarrow.Buffer",
    byte_width: int,
    offset: int,
    length: int,
) -> "pyarrow.Buffer":
    """Copy buffer, if needed."""
    byte_offset = offset * byte_width
    byte_length = length * byte_width
    if offset > 0 or byte_length < buf.size:
        # Array is a zero-copy slice, so we need to copy to a new buffer before
        # serializing; this slice of the underlying buffer (not the array) will ensure
        # that the buffer is properly copied at pickle-time.
        buf = buf.slice(byte_offset, byte_length)
    return buf


def _copy_bitpacked_buffer_if_needed(
    buf: "pyarrow.Buffer",
    offset: int,
    length: int,
) -> "pyarrow.Buffer":
    """Copy bit-packed binary buffer, if needed."""
    bit_offset = offset % 8
    byte_offset = offset // 8
    byte_length = _bytes_for_bits(bit_offset + length) // 8
    if offset > 0 or byte_length < buf.size:
        buf = buf.slice(byte_offset, byte_length)
        if bit_offset != 0:
            # Need to manually shift the buffer to eliminate the bit offset.
            buf = _align_bit_offset(buf, bit_offset, byte_length)
    return buf


def _copy_offsets_buffer_if_needed(
    buf: "pyarrow.Buffer",
    arr_type: "pyarrow.DataType",
    offset: int,
    length: int,
) -> Tuple["pyarrow.Buffer", int, int]:
    """Copy the provided offsets buffer, returning the copied buffer and the
    offset + length of the underlying data.
    """
    import pyarrow as pa
    import pyarrow.compute as pac

    if (
        pa.types.is_large_list(arr_type)
        or pa.types.is_large_string(arr_type)
        or pa.types.is_large_binary(arr_type)
        or pa.types.is_large_unicode(arr_type)
    ):
        offset_type = pa.int64()
    else:
        offset_type = pa.int32()
    # Copy offset buffer, if needed.
    buf = _copy_buffer_if_needed(buf, offset_type, offset, length + 1)
    # Reconstruct the offset array so we can determine the offset and length
    # of the child array.
    offsets = pa.Array.from_buffers(offset_type, length + 1, [None, buf])
    child_offset = offsets[0].as_py()
    child_length = offsets[-1].as_py() - child_offset
    # Create new offsets aligned to 0 for the copied data buffer slice.
    offsets = pac.subtract(offsets, child_offset)
    if pa.types.is_int32(offset_type):
        # We need to cast the resulting Int64Array back down to an Int32Array.
        offsets = offsets.cast(offset_type, safe=False)
    buf = offsets.buffers()[1]
    return buf, child_offset, child_length


def _bytes_for_bits(n: int) -> int:
    """Round up n to the nearest multiple of 8.
    This is used to get the byte-padded number of bits for n bits.
    """
    return (n + 7) & (-8)


def _align_bit_offset(
    buf: "pyarrow.Buffer",
    bit_offset: int,
    byte_length: int,
) -> "pyarrow.Buffer":
    """Align the bit offset into the buffer with the front of the buffer by shifting
    the buffer and eliminating the offset.
    """
    import pyarrow as pa

    bytes_ = buf.to_pybytes()
    bytes_as_int = int.from_bytes(bytes_, sys.byteorder)
    bytes_as_int >>= bit_offset
    bytes_ = bytes_as_int.to_bytes(byte_length, sys.byteorder)
    return pa.py_buffer(bytes_)


def _copy_tensor_array_if_needed(a: "ArrowTensorArray") -> "ArrowTensorArray":
    """Copy tensor array if it's a zero-copy slice. This is to circumvent an Arrow
    serialization bug, where a zero-copy slice serializes the entire underlying array
    buffer.
    """
    import pyarrow as pa
    from ray.data.extensions import ArrowTensorType

    # Offset is propagated to storage array, and the storage array items align with the
    # tensor elements, so we only need to do the straightforward copy of the storage
    # array.
    storage = _copy_array_if_needed(a.storage)
    type_ = ArrowTensorType(a.type.shape, storage.type.value_type)
    return pa.ExtensionArray.from_storage(type_, storage)


def _copy_variable_shaped_tensor_array_if_needed(
    a: "ArrowVariableShapedTensorArray",
) -> "ArrowVariableShapedTensorArray":
    """Copy variable-shaped tensor array if it's a zero-copy slice. This is to
    circumvent an Arrow serialization bug, where a zero-copy slice serializes the entire
    underlying array buffer.
    """
    import pyarrow as pa
    from ray.data.extensions import ArrowVariableShapedTensorType

    # Offset is propagated to storage struct array, and both the data and shape fields
    # items align with tensor elements, so we only need to do the straightforward copy
    # of the storage array.
    storage = _copy_array_if_needed(a.storage)
    type_ = ArrowVariableShapedTensorType(
        storage.field("data").type.value_type, a.type.ndim
    )
    return pa.ExtensionArray.from_storage(type_, storage)


def _arrow_table_ipc_reduce(table: "pyarrow.Table"):
    """Custom reducer for Arrow Table that works around a zero-copy slicing pickling
    bug by using the Arrow IPC format for the underlying serialization.

    This is currently used as a fallback for unsupported types (or unknown bugs) for
    the manual buffer truncation workaround, e.g. for dense unions.
    """
    from pyarrow.ipc import RecordBatchStreamWriter
    from pyarrow.lib import BufferOutputStream

    output_stream = BufferOutputStream()
    with RecordBatchStreamWriter(output_stream, schema=table.schema) as wr:
        wr.write_table(table)
    # NOTE: output_stream.getvalue() materializes the serialized table to a single
    # contiguous bytestring, resulting in a few copy. This adds 1-2 extra copies on the
    # serialization side, and 1 extra copy on the deserialization side.
    return _restore_table_from_ipc, (output_stream.getvalue(),)


def _restore_table_from_ipc(buf: bytes) -> "pyarrow.Table":
    """Restore an Arrow Table serialized to Arrow IPC format."""
    from pyarrow.ipc import RecordBatchStreamReader

    with RecordBatchStreamReader(buf) as reader:
        return reader.read_all()


def _is_dense_union(type_: "pyarrow.DataType") -> bool:
    """Whether the provided Arrow type is a dense union."""
    import pyarrow as pa

    return pa.types.is_union(type_) and type_.mode == "dense"
