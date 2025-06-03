# arrow_serialization.py must resides outside of ray.data, otherwise
# it causes circular dependency issues for AsyncActors due to
# ray.data's lazy import.
# see https://github.com/ray-project/ray/issues/30498 for more context.
import logging
import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Tuple

from ray._private.utils import is_in_test

if TYPE_CHECKING:
    import pyarrow

    from ray.data.extensions import ArrowTensorArray

RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION"
)
RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION"
)

logger = logging.getLogger(__name__)

# Whether we have already warned the user about bloated fallback serialization.
_serialization_fallback_set = set()


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
            if not _is_dense_union(column.type) and is_in_test():
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
    for chunks_payload, type_ in reduced_columns:
        columns.append(_reconstruct_chunked_array(chunks_payload, type_))

    return pa.Table.from_arrays(columns, schema=schema)


def _arrow_chunked_array_reduce(
    ca: "pyarrow.ChunkedArray",
) -> Tuple[List["PicklableArrayPayload"], "pyarrow.DataType"]:
    """Custom reducer for Arrow ChunkedArrays that works around a zero-copy slice
    pickling bug. This reducer does not return a reconstruction function, since it's
    expected to be reconstructed by the Arrow Table reconstructor.
    """
    # Convert chunks to serialization payloads.
    chunk_payloads = []
    for chunk in ca.chunks:
        chunk_payload = PicklableArrayPayload.from_array(chunk)
        chunk_payloads.append(chunk_payload)
    return chunk_payloads, ca.type


def _reconstruct_chunked_array(
    chunks: List["PicklableArrayPayload"], type_: "pyarrow.DataType"
) -> "pyarrow.ChunkedArray":
    """Restore a serialized Arrow ChunkedArray from chunks and type."""
    import pyarrow as pa

    # Reconstruct chunks from serialization payloads.
    chunks = [chunk.to_array() for chunk in chunks]

    return pa.chunked_array(chunks, type_)


@dataclass
class PicklableArrayPayload:
    """Picklable array payload, holding data buffers and array metadata.

    This is a helper container for pickling and reconstructing nested Arrow Arrays while
    ensuring that the buffers that underly zero-copy slice views are properly truncated.
    """

    # Array type.
    type: "pyarrow.DataType"
    # Length of array.
    length: int
    # Underlying data buffers.
    buffers: List["pyarrow.Buffer"]
    # Cached null count.
    null_count: int
    # Slice offset into base array.
    offset: int
    # Serialized array payloads for nested (child) arrays.
    children: List["PicklableArrayPayload"]

    @classmethod
    def from_array(self, a: "pyarrow.Array") -> "PicklableArrayPayload":
        """Create a picklable array payload from an Arrow Array.

        This will recursively accumulate data buffer and metadata payloads that are
        ready for pickling; namely, the data buffers underlying zero-copy slice views
        will be properly truncated.
        """
        return _array_to_array_payload(a)

    def to_array(self) -> "pyarrow.Array":
        """Reconstruct an Arrow Array from this picklable payload."""
        return _array_payload_to_array(self)


def _array_payload_to_array(payload: "PicklableArrayPayload") -> "pyarrow.Array":
    """Reconstruct an Arrow Array from a possibly nested PicklableArrayPayload."""
    import pyarrow as pa

    from ray.air.util.tensor_extensions.arrow import get_arrow_extension_tensor_types

    children = [child_payload.to_array() for child_payload in payload.children]

    tensor_extension_types = get_arrow_extension_tensor_types()

    if pa.types.is_dictionary(payload.type):
        # Dedicated path for reconstructing a DictionaryArray, since
        # Array.from_buffers() doesn't work for DictionaryArrays.
        assert len(children) == 2, len(children)
        indices, dictionary = children
        return pa.DictionaryArray.from_arrays(indices, dictionary)
    elif pa.types.is_map(payload.type) and len(children) > 1:
        # In pyarrow<7.0.0, the underlying map child array is not exposed, so we work
        # with the key and item arrays.
        assert len(children) == 3, len(children)
        offsets, keys, items = children
        return pa.MapArray.from_arrays(offsets, keys, items)
    elif isinstance(
        payload.type,
        tensor_extension_types,
    ):
        # Dedicated path for reconstructing an ArrowTensorArray or
        # ArrowVariableShapedTensorArray, both of which can't be reconstructed by the
        # Array.from_buffers() API.
        assert len(children) == 1, len(children)
        storage = children[0]
        return pa.ExtensionArray.from_storage(payload.type, storage)
    else:
        # Common case: use Array.from_buffers() to construct an array of a certain type.
        return pa.Array.from_buffers(
            type=payload.type,
            length=payload.length,
            buffers=payload.buffers,
            null_count=payload.null_count,
            offset=payload.offset,
            children=children,
        )


def _array_to_array_payload(a: "pyarrow.Array") -> "PicklableArrayPayload":
    """Serialize an Arrow Array to an PicklableArrayPayload for later pickling.

    This function's primary purpose is to dispatch to the handler for the input array
    type.
    """
    import pyarrow as pa

    from ray.air.util.tensor_extensions.arrow import get_arrow_extension_tensor_types

    tensor_extension_types = get_arrow_extension_tensor_types()

    if _is_dense_union(a.type):
        # Dense unions are not supported.
        # TODO(Clark): Support dense unions.
        raise NotImplementedError(
            "Custom slice view serialization of dense union arrays is not yet "
            "supported."
        )

    # Dispatch to handler for array type.
    if pa.types.is_null(a.type):
        return _null_array_to_array_payload(a)
    elif _is_primitive(a.type):
        return _primitive_array_to_array_payload(a)
    elif _is_binary(a.type):
        return _binary_array_to_array_payload(a)
    elif pa.types.is_list(a.type) or pa.types.is_large_list(a.type):
        return _list_array_to_array_payload(a)
    elif pa.types.is_fixed_size_list(a.type):
        return _fixed_size_list_array_to_array_payload(a)
    elif pa.types.is_struct(a.type):
        return _struct_array_to_array_payload(a)
    elif pa.types.is_union(a.type):
        return _union_array_to_array_payload(a)
    elif pa.types.is_dictionary(a.type):
        return _dictionary_array_to_array_payload(a)
    elif pa.types.is_map(a.type):
        return _map_array_to_array_payload(a)
    elif isinstance(a.type, tensor_extension_types):
        return _tensor_array_to_array_payload(a)
    elif isinstance(a.type, pa.ExtensionType):
        return _extension_array_to_array_payload(a)
    else:
        raise ValueError("Unhandled Arrow array type:", a.type)


def _is_primitive(type_: "pyarrow.DataType") -> bool:
    """Whether the provided Array type is primitive (boolean, numeric, temporal or
    fixed-size binary)."""
    import pyarrow as pa

    return (
        pa.types.is_integer(type_)
        or pa.types.is_floating(type_)
        or pa.types.is_decimal(type_)
        or pa.types.is_boolean(type_)
        or pa.types.is_temporal(type_)
        or pa.types.is_fixed_size_binary(type_)
    )


def _is_binary(type_: "pyarrow.DataType") -> bool:
    """Whether the provided Array type is a variable-sized binary type."""
    import pyarrow as pa

    return (
        pa.types.is_string(type_)
        or pa.types.is_large_string(type_)
        or pa.types.is_binary(type_)
        or pa.types.is_large_binary(type_)
    )


def _null_array_to_array_payload(a: "pyarrow.NullArray") -> "PicklableArrayPayload":
    """Serialize null array to PicklableArrayPayload."""
    # Buffer scheme: [None]
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[None],  # Single null buffer is expected.
        null_count=a.null_count,
        offset=0,
        children=[],
    )


def _primitive_array_to_array_payload(a: "pyarrow.Array") -> "PicklableArrayPayload":
    """Serialize primitive (numeric, temporal, boolean) arrays to
    PicklableArrayPayload.
    """
    assert _is_primitive(a.type), a.type
    # Buffer scheme: [bitmap, data]
    buffers = a.buffers()
    assert len(buffers) == 2, len(buffers)

    # Copy bitmap buffer, if needed.
    bitmap_buf = buffers[0]
    if a.null_count > 0:
        bitmap_buf = _copy_bitpacked_buffer_if_needed(bitmap_buf, a.offset, len(a))
    else:
        bitmap_buf = None

    # Copy data buffer, if needed.
    data_buf = buffers[1]
    if data_buf is not None:
        data_buf = _copy_buffer_if_needed(buffers[1], a.type, a.offset, len(a))

    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[bitmap_buf, data_buf],
        null_count=a.null_count,
        offset=0,
        children=[],
    )


def _binary_array_to_array_payload(a: "pyarrow.Array") -> "PicklableArrayPayload":
    """Serialize binary (variable-sized binary, string) arrays to
    PicklableArrayPayload.
    """
    assert _is_binary(a.type), a.type
    # Buffer scheme: [bitmap, value_offsets, data]
    buffers = a.buffers()
    assert len(buffers) == 3, len(buffers)

    # Copy bitmap buffer, if needed.
    if a.null_count > 0:
        bitmap_buf = _copy_bitpacked_buffer_if_needed(buffers[0], a.offset, len(a))
    else:
        bitmap_buf = None

    # Copy offset buffer, if needed.
    offset_buf = buffers[1]
    offset_buf, data_offset, data_length = _copy_offsets_buffer_if_needed(
        offset_buf, a.type, a.offset, len(a)
    )
    data_buf = buffers[2]
    data_buf = _copy_buffer_if_needed(data_buf, None, data_offset, data_length)
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[bitmap_buf, offset_buf, data_buf],
        null_count=a.null_count,
        offset=0,
        children=[],
    )


def _list_array_to_array_payload(a: "pyarrow.Array") -> "PicklableArrayPayload":
    """Serialize list (regular and large) arrays to PicklableArrayPayload."""
    # Dedicated path for ListArrays. These arrays have a nested set of bitmap and
    # offset buffers, eventually bottoming out on a data buffer.
    # Buffer scheme:
    # [bitmap, offsets, bitmap, offsets, ..., bitmap, data]
    buffers = a.buffers()
    assert len(buffers) > 1, len(buffers)

    # Copy bitmap buffer, if needed.
    if a.null_count > 0:
        bitmap_buf = _copy_bitpacked_buffer_if_needed(buffers[0], a.offset, len(a))
    else:
        bitmap_buf = None

    # Copy offset buffer, if needed.
    offset_buf = buffers[1]
    offset_buf, child_offset, child_length = _copy_offsets_buffer_if_needed(
        offset_buf, a.type, a.offset, len(a)
    )

    # Propagate slice to child.
    child = a.values.slice(child_offset, child_length)

    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[bitmap_buf, offset_buf],
        null_count=a.null_count,
        offset=0,
        children=[_array_to_array_payload(child)],
    )


def _fixed_size_list_array_to_array_payload(
    a: "pyarrow.FixedSizeListArray",
) -> "PicklableArrayPayload":
    """Serialize fixed size list arrays to PicklableArrayPayload."""
    # Dedicated path for fixed-size lists.
    # Buffer scheme:
    # [bitmap, values_bitmap, values_data, values_subbuffers...]
    buffers = a.buffers()
    assert len(buffers) >= 1, len(buffers)

    # Copy bitmap buffer, if needed.
    if a.null_count > 0:
        bitmap_buf = _copy_bitpacked_buffer_if_needed(buffers[0], a.offset, len(a))
    else:
        bitmap_buf = None

    # Propagate slice to child.
    child_offset = a.type.list_size * a.offset
    child_length = a.type.list_size * len(a)
    child = a.values.slice(child_offset, child_length)

    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[bitmap_buf],
        null_count=a.null_count,
        offset=0,
        children=[_array_to_array_payload(child)],
    )


def _struct_array_to_array_payload(a: "pyarrow.StructArray") -> "PicklableArrayPayload":
    """Serialize struct arrays to PicklableArrayPayload."""
    # Dedicated path for StructArrays.
    # StructArrays have a top-level bitmap buffer and one or more children arrays.
    # Buffer scheme: [bitmap, None, child_bitmap, child_data, ...]
    buffers = a.buffers()
    assert len(buffers) >= 1, len(buffers)

    # Copy bitmap buffer, if needed.
    if a.null_count > 0:
        bitmap_buf = _copy_bitpacked_buffer_if_needed(buffers[0], a.offset, len(a))
    else:
        bitmap_buf = None

    # Get field children payload.
    # Offsets and truncations are already propagated to the field arrays, so we can
    # serialize them as-is.
    children = [_array_to_array_payload(a.field(i)) for i in range(a.type.num_fields)]
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[bitmap_buf],
        null_count=a.null_count,
        offset=0,
        children=children,
    )


def _union_array_to_array_payload(a: "pyarrow.UnionArray") -> "PicklableArrayPayload":
    """Serialize union arrays to PicklableArrayPayload."""
    import pyarrow as pa

    # Dedicated path for UnionArrays.
    # UnionArrays have a top-level bitmap buffer and type code buffer, and one or
    # more children arrays.
    # Buffer scheme: [None, typecodes, child_bitmap, child_data, ...]
    assert not _is_dense_union(a.type)
    buffers = a.buffers()
    assert len(buffers) > 1, len(buffers)

    bitmap_buf = buffers[0]
    assert bitmap_buf is None, bitmap_buf

    # Copy type code buffer, if needed.
    type_code_buf = buffers[1]
    type_code_buf = _copy_buffer_if_needed(type_code_buf, pa.int8(), a.offset, len(a))

    # Get field children payload.
    # Offsets and truncations are already propagated to the field arrays, so we can
    # serialize them as-is.
    children = [_array_to_array_payload(a.field(i)) for i in range(a.type.num_fields)]
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[bitmap_buf, type_code_buf],
        null_count=a.null_count,
        offset=0,
        children=children,
    )


def _dictionary_array_to_array_payload(
    a: "pyarrow.DictionaryArray",
) -> "PicklableArrayPayload":
    """Serialize dictionary arrays to PicklableArrayPayload."""
    # Dedicated path for DictionaryArrays.
    # Buffer scheme: [indices_bitmap, indices_data] (dictionary stored separately)
    indices_payload = _array_to_array_payload(a.indices)
    dictionary_payload = _array_to_array_payload(a.dictionary)
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[],
        null_count=a.null_count,
        offset=0,
        children=[indices_payload, dictionary_payload],
    )


def _map_array_to_array_payload(a: "pyarrow.MapArray") -> "PicklableArrayPayload":
    """Serialize map arrays to PicklableArrayPayload."""
    import pyarrow as pa

    # Dedicated path for MapArrays.
    # Buffer scheme: [bitmap, offsets, child_struct_array_buffers, ...]
    buffers = a.buffers()
    assert len(buffers) > 0, len(buffers)

    # Copy bitmap buffer, if needed.
    if a.null_count > 0:
        bitmap_buf = _copy_bitpacked_buffer_if_needed(buffers[0], a.offset, len(a))
    else:
        bitmap_buf = None

    new_buffers = [bitmap_buf]

    # Copy offsets buffer, if needed.
    offset_buf = buffers[1]
    offset_buf, data_offset, data_length = _copy_offsets_buffer_if_needed(
        offset_buf, a.type, a.offset, len(a)
    )

    if isinstance(a, pa.lib.ListArray):
        # Map arrays directly expose the one child struct array in pyarrow>=7.0.0, which
        # is easier to work with than the raw buffers.
        new_buffers.append(offset_buf)
        children = [_array_to_array_payload(a.values.slice(data_offset, data_length))]
    else:
        # In pyarrow<7.0.0, the child struct array is not exposed, so we work with the
        # key and item arrays.
        buffers = a.buffers()
        assert len(buffers) > 2, len(buffers)
        # Reconstruct offsets array.
        offsets = pa.Array.from_buffers(
            pa.int32(), len(a) + 1, [bitmap_buf, offset_buf]
        )
        # Propagate slice to keys.
        keys = a.keys.slice(data_offset, data_length)
        # Propagate slice to items.
        items = a.items.slice(data_offset, data_length)
        children = [
            _array_to_array_payload(offsets),
            _array_to_array_payload(keys),
            _array_to_array_payload(items),
        ]
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=new_buffers,
        null_count=a.null_count,
        offset=0,
        children=children,
    )


def _tensor_array_to_array_payload(a: "ArrowTensorArray") -> "PicklableArrayPayload":
    """Serialize tensor arrays to PicklableArrayPayload."""
    # Offset is propagated to storage array, and the storage array items align with the
    # tensor elements, so we only need to do the straightforward creation of the storage
    # array payload.
    storage_payload = _array_to_array_payload(a.storage)
    return PicklableArrayPayload(
        type=a.type,
        length=len(a),
        buffers=[],
        null_count=a.null_count,
        offset=0,
        children=[storage_payload],
    )


def _extension_array_to_array_payload(
    a: "pyarrow.ExtensionArray",
) -> "PicklableArrayPayload":
    payload = _array_to_array_payload(a.storage)
    payload.type = a.type
    payload.length = len(a)
    payload.null_count = a.null_count
    return payload


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
