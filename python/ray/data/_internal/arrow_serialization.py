import logging
import os
import sys
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION"
)
RAY_DISABLE_CUSTOM_ARROW_ARRAY_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_ARRAY_SERIALIZATION"
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


def _register_custom_serializers():
    # Register all custom serializers required by Datasets.
    _register_arrow_array_serializer()
    _register_arrow_json_readoptions_serializer()
    _register_arrow_json_parseoptions_serializer()


# Register custom Arrow JSON ReadOptions serializer to workaround it not being picklable
# in Arrow < 8.0.0.
def _register_arrow_json_readoptions_serializer():
    import ray

    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Disabling custom Arrow JSON ReadOptions serialization.")
        return

    try:
        import pyarrow.json as pajson
    except ModuleNotFoundError:
        return

    ray.util.register_serializer(
        pajson.ReadOptions,
        serializer=lambda opts: (opts.use_threads, opts.block_size),
        deserializer=lambda args: pajson.ReadOptions(*args),
    )


def _register_arrow_json_parseoptions_serializer():
    import ray

    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Disabling custom Arrow JSON ParseOptions serialization.")
        return

    try:
        import pyarrow.json as pajson
    except ModuleNotFoundError:
        return

    ray.util.register_serializer(
        pajson.ParseOptions,
        serializer=lambda opts: (
            opts.explicit_schema,
            opts.newlines_in_values,
            opts.unexpected_field_behavior,
        ),
        deserializer=lambda args: pajson.ParseOptions(*args),
    )


# Register custom Arrow array serializer to work around zero-copy slice pickling bug.
# See https://issues.apache.org/jira/browse/ARROW-10739.
def _register_arrow_array_serializer():
    import ray

    if os.environ.get(RAY_DISABLE_CUSTOM_ARROW_ARRAY_SERIALIZATION) == "1":
        import logging

        logger = logging.getLogger(__name__)
        logger.info(
            "Disabling custom Arrow array serialization. This may result in bloated "
            "serialization of Arrow arrays!"
        )
        return

    context = ray._private.worker.global_worker.get_serialization_context()
    array_types = _get_arrow_array_types()
    for array_type in array_types:
        context._register_cloudpickle_reducer(array_type, _arrow_array_reduce)


def _get_arrow_array_types() -> List[type]:
    """Get all Arrow array types that we want to register a custom serializer for."""
    try:
        import pyarrow as pa
    except ModuleNotFoundError:
        # No pyarrow installed so not using Arrow, so no need for custom serializer.
        return []

    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    array_types = [
        pa.lib.NullArray,
        pa.lib.BooleanArray,
        pa.lib.UInt8Array,
        pa.lib.UInt16Array,
        pa.lib.UInt32Array,
        pa.lib.UInt64Array,
        pa.lib.Int8Array,
        pa.lib.Int16Array,
        pa.lib.Int32Array,
        pa.lib.Int64Array,
        pa.lib.Date32Array,
        pa.lib.Date64Array,
        pa.lib.TimestampArray,
        pa.lib.Time32Array,
        pa.lib.Time64Array,
        pa.lib.DurationArray,
        pa.lib.HalfFloatArray,
        pa.lib.FloatArray,
        pa.lib.DoubleArray,
        pa.lib.ListArray,
        pa.lib.LargeListArray,
        pa.lib.MapArray,
        pa.lib.FixedSizeListArray,
        pa.lib.UnionArray,
        pa.lib.BinaryArray,
        pa.lib.StringArray,
        pa.lib.LargeBinaryArray,
        pa.lib.LargeStringArray,
        pa.lib.DictionaryArray,
        pa.lib.FixedSizeBinaryArray,
        pa.lib.Decimal128Array,
        pa.lib.Decimal256Array,
        pa.lib.StructArray,
        pa.lib.ExtensionArray,
        ArrowTensorArray,
        ArrowVariableShapedTensorArray,
    ]
    try:
        array_types.append(pa.lib.MonthDayNanoIntervalArray)
    except AttributeError:
        # MonthDayNanoIntervalArray doesn't exist on older pyarrow versions.
        pass
    return array_types


def _arrow_array_reduce(a: "pyarrow.Array"):
    """Custom reducer for Arrow arrays that works around a zero-copy slicing pickling
    bug.

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

    try:
        maybe_copy = _copy_array_if_needed(a)
    except Exception as e:
        if _is_in_test():
            # If running in a test, we want to raise the error, not fall back.
            raise e from None
        if type(a) not in _serialization_fallback_set:
            logger.warning(
                "Failed to complete optimized serialization of Arrow array of type "
                f"{type(a)}, falling back to default serialization. Note that this "
                "may result in bloated serialized arrays. Error:",
                exc_info=True,
            )
            _serialization_fallback_set.add(type(a))
        maybe_copy = a
    return maybe_copy.__reduce__()


def _copy_array_if_needed(a: "pyarrow.Array") -> "pyarrow.Array":
    """Copy the provided Arrow array, if needed.

    This method recursively traverses the array and subarrays, translating array-level
    slices to buffer-level slices, thereby ensuring a copy at pickle time.
    """
    # See the Arrow buffer layouts for each type for information on how this buffer
    # traversal and copying works:
    # https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
    # TODO(Clark): Preserve null count on copy (if already computed) to prevent it
    # needing to be recomputed on deserialization?
    import pyarrow as pa

    from ray.air.util.tensor_extensions.arrow import (
        ArrowTensorArray,
        ArrowVariableShapedTensorArray,
        _copy_tensor_array_if_needed,
        _copy_variable_shaped_tensor_array_if_needed,
    )

    if pa.types.is_union(a.type) and a.type.mode != "sparse":
        # Dense unions not supported.
        # TODO(Clark): Support dense unions.
        raise NotImplementedError(
            "Custom slice view serialization of dense union arrays is not yet "
            "supported."
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
        bitmap, buf, a.type, a.offset, len(a), buffers=buffers, children=children
    )


def _copy_array_buffers_if_needed(
    bitmap: "pyarrow.Buffer",
    buf: "pyarrow.Buffer",
    type_: "pyarrow.DataType",
    offset: int,
    length: int,
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
            assert type_.mode == "sparse"
            assert buf is not None
            buf = _copy_buffer_if_needed(buf, pa.int8(), offset, length)
            new_buffers.append(buf)
        else:
            assert buf is None
        # We copy the children arrays.
        new_children = []
        for child in children:
            new_children.append(_copy_array_if_needed(child))
    elif pa.types.is_string(type_) or pa.types.is_large_string(type_):
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
    return pa.Array.from_buffers(type_, length, new_buffers, children=new_children)


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

    offset_type = pa.int64() if pa.types.is_large_list(arr_type) else pa.int32()
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
