"""Arrow array kinds and widths."""

from __future__ import annotations

import pyarrow as pa

# Array kinds. ``_LIST`` has one offsets buffer: a list, large_list or map. A
# fixed_size_list has none; its children hold ``list_size`` elements per element.
_STRUCT = "struct"
_LIST = "list"
_FIXED_SIZE_LIST = "fixed_size_list"
_FIXED_WIDTH = "fixed_width"
_BOOLEAN = "boolean"
_NULL = "null"  # Arrow's null type allocates no buffers.
_UNSUPPORTED = "unsupported"  # A view, a dictionary, a union: not modelled.
# String and binary leaves, which only BYTE_ARRAY values decode to.
_BINARY = "binary"

# PyArrow 24 hands these to Arrow zero-copy, keeping an optional leaf's all-valid
# bitmap; other types drop it. INT96 timestamps are converted, not zero-copied.
_ZERO_COPY_PHYSICAL_TYPES = frozenset({"INT32", "INT64", "FLOAT", "DOUBLE"})


def _is_zero_copy_type(arrow_type: pa.DataType) -> bool:
    numeric = (pa.int32(), pa.int64(), pa.float32(), pa.float64())
    return arrow_type in numeric or pa.types.is_timestamp(arrow_type)


def _leaf_layout(arrow_type: pa.DataType) -> tuple[str, int]:
    """``(kind, width)`` of a leaf's Arrow type."""
    if pa.types.is_null(arrow_type):
        return _NULL, 0
    if pa.types.is_boolean(arrow_type):
        return _BOOLEAN, 0
    if detect_string_type(arrow_type):
        return _BINARY, _offset_width(arrow_type)
    try:
        bit_width = arrow_type.bit_width
    except ValueError:
        # No fixed width: a view, a union, a list view.
        return _UNSUPPORTED, 0
    if pa.types.is_dictionary(arrow_type) or bit_width % 8:
        return _UNSUPPORTED, 0
    return _FIXED_WIDTH, bit_width // 8


def _is_repeated(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` is a list, a map, or a struct of only those."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        return arrow_type.num_fields > 0 and all(
            _is_repeated(field.type) for field in arrow_type
        )
    return _is_list(arrow_type) or pa.types.is_map(arrow_type)


def _is_list(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )


def _bytes_for_bits(count: int) -> int:
    """Bytes holding ``count`` bits: a validity bitmap or boolean values."""
    return (count + 7) // 8


# Plain string and binary leaves.
def detect_string_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_large_binary(arrow_type)
    )


def _offset_width(arrow_type: pa.DataType) -> int:
    """Bytes of a string or binary value's offset."""
    if pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 8
    return 4


def _is_view(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string_view(arrow_type) or pa.types.is_binary_view(arrow_type)


def _is_text(arrow_type: pa.DataType) -> bool:
    return detect_string_type(arrow_type) or _is_view(arrow_type)
