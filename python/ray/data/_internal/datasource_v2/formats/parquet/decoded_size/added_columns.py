"""Bytes of the columns Ray adds after decoding."""

from __future__ import annotations

import math
from typing import Any, Mapping, NamedTuple, Sequence

import pyarrow as pa
from pyarrow.parquet import RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.decoded_size.arrow_layout import (
    _is_list,
    _is_text,
    _is_view,
    _offset_width,
)


class Read(NamedTuple):
    """What a read task does besides decoding the file's columns."""

    # Bytes per row of the columns Ray appends, and once per row group on top.
    added_row_bytes: float = 0
    added_row_group_bytes: int = 0


# Columns Ray adds after decoding: the file's path, partition values, a row hash,
# a checkpoint ID, and every dataset column the pin fills with nulls, which the
# file lacks or holds as a null-typed column.
def estimate_added_columns(row_group: RowGroupMetaData, read: Read) -> int:
    """Bytes of the columns Ray appends to one row group."""
    per_row = math.ceil(row_group.num_rows * read.added_row_bytes)
    return per_row + read.added_row_group_bytes


# Bytes an added array takes once per row group: its bitmap's last byte and the
# offset past its last element.
_ADDED_ROW_GROUP_BYTES = 9


def added_bytes(
    added: Sequence[pa.Field], path: str, partition_values: Mapping[str, Any]
) -> Read:
    """The bytes of the columns Ray adds to a file's rows.

    Args:
        added: The columns Ray appends, and every dataset column the pin fills with
            nulls: one the file lacks, or holds as a null-typed column.
        path: The file's path, the text of the column named ``path``.
        partition_values: The file's partition values, as the reader parses them:
            ``{"year": "2024"}`` gives ``year`` 4 bytes of text a row, a null none.

    Returns:
        The columns' bytes per row and once per row group.
    """
    per_row, per_row_group = 0.0, 0
    for field in added:
        if field.name == "path":
            text = path
        else:
            value = partition_values.get(field.name)
            text = "" if value is None else str(value)
        row, once = _added_array_bytes(
            arrow_type=field.type, text_bytes=len(text.encode())
        )
        per_row += row
        per_row_group += once
    return Read(added_row_bytes=per_row, added_row_group_bytes=per_row_group)


def _added_array_bytes(arrow_type: pa.DataType, text_bytes: int) -> tuple[float, int]:
    """Bytes per element of an added array and its children, and once per row group.

    Widths follow the Arrow columnar format. Besides the table, every array but a
    null-type one is charged a validity bit a row and ``_ADDED_ROW_GROUP_BYTES``
    once: the bitmap's last byte (1) and the offset past its last element (at
    most 8). A list's or map's child is charged its once-bytes only, since the
    cast fills the column with nulls and leaves the child empty.

    ========================  ================================  ====================
    Type                      Per row                           Once
    ========================  ================================  ====================
    null                      0, no buffers                     0
    fixed width, bool         ``bit_width / 8``
    string, binary            offset (4, ``large_``: 8) + text
    string_view, binary_view  16-byte view + text
    struct                    each child's                      each child's
    fixed_size_list           ``list_size`` x the child's       the child's
    list, map                 offset (4, ``large_list``: 8)     the child's, empty
    dictionary                index ``bit_width / 8``           one dictionary value
    union, list view          16, a rough guess
    ========================  ================================  ====================

    Two cases come out high: a column without nulls, as the path, has no bitmap,
    and a view holds a value of 12 bytes or less inside its 16.
    """
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_null(arrow_type):
        return 0, 0

    def below(child_type: pa.DataType, child_text: int) -> tuple[float, int]:
        return _added_array_bytes(arrow_type=child_type, text_bytes=child_text)

    # A validity bit a row.
    row, once = 1 / 8, _ADDED_ROW_GROUP_BYTES
    if pa.types.is_struct(arrow_type):
        for field in arrow_type:
            child_row, child_once = below(field.type, text_bytes)
            row, once = row + child_row, once + child_once
    elif pa.types.is_fixed_size_list(arrow_type):
        child_row, child_once = below(arrow_type.value_type, text_bytes)
        row, once = row + arrow_type.list_size * child_row, once + child_once
    elif pa.types.is_map(arrow_type):
        entry = pa.struct([arrow_type.key_field, arrow_type.item_field])
        row, once = row + 4, once + below(entry, 0)[1]
    elif _is_list(arrow_type):
        row += 8 if pa.types.is_large_list(arrow_type) else 4
        once += below(arrow_type.value_type, 0)[1]
    elif pa.types.is_dictionary(arrow_type):
        row += arrow_type.index_type.bit_width / 8
        value_row, value_once = below(arrow_type.value_type, text_bytes)
        once += math.ceil(value_row) + value_once
    elif _is_text(arrow_type):
        # A view takes 16 bytes an element; a string or binary, its offset.
        width = 16 if _is_view(arrow_type) else _offset_width(arrow_type)
        row += width + text_bytes
    else:
        try:
            row += arrow_type.bit_width / 8
        except ValueError:
            # No fixed width, as a list view or a union: a crude 16 bytes a row.
            row += 16
    return row, once
