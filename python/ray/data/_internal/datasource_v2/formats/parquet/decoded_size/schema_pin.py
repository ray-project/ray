"""The schema walk redone at the types a read casts every batch to."""

from __future__ import annotations

import pyarrow as pa
from pyarrow.parquet import ParquetSchema

from ray.data._internal.datasource_v2.formats.parquet.decoded_size.arrow_layout import (
    _BINARY,
    _is_list,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.schema_walk import (
    _matches,
    _SchemaWalk,
    _walk,
)


def _cast_keeps_bitmap(stored: pa.DataType, pinned: pa.DataType) -> bool:
    """Whether casting a null-free ``stored`` array keeps its bitmap, as measured on
    PyArrow 23 to 26: an integer to a temporal type of its width (int32 to date32)
    and a timestamp to a timestamp or int64 do; the rest, as int32 to int64, don't."""
    if stored == pinned:
        return True
    if pa.types.is_timestamp(stored):
        return pa.types.is_timestamp(pinned) or pinned == pa.int64()
    if pa.types.is_integer(stored):
        return pa.types.is_temporal(pinned) and pinned.bit_width == stored.bit_width
    return False


# A read involves three schemas:
# 1. The file's Parquet schema: how the columns are stored. A leaf is one stored
#    column of primitive values, so a struct of three ints is three leaves. The
#    footer records its numbers (value counts, null counts, string bytes) per leaf.
# 2. The file's Arrow schema: what PyArrow decodes those leaves into. A decoded
#    value's size often differs from the stored one's:
#    - A 4-byte INT32 annotated Int(16) decodes to a 2-byte int16.
#    - A 4-byte FIXED_LEN_BYTE_ARRAY Decimal(9, 2) decodes to a 16-byte decimal128.
#    - A 12-byte INT96 decodes to an 8-byte timestamp[ns].
#    - A BYTE_ARRAY String decodes to string (4-byte offsets), large_string (8-byte
#      offsets) or a dictionary, whichever the footer's ARROW:schema metadata says.
#    - An INT32 annotated Null decodes to a null array, which holds no bytes.
# 3. Ray's pin, the dataset schema every batch is then cast to, unless it holds an
#    extension type (``FileReader._file_dataset_schema``).
def pinned_type(field: pa.Field, pinned_schema: pa.Schema | None) -> pa.DataType | None:
    """The type the pin casts a file column to, or ``None`` if it keeps its own."""
    if pinned_schema is None:
        return None
    idx = pinned_schema.get_field_index(field.name)
    if idx < 0 or pinned_schema.field(idx).type == field.type:
        return None
    return pinned_schema.field(idx).type


def _pinned_walk(
    file_walk: _SchemaWalk,
    arrow_schema: pa.Schema,
    pinned_schema: pa.Schema,
    parquet_schema: ParquetSchema,
) -> tuple[_SchemaWalk, set[int]]:
    """``file_walk`` redone at the pinned types, and the leaves of columns whose
    cast changes their shape: a struct that gained or reordered fields, a nested
    field made nullable, numbers cast to text. Those are walked as stored and left
    unmodelled."""
    # A null-typed column keeps its type, sized 0: the caller counts the nulls the
    # cast fills it with as an added column (``added_bytes``).
    casts: dict[int, pa.Field] = {}
    for idx, field in enumerate(arrow_schema):
        if pa.types.is_null(field.type):
            continue
        arrow_type = pinned_type(field=field, pinned_schema=pinned_schema)
        if arrow_type is not None:
            casts[idx] = field.with_type(arrow_type)
    if not casts:
        return file_walk, set()
    fields = [casts.get(idx, field) for idx, field in enumerate(arrow_schema)]
    walk = _walk(pa.schema(fields))
    unmapped: set[int] = set()
    for idx, pinned in casts.items():
        stored = file_walk.field_leaves(idx)
        # The cast matches struct fields by name, the walk by position.
        if _struct_names(pinned.type) != _struct_names(
            arrow_schema.field(idx).type
        ) or not _cast_maps(
            file_walk=file_walk,
            stored=stored,
            walk=walk,
            pinned=walk.field_leaves(idx),
            parquet_schema=parquet_schema,
        ):
            unmapped.update(stored)
            fields[idx] = arrow_schema.field(idx)
    if unmapped:
        walk = _walk(pa.schema(fields))
    return walk, unmapped


def _struct_names(arrow_type: pa.DataType) -> tuple:
    """Every struct's field names in ``arrow_type``, nested, in their order."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        return tuple((field.name, _struct_names(field.type)) for field in arrow_type)
    if pa.types.is_map(arrow_type):
        return (_struct_names(arrow_type.key_type), _struct_names(arrow_type.item_type))
    if _is_list(arrow_type):
        return (_struct_names(arrow_type.value_type),)
    return ()


def _cast_maps(
    file_walk: _SchemaWalk,
    stored: range,
    walk: _SchemaWalk,
    pinned: range,
    parquet_schema: ParquetSchema,
) -> bool:
    """Whether a column's ``stored`` leaves follow its ``pinned`` ones one for one."""
    if len(stored) != len(pinned):
        return False
    for stored_idx, pinned_idx in zip(stored, pinned):
        chain, rep = walk.leaves[pinned_idx]
        leaf = chain[-1]
        if not _matches(leaf=leaf, rep=rep, column=parquet_schema.column(stored_idx)):
            return False
        # Text keeps its bytes; anything else cast to text, as a decimal stored as
        # BYTE_ARRAY, gets new ones the footer doesn't count.
        if leaf.kind == _BINARY and file_walk.leaves[stored_idx][0][-1].kind != _BINARY:
            return False
    return True
