"""Estimate PyArrow 24's peak memory decoding a Parquet row group, from its footer.

The peak is the I/O (the footer read, ``pre_buffer``'s gaps and the compressed
chunks), what every leaf keeps (levels, values, dictionary, decompression buffer),
the decode-time temporaries of the largest column and most of the next, and a
schema pin's cast. The rules are fitted to measured peaks and lean high.
"""

from typing import Mapping, Optional, Sequence, Set, Tuple

import pyarrow as pa
from pyarrow.parquet import ColumnSchema, RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    _LIST,
    LeafChunk,
    Read,
    detect_delta_byte_array,
    detect_dictionary_columns,
    detect_dictionary_type,
    estimate_added_columns,
    estimate_ancestors,
    estimate_leaf,
    estimate_row_group,
    estimate_string_bytes,
)

# PyArrow reads a file's last 64 KiB for its footer, and pre_buffer coalesces
# chunks across gaps of up to 8 KiB; one gap measured short, two cover it.
_FOOTER_BYTES = 80 << 10
# Below this the footer read took the whole file; 64 KiB put one row group under.
_SMALL_FILE_BYTES = 48 << 10
# Arrow's string and binary view builders allocate value bytes in 32 KiB blocks.
_VIEW_BLOCK_BYTES = 32 << 10
# Twice the 1 MiB dictionary page limit parquet-cpp and arrow-rs default to,
# for a page the footer can't size.
_DICTIONARY_BYTES = 2 << 20
# Casts to text measured above the cast estimate; 1.5 covers all but a few, and
# 3 covered more but raised the worst overestimate by a fifth to a third.
_CAST_FACTOR = 1.5
# Bytes per value as PyArrow decodes each fixed-width physical type.
_PHYSICAL_WIDTH = {
    "BOOLEAN": 1,
    "INT32": 4,
    "INT64": 8,
    "INT96": 12,
    "FLOAT": 4,
    "DOUBLE": 8,
}
# Encodings whose decoders unpack a page's value lengths up front.
_DELTA_ENCODINGS = {"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY"}


def estimate_row_group_peak(
    row_group: RowGroupMetaData,
    leaf_indices: Sequence[int],
    row_group_work: int,
    file_size: int,
) -> int:
    """PyArrow's peak bytes reading ``row_group``, given its work and cast."""
    if file_size < _SMALL_FILE_BYTES:
        # The footer read took the whole file. 512 bytes and 256 a leaf are the
        # least slack that covered the measured peaks.
        return file_size + 512 + 256 * len(leaf_indices) + row_group_work
    compressed = sum(
        row_group.column(leaf_idx).total_compressed_size for leaf_idx in leaf_indices
    )
    return _FOOTER_BYTES + compressed + row_group_work


def estimate_row_group_work(
    row_group: RowGroupMetaData,
    columns: Sequence[ColumnSchema],
    column_leaves: Sequence[Sequence[int]],
    read: Read,
    leaves: Mapping[int, LeafChunk],
    counted: Set[int],
    data_pages: Optional[Sequence[Optional[int]]] = None,
) -> int:
    """What one row group's leaves keep, plus two columns' decode-time buffers.

    ``column_leaves`` holds each column read, in ``columns=`` order, as its leaf
    indices; ``leaves`` the unpinned leaves; ``counted`` the ancestors sized.
    """
    # Two zeros, so there is always a largest column and a next.
    kept, temporaries = 0, [0, 0]
    for position, leaf_indices in enumerate(column_leaves):
        # PyArrow 24 reserves the k-th column's leaves for as many slots as the
        # file's k-th *leaf* has values, whichever column that leaf is in.
        reserved = row_group.column(position).num_values
        # A column's leaves decode in turn: its peak is one leaf's temporaries
        # over what that leaf and the ones before it keep.
        column_kept = column_peak = 0
        for leaf_idx in leaf_indices:
            leaf = leaves[leaf_idx]
            ancestors = estimate_ancestors(leaf=leaf, counted=counted)
            leaf_kept, leaf_temporary = estimate_leaf_work(
                leaf=leaf,
                column=columns[leaf_idx],
                reserved=reserved,
                rows=row_group.num_rows,
                read=read,
                ancestors=ancestors,
                pages=data_pages[leaf_idx] if data_pages else None,
            )
            column_kept += ancestors + leaf_kept
            column_peak = max(column_peak, column_kept + leaf_temporary)
        kept += column_kept
        temporaries.append(column_peak - column_kept)
    # Columns decode in parallel: the peaks fit the largest column's temporaries
    # and 3/4 of the next one's (1/2 left 8 row groups under), or all of it when
    # the two are within 10%.
    second, first = sorted(temporaries)[-2:]
    return kept + first + (second if 10 * second >= 9 * first else 3 * second // 4)


def estimate_leaf_work(
    leaf: LeafChunk,
    column: ColumnSchema,
    reserved: int,
    rows: int,
    read: Read,
    ancestors: int = 0,
    pages: Optional[int] = None,
) -> Tuple[int, int]:
    """One leaf's buffers: (kept until the row group is read, temporary)."""
    chunk = leaf.chunk
    levels = chunk.num_values
    # Buffers start at ``reserved`` slots and double to a power of two.
    capacity = 1 << (max(levels, reserved, 1) - 1).bit_length()
    repeated = column.max_repetition_level > 0
    nullable = column.max_definition_level > 0
    # A repeated leaf's buffers measured up to one doubling past ``capacity``.
    grows = 2 if repeated else 1
    decoded = estimate_leaf(leaf=leaf, read=read)
    dictionary = _dictionary_bytes(leaf)
    # A DELTA_BYTE_ARRAY guess (128 times its pages) covers the decoder's buffers.
    guessed = chunk.physical_type == "BYTE_ARRAY" and leaf.size_statistics_bytes is None
    guessed = guessed and detect_delta_byte_array(chunk)
    # int16 definition and repetition levels, and the decoded leaf.
    kept = 2 * capacity * (nullable + repeated) + decoded
    temporary = 0
    reader_bytes = 0
    if chunk.compression != "UNCOMPRESSED":
        # Pages decompress into a buffer the size of the largest page or the
        # dictionary page. Pages fill to a byte target, so the largest is at
        # most twice the average; without a page count, the whole chunk.
        uncompressed = chunk.total_uncompressed_size
        page = uncompressed if pages is None else 2 * uncompressed // max(pages, 1)
        reader_bytes = max(min(page, uncompressed), dictionary)
    if _DELTA_ENCODINGS & set(chunk.encodings) and not guessed:
        # DELTA decoders unpack a page's int32 lengths, 4 bytes a value, and
        # DELTA_BYTE_ARRAY's its prefix lengths and rebuilt values too.
        state = 4 * levels
        if "DELTA_BYTE_ARRAY" in chunk.encodings:
            state = 8 * levels + decoded
        reader_bytes += min(state, 2 * state // max(pages or 1, 1))
    if reserved > rows:
        # Reserved past the rows (a list's position), the page reader measured
        # as held only while the leaf decodes.
        temporary += reader_bytes
    else:
        kept += reader_bytes
    # A list's offsets for every slot reserved, past what its ancestors keep.
    list_offsets = repeated * max(_list_offset_width(leaf) * reserved - ancestors, 0)
    if chunk.physical_type == "BYTE_ARRAY":
        # Without a header, each dictionary entry is at least its 4-byte length.
        header = leaf.dictionary_page
        entries = dictionary // 4 if header is None else header.num_values
        view = leaf.type in (pa.string_view(), pa.binary_view())
        large = leaf.type in (pa.large_string(), pa.large_binary())
        width = 16 if view else 8 if large else 4
        # The dictionary, parquet-cpp's 16-byte length and pointer per entry,
        # and a view builder's first block.
        kept += dictionary + 16 * entries + view * _VIEW_BLOCK_BYTES
        slots = leaf.slots()
        # Offsets reserved past the slots used; views double as lists grow.
        temporary += width * max((grows if view else 1) * capacity - slots, 0)
        # The 128-times guess already covers the value bytes.
        chars = 0 if guessed else estimate_string_bytes(leaf=leaf)
        as_dictionary = detect_dictionary_columns(
            chunk=chunk, format_kwargs=read.format_kwargs
        ) or detect_dictionary_type(arrow_type=leaf.type)
        if as_dictionary:
            # A hash table of the distinct values, the values twice (builders
            # double) and int32 indices. Distinct values are at most one per slot
            # of at least 4 bytes, and no fewer than the page's entries.
            distinct = max(min(slots, max(decoded - slots, 0) // 4 + 1), entries)
            # A flat leaf's are the page's entries, unless its data pages are over
            # twice what their indices need: a PLAIN fallback (up to 10 times
            # measured the same, 20 let one through). Lists measured above it.
            indices = levels * max(entries - 1, 1).bit_length() // 8
            data = chunk.total_uncompressed_size - dictionary
            if chunk.has_dictionary_page and not repeated and data <= 2 * indices:
                distinct = entries
            builder_values = 2 * min(chars, decoded)
            temporary += _hash_table_bytes(distinct) + builder_values + 4 * slots
        elif not view:
            # The builder's copy of the values; a view builder appends blocks.
            temporary += chars
        # A string list's offsets are held alongside the builder: both count.
        return kept, temporary + list_offsets
    # Fixed width: the dictionary, then values and validity at capacity.
    kept += dictionary
    slots = grows * capacity
    if repeated and _list_offset_width(leaf) == 4:
        # A list's values start at the slots reserved and grow by the levels past
        # the rows; a large_list keeps the doubling, which one measured above.
        slots = 1 << (max(levels, reserved + levels - rows, 1) - 1).bit_length()
    validity = nullable * slots // 8
    if chunk.physical_type == "FIXED_LEN_BYTE_ARRAY":
        # The builder's values, and from a dictionary its values again.
        values, zero_copy = slots * column.length, False
        temporary += chunk.has_dictionary_page * values
    else:
        values = slots * _PHYSICAL_WIDTH[chunk.physical_type]
        zero_copy = _zero_copy(physical_type=chunk.physical_type, arrow_type=leaf.type)
    if zero_copy:
        # Trimmed and handed over: the decoded size counts what's kept.
        temporary += max(values - decoded, 0) + validity
    else:
        # Converted into a new array; the decode buffer measured as kept.
        kept += values + validity
    # A fixed-width list's offsets come after its values: the larger counts.
    return kept, max(temporary, list_offsets)


def estimate_cast(
    row_group: RowGroupMetaData,
    column_leaves: Mapping[str, Sequence[LeafChunk]],
    file_schema: pa.Schema,
    pinned_schema: pa.Schema,
    pieces: int,
    read: Read,
) -> int:
    """What a pin's cast allocates for one row group, times 1.5: a crude bound.

    ``column_leaves`` holds each column read as its leaves, profiled as pinned. A
    nested or view cast copies once per one of the ``pieces`` the scanner yields.
    """
    size = estimate_added_columns(row_group=row_group, read=read)
    for name, leaves in column_leaves.items():
        before, after = file_schema.field(name).type, pinned_schema.field(name).type
        if before == after:
            continue
        copies = pieces if _nested_or_view(before) or _nested_or_view(after) else 1
        size += copies * estimate_row_group(
            row_group=row_group,
            leaves=leaves,
            read=read._replace(added_row_bytes=0, added_row_group_bytes=0),
        )
    return int(_CAST_FACTOR * size)


def _dictionary_bytes(leaf: LeafChunk) -> int:
    """The dictionary page's uncompressed bytes: from its header, else its gap."""
    chunk = leaf.chunk
    if not chunk.has_dictionary_page:
        return 0
    if leaf.dictionary_page is not None:
        return leaf.dictionary_page.uncompressed_page_size
    # The bytes before the first data page at the chunk's ratio, never below
    # them, since a page that doesn't compress is stored as is.
    gap = chunk.data_page_offset - (chunk.dictionary_page_offset or 0)
    if 0 < gap <= chunk.total_compressed_size:
        ratio = chunk.total_uncompressed_size / max(chunk.total_compressed_size, 1)
        return int(gap * max(1.0, ratio))
    # Offsets that don't bound the page: the chunk's bytes, at most 2 MiB.
    return min(chunk.total_uncompressed_size, _DICTIONARY_BYTES)


def _list_offset_width(leaf: LeafChunk) -> int:
    """Bytes per offset of the widest list above ``leaf``, 4 if none."""
    return max(
        (node.width for node in leaf.profile.nodes[:-1] if node.kind == _LIST),
        default=4,
    )


def _hash_table_bytes(distinct: int) -> int:
    """Arrow's memo table: 16-byte slots, from 32, quadrupling while half full."""
    slots = 32
    while slots <= 2 * distinct:
        slots *= 4
    return 16 * slots


def _zero_copy(physical_type: str, arrow_type: pa.DataType) -> bool:
    """Whether PyArrow hands the decoded values to Arrow without a copy."""
    # Only these: it copies into every other type, even date32.
    if physical_type in ("INT32", "INT64"):
        return arrow_type in (pa.int32(), pa.int64()) or (
            physical_type == "INT64" and pa.types.is_timestamp(arrow_type)
        )
    return (physical_type, arrow_type) in (
        ("FLOAT", pa.float32()),
        ("DOUBLE", pa.float64()),
    )


def _nested_or_view(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` has a list, map, union or view: casts copy per piece."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        return any(_nested_or_view(field.type) for field in arrow_type)
    return (
        pa.types.is_nested(arrow_type)
        or pa.types.is_string_view(arrow_type)
        or pa.types.is_binary_view(arrow_type)
    )
