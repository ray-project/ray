"""Estimate PyArrow's peak memory decoding a Parquet row group, from its footer.

Terms: a column is what ``columns=`` names, and its leaves are the primitive
columns Parquet stores it as: an ``int64`` column has one leaf, a ``struct<a:
int64, b: string>`` two. A leaf's data in one row group is a chunk, stored as
pages. PyArrow decodes a row group leaf by leaf into Arrow arrays, whose
entries, values or nulls, are slots. A leaf's buffers are either kept until the
row group is read or temporary, freed once that leaf is decoded.

Nothing is decoded here. The inputs are the footer, the data page counts
``read_footer_stats`` reads from it, the file's size, and the dictionary page
headers the footer reader fetches. Modelled on PyArrow 24's reader, a row group's peak is the sum of:

1. I/O: PyArrow's footer read, the gaps its ``pre_buffer`` option reads between
   chunks, and the row group's compressed chunks; for a small file, which the
   footer read took whole, the file's size instead.
2. Every leaf's kept buffers: levels, decoded values, the decoded dictionary
   and, mostly, the page reader's decompression buffer.
3. Temporaries: buffers reserved past what's used, builders and list offsets.
   A column's leaves decode in turn and columns in parallel, so this is the
   largest column's temporaries and most of the next one's.
4. A schema pin's cast.

The rules are fitted to measured peaks and lean high: an underestimate is what
runs a node out of memory.
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
# Below this the footer read took the whole file; 64 KiB measured one row group
# under the floor.
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
        # The footer read took the whole file, which stands in for the footer
        # and the chunks. 512 bytes and 256 a leaf are the least slack over the
        # file's size that covered the measured peaks; without it, 5 to 10 row
        # groups fell under the floor.
        return file_size + 512 + 256 * len(leaf_indices) + row_group_work
    # The footer read and gaps, plus the chunks pre_buffer holds compressed.
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
        # on top of what that leaf and the leaves before it keep.
        column_kept = column_peak = 0
        for leaf_idx in leaf_indices:
            leaf = leaves[leaf_idx]
            ancestors = estimate_ancestors(leaf=leaf, counted=counted)
            # Without the footer's page counts, a leaf is taken as one page.
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
    # Columns decode in parallel but seldom peak together: the measured peaks
    # fit the largest column's temporaries and three quarters of the next one's
    # (half left 8 row groups under the floor), or all of it when the two are
    # within 10%, where three quarters fell short.
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
    # Buffers start at ``reserved`` slots and double as the levels fill them,
    # so they end at a power of two.
    capacity = 1 << (max(levels, reserved, 1) - 1).bit_length()
    repeated = column.max_repetition_level > 0
    nullable = column.max_definition_level > 0
    # A repeated leaf's buffers measured up to one doubling past ``capacity``.
    grows = 2 if repeated else 1
    decoded = estimate_leaf(leaf=leaf, read=read)
    dictionary = _dictionary_bytes(leaf)
    # Without SizeStatistics, a DELTA_BYTE_ARRAY string leaf's decoded size is
    # already a guess at 128 times its pages, which covers the decoder's buffers.
    guessed = chunk.physical_type == "BYTE_ARRAY" and leaf.size_statistics_bytes is None
    guessed = guessed and detect_delta_byte_array(chunk)
    # Definition and repetition levels per slot, 2 bytes each since PyArrow
    # decodes them as int16, and the decoded leaf.
    kept = 2 * capacity * (nullable + repeated) + decoded
    temporary = 0
    reader_bytes = 0
    if chunk.compression != "UNCOMPRESSED":
        # A compressed page is decompressed into a buffer the size of the largest
        # page or the dictionary page, whichever is bigger. An uncompressed page
        # is decoded where it was read. Writers fill pages to a byte target, so
        # all but the last are full and the largest is at most twice the
        # average; without a page count, the whole chunk.
        uncompressed = chunk.total_uncompressed_size
        page = uncompressed if pages is None else 2 * uncompressed // max(pages, 1)
        reader_bytes = max(min(page, uncompressed), dictionary)
    if _DELTA_ENCODINGS & set(chunk.encodings) and not guessed:
        # A DELTA decoder decodes a page's lengths up front, as int32: 4 bytes a
        # value. A DELTA_BYTE_ARRAY decoder holds int32 prefix lengths too, 8
        # bytes a value, and the values it rebuilds. Sized for the largest
        # page, like the buffer above.
        state = 4 * levels
        if "DELTA_BYTE_ARRAY" in chunk.encodings:
            state = 8 * levels + decoded
        reader_bytes += min(state, 2 * state // max(pages or 1, 1))
    if reserved > rows:
        # Where PyArrow reserved more slots than rows (a list's position), the
        # measured peaks fit the page reader as held only while the leaf decodes.
        temporary += reader_bytes
    else:
        # Elsewhere it stays until the row group is read.
        kept += reader_bytes
    # A list's offsets, built for every slot reserved, past what its ancestors
    # keep already; none for a flat leaf.
    list_offsets = repeated * max(_list_offset_width(leaf) * reserved - ancestors, 0)
    if chunk.physical_type == "BYTE_ARRAY":
        # Strings and binaries: offsets or views, the dictionary, a builder.
        # The dictionary's entries come from its page header; without one, each
        # entry is at least its 4-byte length prefix.
        header = leaf.dictionary_page
        entries = dictionary // 4 if header is None else header.num_values
        # Arrow's layouts: int32 offsets, int64 for the large types, and a
        # 16-byte view.
        view = leaf.type in (pa.string_view(), pa.binary_view())
        large = leaf.type in (pa.large_string(), pa.large_binary())
        width = 16 if view else 8 if large else 4
        # Kept: the decoded dictionary, its bytes and the 16-byte length and
        # pointer parquet-cpp keeps per entry, and a view builder's first block.
        kept += dictionary + 16 * entries + view * _VIEW_BLOCK_BYTES
        slots = leaf.slots()
        # Offsets reserved past the slots used. Views double as their lists grow;
        # 4- and 8-byte offsets measured within one reservation.
        temporary += width * max((grows if view else 1) * capacity - slots, 0)
        # The 128-times guess already covers the value bytes.
        chars = 0 if guessed else estimate_string_bytes(leaf=leaf)
        if _reads_dictionary(leaf=leaf, read=read):
            # Read as an Arrow dictionary, PyArrow builds one: a hash table of the
            # distinct values, the values (twice, since builders grow by
            # doubling) and 4-byte int32 indices.
            # Distinct values are at most one per slot, each taking at least 4
            # of the decoded bytes, and no fewer than the dictionary page's
            # entries.
            distinct = max(min(slots, max(decoded - slots, 0) // 4 + 1), entries)
            # A flat leaf's table holds exactly the page's entries, unless its data
            # pages are over twice what their indices need: the writer fell back
            # to PLAIN, and those pages hold new values. Lists keep the bound: one
            # measured above it. Twice leaves room for page headers and levels; up
            # to 10 times measured the same, and 20 let a fallback chunk through.
            indices = levels * max(entries - 1, 1).bit_length() // 8
            data = chunk.total_uncompressed_size - dictionary
            if chunk.has_dictionary_page and not repeated and data <= 2 * indices:
                distinct = entries
            builder_values = 2 * min(chars, decoded)
            temporary += _hash_table_bytes(distinct) + builder_values + 4 * slots
        elif not view:
            # The value bytes again: the builder doubles them from a page's worth.
            temporary += chars
        # A view builder appends fixed blocks, so it holds no second copy.
        # A string list's offsets are held alongside the builder: both count.
        return kept, temporary + list_offsets
    # Fixed width: the decoded dictionary, then values and validity at capacity.
    kept += dictionary
    slots = grows * capacity
    if repeated and _list_offset_width(leaf) == 4:
        # A list's values start at the slots reserved and grow by the levels
        # past the rows, at most one value per level. A large_list keeps the
        # doubling: one measured above this.
        slots = 1 << (max(levels, reserved + levels - rows, 1) - 1).bit_length()
    # A validity bit per slot when the leaf can be null.
    validity = nullable * slots // 8
    if chunk.physical_type == "FIXED_LEN_BYTE_ARRAY":
        # The fixed-size binary builder's values, and from a dictionary its
        # values again as it grows.
        values, zero_copy = slots * column.length, False
        temporary += chunk.has_dictionary_page * values
    else:
        values = slots * _PHYSICAL_WIDTH[chunk.physical_type]
        zero_copy = _zero_copy(physical_type=chunk.physical_type, arrow_type=leaf.type)
    if zero_copy:
        # Values at capacity until PyArrow trims them and hands them over: the
        # decoded size counts what it keeps, the rest is held only while decoding.
        temporary += max(values - decoded, 0) + validity
    else:
        # PyArrow converts the values into a new array, and the decode buffer
        # measured as staying until the row group is read.
        kept += values + validity
    # A fixed-width list's offsets are built after its values, so only the
    # larger of the two is held at once.
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
    list, map or view cast copies once per one of the ``pieces`` the scanner
    yields the row group in, and 1.5 covers the casts to text it undercounts.
    """
    size = estimate_added_columns(row_group=row_group, read=read)
    for name, leaves in column_leaves.items():
        before, after = file_schema.field(name).type, pinned_schema.field(name).type
        if before == after:
            # A column the pin leaves as it is isn't cast.
            continue
        # A nested or view cast copies per piece, any other cast once.
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
        # The footer reader fetched the header, which has the exact size.
        return leaf.dictionary_page.uncompressed_page_size
    # The bytes before the first data page, scaled by the chunk's ratio and
    # never below them, since a page that doesn't compress is stored as is.
    gap = chunk.data_page_offset - (chunk.dictionary_page_offset or 0)
    if 0 < gap <= chunk.total_compressed_size:
        ratio = chunk.total_uncompressed_size / max(chunk.total_compressed_size, 1)
        return int(gap * max(1.0, ratio))
    # Offsets that don't bound the page: the chunk's bytes, at most 2 MiB.
    return min(chunk.total_uncompressed_size, _DICTIONARY_BYTES)


def _list_offset_width(leaf: LeafChunk) -> int:
    """Bytes per offset of the widest list above ``leaf``, 4 if none."""
    # A list's offsets are int32, a large_list's int64.
    return max(
        (node.width for node in leaf.profile.nodes[:-1] if node.kind == _LIST),
        default=4,
    )


def _hash_table_bytes(distinct: int) -> int:
    """A memo table's 16-byte slots: from 32, quadrupling while half full."""
    # As Arrow's hashing.h: a 64-bit hash and an int32 index pad to 16 bytes.
    slots = 32
    while slots <= 2 * distinct:
        slots *= 4
    return 16 * slots


def _reads_dictionary(leaf: LeafChunk, read: Read) -> bool:
    """Whether PyArrow reads ``leaf`` as a dictionary."""
    return detect_dictionary_columns(
        chunk=leaf.chunk, format_kwargs=read.format_kwargs
    ) or detect_dictionary_type(arrow_type=leaf.type)


def _zero_copy(physical_type: str, arrow_type: pa.DataType) -> bool:
    """Whether PyArrow hands the decoded values to Arrow without a copy."""
    # PyArrow reuses the buffer only for these; it copies into every other
    # type, even one as wide as its physical type, such as date32.
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
        # An extension type casts as its storage.
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        # A struct copies per piece only through a child that does.
        return any(_nested_or_view(field.type) for field in arrow_type)
    return (
        pa.types.is_nested(arrow_type)
        or pa.types.is_string_view(arrow_type)
        or pa.types.is_binary_view(arrow_type)
    )
