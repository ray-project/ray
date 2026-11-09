"""Estimate PyArrow 24's peak memory decoding a Parquet row group, from its footer.

The peak is the I/O (the footer read, ``pre_buffer``'s gaps and the compressed
chunks), every column's decoded values, the buffers of the columns decoding at once
(levels, values, dictionary, decompression buffer, temporaries), and a schema pin's
cast. Each CPU thread decodes one column at a time, and a column's buffers are freed
when it is read. The rules are fitted to measured peaks and lean high.
"""

from typing import Mapping, Sequence, Set, Tuple

import pyarrow as pa
from pyarrow.parquet import ColumnSchema, RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    _BINARY,
    _LIST,
    LeafChunk,
    Read,
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
# Twice the 1 MiB dictionary page limit parquet-cpp and arrow-rs default to,
# for a page the footer can't size.
_DICTIONARY_BYTES = 2 << 20
# Casts to text measured above the cast estimate; 1.5 covers all but a few, and
# 3 covered more but raised the worst overestimate by a fifth to a third.
_CAST_FACTOR = 1.5
# Bytes per value as PyArrow decodes each fixed-width physical type.
_PHYSICAL_WIDTH = dict(BOOLEAN=1, INT32=4, INT64=8, INT96=12, FLOAT=4, DOUBLE=8)
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
    leaves: Mapping[int, LeafChunk],
    counted: Set[int],
    decode_threads: int,
) -> int:
    """One row group's decoded columns, plus the buffers of the ``decode_threads``
    largest columns, or of every column if it is 0 or less: ``column_leaves`` holds
    each column read, in ``columns=`` order, as its leaf indices; ``leaves`` the
    unpinned leaves modelled; ``counted`` the ancestors sized."""
    decoded, buffers = 0, []
    for position, leaf_indices in enumerate(column_leaves):
        # PyArrow 24 reserves the k-th column's leaves for as many slots as the
        # file's k-th *leaf* has values, whichever column that leaf is in.
        reserved = row_group.column(position).num_values
        # A column's leaves decode in turn: its peak is one leaf's temporaries
        # over what that leaf and the ones before it keep. Once the column is
        # read, only its decoded values stay.
        column_kept = column_peak = column_decoded = 0
        for leaf_idx in leaf_indices:
            # A leaf the estimator doesn't model is charged by the caller.
            if (leaf := leaves.get(leaf_idx)) is None:
                continue
            ancestors = estimate_ancestors(leaf=leaf, counted=counted)
            leaf_kept, leaf_temporary = estimate_leaf_work(
                leaf=leaf,
                column=columns[leaf_idx],
                reserved=reserved,
                rows=row_group.num_rows,
                ancestors=ancestors,
            )
            column_kept += ancestors + leaf_kept
            column_peak = max(column_peak, column_kept + leaf_temporary)
            column_decoded += ancestors + estimate_leaf(leaf=leaf)
        decoded += column_decoded
        buffers.append(column_peak - column_decoded)
    # Each CPU thread decodes one column at a time.
    if decode_threads > 0:
        buffers = sorted(buffers)[-decode_threads:]
    return decoded + sum(buffers)


def estimate_leaf_work(
    leaf: LeafChunk,
    column: ColumnSchema,
    reserved: int,
    rows: int,
    ancestors: int = 0,
) -> Tuple[int, int]:
    """One leaf's buffers: (kept until its column is read, temporary)."""
    chunk = leaf.chunk
    levels = chunk.num_values
    # Buffers start at ``reserved`` slots and double to a power of two.
    capacity = 1 << (max(levels, reserved, 1) - 1).bit_length()
    repeated = column.max_repetition_level > 0
    nullable = column.max_definition_level > 0
    # A repeated leaf's buffers measured up to one doubling past ``capacity``.
    grows = 2 if repeated else 1
    decoded = estimate_leaf(leaf=leaf)
    dictionary = _dictionary_bytes(leaf)
    # int16 definition and repetition levels, and the decoded leaf.
    kept = 2 * capacity * (nullable + repeated) + decoded
    temporary = 0
    reader_bytes = 0
    if chunk.compression != "UNCOMPRESSED":
        # Pages decompress into a buffer the size of the largest page, bounded by
        # the chunk, or of the dictionary page.
        reader_bytes = max(chunk.total_uncompressed_size, dictionary)
    if _DELTA_ENCODINGS & set(chunk.encodings):
        # DELTA decoders unpack a page's int32 lengths, 4 bytes a value, and
        # DELTA_BYTE_ARRAY's its prefix lengths and rebuilt values too.
        reader_bytes += 4 * levels
        if "DELTA_BYTE_ARRAY" in chunk.encodings:
            reader_bytes += 4 * levels + decoded
    if reserved > rows:
        # Reserved past the rows (a list's position), the page reader measured
        # as held only while the leaf decodes.
        temporary += reader_bytes
    else:
        kept += reader_bytes
    # A list's offsets for every slot reserved, past what its ancestors keep.
    list_offsets = repeated * max(_list_offset_width(leaf) * reserved - ancestors, 0)
    if chunk.physical_type == "BYTE_ARRAY":
        # Each dictionary entry is at least its 4-byte length.
        entries = dictionary // 4
        # The dictionary, and parquet-cpp's 16-byte length and pointer per entry.
        kept += dictionary + 16 * entries
        # Offsets reserved past the slots used: 4 bytes, 8 for a large string, 4
        # for a decimal, which BYTE_ARRAY also carries.
        node = leaf.profile.nodes[-1]
        width = node.width if node.kind == _BINARY else 4
        temporary += width * max(capacity - leaf.slots(), 0)
        # The builder's copy of the values; a decimal without SizeStatistics
        # bytes at most its width each.
        if node.kind == _BINARY or leaf.size_statistics_bytes is not None:
            temporary += estimate_string_bytes(leaf=leaf)
        else:
            temporary += node.width * leaf.slots()
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
        zero_copy = leaf.profile.nodes[-1].keeps_bitmap
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
    read: Read,
) -> int:
    """What a pin's cast allocates for one row group, times 1.5: a crude bound.
    ``column_leaves`` holds each column read as its leaves, profiled as pinned."""
    size = estimate_added_columns(row_group=row_group, read=read)
    for name, leaves in column_leaves.items():
        before, after = file_schema.field(name).type, pinned_schema.field(name).type
        if before == after:
            continue
        size += estimate_row_group(
            row_group=row_group,
            leaves=leaves,
            read=read._replace(added_row_bytes=0, added_row_group_bytes=0),
        )
    return int(_CAST_FACTOR * size)


def _dictionary_bytes(leaf: LeafChunk) -> int:
    """The dictionary page's uncompressed bytes, from its gap."""
    chunk = leaf.chunk
    if not chunk.has_dictionary_page:
        return 0
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
    widths = [node.width for node in leaf.profile.nodes[:-1] if node.kind == _LIST]
    return max(widths, default=4)
