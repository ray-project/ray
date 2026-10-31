"""Estimate PyArrow's peak memory decoding a Parquet row group, from its footer.

A row group's peak is the footer read and its compressed chunks, what each leaf
keeps until the scan ends, the two largest leaves' decode-time buffers (columns
decode in parallel), and a pin's cast. Modelled on PyArrow 24's reader.
"""

from typing import List, Mapping, Optional, Sequence, Set, Tuple

import pyarrow as pa
from pyarrow.parquet import ColumnSchema, FileMetaData, RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    _LIST,
    LeafChunk,
    Read,
    detect_binary_or_list_type,
    detect_dictionary_columns,
    detect_dictionary_type,
    estimate_added_columns,
    estimate_ancestors,
    estimate_leaf,
    estimate_row_group,
    estimate_string_bytes,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    _COLUMN_CHUNK_META_DATA,
    _FILE_META_ROW_GROUPS,
    _I32,
    _LIST as _THRIFT_LIST,
    _ROW_GROUP_COLUMNS,
    _STRUCT,
    _CompactReader,
    _ThriftDesync,
    footer_bytes,
)

# A scan without the footer cached reads the file's last 64 KiB in one buffer.
_FOOTER_BYTES = 64 << 10
# pre_buffer merges column chunks less than this far apart into one read
# (``CacheOptions.hole_size_limit``), so it may hold a gap this big per chunk.
_HOLE_BYTES = 8 << 10
# A dictionary page the footer can't size is taken as at most this big.
_DICTIONARY_BYTES = 2 << 20
# The cast estimate runs low for casts to text; this covers what was measured.
_CAST_FACTOR = 1.5
_PHYSICAL_WIDTH = {
    "BOOLEAN": 1,
    "INT32": 4,
    "INT64": 8,
    "INT96": 12,
    "FLOAT": 4,
    "DOUBLE": 8,
}
_DELTA_ENCODINGS = {"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY"}
# parquet.thrift: ColumnMetaData.encoding_stats, PageEncodingStats.page_type
# and .count, PageType DATA_PAGE and DATA_PAGE_V2.
_ENCODING_STATS, _STATS_PAGE_TYPE, _STATS_COUNT = 13, 1, 3
_DATA_PAGES = (0, 3)


def estimate_row_group_peak(
    row_group: RowGroupMetaData,
    leaf_indices: Sequence[int],
    row_group_work: int,
) -> int:
    """PyArrow's peak bytes reading ``row_group``, given its work and cast."""
    held = _FOOTER_BYTES + sum(
        row_group.column(leaf_idx).total_compressed_size + _HOLE_BYTES
        for leaf_idx in leaf_indices
    )
    return held + row_group_work


def estimate_row_group_work(
    row_group: RowGroupMetaData,
    columns: Sequence[ColumnSchema],
    column_leaves: Sequence[Sequence[int]],
    read: Read,
    leaves: Mapping[int, LeafChunk],
    counted: Set[int],
    data_pages: Optional[Sequence[Optional[int]]] = None,
) -> int:
    """Bytes PyArrow allocates decoding one row group's leaves.

    Args:
        row_group: The row group.
        columns: The file's Parquet schema columns.
        column_leaves: Each top-level column read, in ``columns=`` order: its leaves.
        read: The read options.
        leaves: The file's unpinned leaves by index; PyArrow casts after decoding.
        counted: Ancestors already sized.
        data_pages: This row group's :func:`read_data_pages`.

    Returns:
        What the leaves keep, plus the two largest leaves' decode-time buffers.
    """
    kept, temporaries = 0, [0, 0]
    for position, leaf_indices in enumerate(column_leaves):
        # PyArrow 24 reserves the k-th column's leaves for as many slots as the
        # file's k-th *leaf* has values, whichever column that leaf is in.
        reserved = row_group.column(position).num_values
        for leaf_idx in leaf_indices:
            leaf = leaves[leaf_idx]
            leaf_kept, leaf_temporary = estimate_leaf_work(
                leaf=leaf,
                column=columns[leaf_idx],
                reserved=reserved,
                read=read,
                pages=data_pages[leaf_idx] if data_pages else None,
            )
            kept += estimate_ancestors(leaf=leaf, counted=counted) + leaf_kept
            temporaries.append(leaf_temporary)
    # Columns decode in parallel, so two leaves' temporaries can overlap.
    temporaries.sort()
    return kept + temporaries[-1] + temporaries[-2]


def estimate_leaf_work(
    leaf: LeafChunk,
    column: ColumnSchema,
    reserved: int,
    read: Read,
    pages: Optional[int] = None,
) -> Tuple[int, int]:
    """One leaf's buffers: (kept until the scan ends, held while it decodes)."""
    chunk = leaf.chunk
    levels = chunk.num_values
    # Buffers start at ``reserved`` slots and double as the levels fill them.
    capacity = 1 << max(max(levels, reserved) - 1, 0).bit_length()
    repeated = column.max_repetition_level > 0
    nullable = column.max_definition_level > 0
    grows = 2 if repeated else 1
    decoded = estimate_leaf(leaf=leaf, read=read)
    dictionary = _dictionary_bytes(leaf)
    # 2-byte definition and repetition levels per slot, and the decoded leaf.
    kept = 2 * capacity * (nullable + repeated) + decoded
    temporary = 0
    # The page reader's decompression buffer: the largest page (at most twice
    # the average, the whole chunk if the footer has no page count) or the
    # dictionary page.
    reader_bytes = 0
    if chunk.compression != "UNCOMPRESSED":
        uncompressed = chunk.total_uncompressed_size
        page = (
            uncompressed
            if pages is None
            else min(uncompressed, 2 * uncompressed // max(pages, 1))
        )
        reader_bytes = max(page, dictionary)
    if _DELTA_ENCODINGS & set(chunk.encodings):
        # A DELTA decoder: two lengths per value and its page's rebuilt values.
        reader_bytes += (8 * levels + decoded) // max(pages or 1, 1)
    # A leaf that doesn't repeat keeps its page reader until the scan ends.
    if repeated:
        # List offsets, built for every slot reserved, then trimmed.
        list_offsets = _list_offset_width(leaf) * max(levels, reserved)
        temporary += list_offsets + reader_bytes
    else:
        kept += reader_bytes
    if detect_binary_or_list_type(format_kwargs=read.format_kwargs):
        # The copy at the retyped type.
        temporary += decoded
    if chunk.physical_type == "BYTE_ARRAY":
        if chunk.has_dictionary_page:
            # The decoded dictionary: its bytes and 16 bytes per entry, each
            # entry at least 4 bytes on the page.
            entries = (
                leaf.dictionary_page.num_values
                if leaf.dictionary_page is not None
                else dictionary // 4
            )
            kept += dictionary + 16 * entries
        slots = leaf.slots()
        if pa.types.is_string_view(leaf.type) or pa.types.is_binary_view(leaf.type):
            width = 16
        elif pa.types.is_large_string(leaf.type) or pa.types.is_large_binary(leaf.type):
            width = 8
        else:
            width = 4
        # Offsets (or views) reserved past the slots used.
        temporary += width * max(grows * capacity - slots, 0)
        chars = estimate_string_bytes(leaf=leaf)
        if _reads_dictionary(leaf=leaf, read=read):
            # The dictionary builder: hash table, values doubled as they grow,
            # int32 indices. Each distinct value is at least 4 decoded bytes.
            distinct = min(slots, max(decoded - slots, 0) // 4 + 1)
            builder_values = 2 * min(chars, decoded)
            temporary += _hash_table_bytes(distinct) + builder_values + 4 * slots
        else:
            # Value bytes doubled from a page's worth: up to twice their size.
            temporary += chars
        return kept, temporary
    kept += dictionary
    validity = nullable * grows * capacity // 8
    if chunk.physical_type == "FIXED_LEN_BYTE_ARRAY":
        values, zero_copy = grows * capacity * column.length, False
    else:
        values = grows * capacity * _PHYSICAL_WIDTH[chunk.physical_type]
        zero_copy = _zero_copy(physical_type=chunk.physical_type, arrow_type=leaf.type)
    if zero_copy:
        # Values at capacity until PyArrow trims them and hands them over.
        return kept, temporary + max(values - decoded, 0) + validity
    return kept + values + validity, temporary


def estimate_cast(
    row_group: RowGroupMetaData,
    column_leaves: Mapping[str, Sequence[LeafChunk]],
    file_schema: pa.Schema,
    pinned_schema: pa.Schema,
    pieces: int,
    read: Read,
) -> int:
    """What a pin's cast allocates for one row group: a crude bound.

    Args:
        row_group: The row group.
        column_leaves: Each column read: its leaves, profiled as pinned.
        file_schema: The file's Arrow schema.
        pinned_schema: The schema the pin casts to.
        pieces: How many pieces the scanner yields the row group in; a list,
            map or view cast copies once per piece.
        read: The read options, with the columns the pin adds.

    Returns:
        The cast's bytes, times 1.5 for the casts to text it undercounts.
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


def read_data_pages(metadata: FileMetaData) -> Optional[List[List[Optional[int]]]]:
    """Each chunk's data page count from ``encoding_stats``, by row group then leaf.

    ``None`` for a chunk without the stats (polars writes none), and for the
    whole file if the footer doesn't parse.
    """
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    reader = _CompactReader(buf)
    row_groups: List[List[Optional[int]]] = []
    try:
        for field_id, field_type in reader._fields():
            if field_id == _FILE_META_ROW_GROUPS and field_type == _THRIFT_LIST:
                size, _ = reader._list_header()
                row_groups = [_row_group_pages(reader) for _ in range(size)]
            else:
                reader._skip(field_type)
    except (IndexError, _ThriftDesync):
        return None
    if len(row_groups) != metadata.num_row_groups or any(
        len(chunks) != metadata.num_columns for chunks in row_groups
    ):
        return None
    return row_groups


def _row_group_pages(reader: _CompactReader) -> List[Optional[int]]:
    chunks: List[Optional[int]] = []
    for field_id, field_type in reader._fields():
        if field_id == _ROW_GROUP_COLUMNS and field_type == _THRIFT_LIST:
            size, _ = reader._list_header()
            chunks = [_chunk_pages(reader) for _ in range(size)]
        else:
            reader._skip(field_type)
    return chunks


def _chunk_pages(reader: _CompactReader) -> Optional[int]:
    pages = None
    for field_id, field_type in reader._fields():
        if field_id == _COLUMN_CHUNK_META_DATA and field_type == _STRUCT:
            for meta_id, meta_type in reader._fields():
                if meta_id == _ENCODING_STATS and meta_type == _THRIFT_LIST:
                    pages = _encoding_stats_pages(reader)
                else:
                    reader._skip(meta_type)
        else:
            reader._skip(field_type)
    return pages


def _encoding_stats_pages(reader: _CompactReader) -> int:
    """The data pages an ``encoding_stats`` list counts."""
    size, _ = reader._list_header()
    pages = 0
    for _ in range(size):
        page_type, count = None, 0
        for field_id, field_type in reader._fields():
            if field_id == _STATS_PAGE_TYPE and field_type == _I32:
                page_type = reader._zigzag()
            elif field_id == _STATS_COUNT and field_type == _I32:
                count = reader._zigzag()
            else:
                reader._skip(field_type)
        if page_type in _DATA_PAGES:
            pages += count
    return pages


def _dictionary_bytes(leaf: LeafChunk) -> int:
    """The dictionary page's uncompressed bytes: from its header, else its gap."""
    chunk = leaf.chunk
    if not chunk.has_dictionary_page:
        return 0
    if leaf.dictionary_page is not None:
        return leaf.dictionary_page.uncompressed_page_size
    # The bytes before the first data page, scaled by the chunk's ratio.
    gap = chunk.data_page_offset - (chunk.dictionary_page_offset or 0)
    if 0 < gap <= chunk.total_compressed_size:
        ratio = chunk.total_uncompressed_size / max(chunk.total_compressed_size, 1)
        return int(gap * max(1.0, ratio))
    return min(chunk.total_uncompressed_size, _DICTIONARY_BYTES)


def _list_offset_width(leaf: LeafChunk) -> int:
    return max(
        (node.width for node in leaf.profile.nodes[:-1] if node.kind == _LIST),
        default=4,
    )


def _hash_table_bytes(distinct: int) -> int:
    """A memo table's 16-byte slots: from 32, quadrupling while half full."""
    slots = 32
    while slots <= 2 * distinct:
        slots *= 4
    return 16 * slots


def _reads_dictionary(leaf: LeafChunk, read: Read) -> bool:
    return detect_dictionary_columns(
        chunk=leaf.chunk, format_kwargs=read.format_kwargs
    ) or detect_dictionary_type(arrow_type=leaf.type)


def _zero_copy(physical_type: str, arrow_type: pa.DataType) -> bool:
    if physical_type in ("INT32", "INT64"):
        return arrow_type in (pa.int32(), pa.int64()) or (
            physical_type == "INT64" and pa.types.is_timestamp(arrow_type)
        )
    return (physical_type, arrow_type) in (
        ("FLOAT", pa.float32()),
        ("DOUBLE", pa.float64()),
    )


def _nested_or_view(arrow_type: pa.DataType) -> bool:
    if isinstance(arrow_type, pa.ExtensionType):
        arrow_type = arrow_type.storage_type
    if (
        pa.types.is_string_view(arrow_type)
        or pa.types.is_binary_view(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_map(arrow_type)
    ):
        return True
    if pa.types.is_struct(arrow_type):
        return any(_nested_or_view(field.type) for field in arrow_type)
    return False
