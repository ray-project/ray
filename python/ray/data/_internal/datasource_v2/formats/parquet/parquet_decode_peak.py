"""Estimate PyArrow's peak memory decoding a Parquet row group, from its footer.

The peak is the I/O (the footer read, ``pre_buffer``'s gaps and the compressed
chunks), every column's decoded values, the buffers of the columns decoding at once,
and a schema pin's cast. Each CPU thread decodes one column at a time. A leaf's
buffers are a sum of each one at its largest, so the estimate leans high; it never
came under the peaks PyArrow's pool measured, and carries a flat safety factor.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType

import pyarrow as pa
from pyarrow.parquet import ColumnChunkMetaData, ColumnSchema, RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.decoded_size import (
    LeafChunk,
    Read,
    estimate_ancestors,
    estimate_leaf,
    estimate_row_group,
)

# PyArrow reads a file's last 64 KiB for its footer, and pre_buffer coalesces
# chunks across gaps of up to 8 KiB; one gap measured short, two cover it.
_FOOTER_BYTES = 80 << 10
# pre_buffer also reads what lies between two chunks it wants when that is 8 KiB or
# less: the chunks of leaves the read skips.
_HOLE_BYTES = 8 << 10
# Below this the footer read took the whole file; 64 KiB put one row group under.
_SMALL_FILE_BYTES = 48 << 10
# Twice the 1 MiB dictionary page limit parquet-cpp and arrow-rs default to,
# for a page the footer can't size.
_DICTIONARY_BYTES = 2 << 20
# Casts to text measured above the cast estimate; 1.5 covers all but a few, and
# 3 covered more but raised the worst overestimate by a fifth to a third.
_CAST_FACTOR = 1.5
# A safety margin on the whole estimate, for what the process holds past PyArrow's
# pool. Telemetry decides whether it stays.
_SAFETY_FACTOR = 1.1
# Bytes per value as PyArrow decodes each fixed-width physical type.
_PHYSICAL_WIDTH = MappingProxyType(
    dict(BOOLEAN=1, INT32=4, INT64=8, INT96=12, FLOAT=4, DOUBLE=8)
)
# Encodings whose decoders unpack a page's value lengths up front.
_DELTA_ENCODINGS = frozenset({"DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY"})
# Encodings of the data pages of a chunk that has a dictionary page.
_DICTIONARY_ENCODINGS = frozenset({"PLAIN_DICTIONARY", "RLE_DICTIONARY"})
# From PyArrow 26 a column's leaves are reserved for the row group's rows. Before,
# the k-th column read was reserved for as many slots as the file's k-th *leaf* has
# values, whichever column that leaf is in.
_RESERVES_ROWS = pa.cpp_version_info.major >= 26


def estimate_row_group_peak(
    row_group: RowGroupMetaData,
    leaf_indices: Sequence[int],
    row_group_work: int,
    file_size: int,
) -> int:
    """Peak bytes reading ``leaf_indices`` of ``row_group``, given
    ``row_group_work``: its decoded bytes, cast included, and its buffers."""
    if file_size < _SMALL_FILE_BYTES:
        # The footer read took the whole file. 512 bytes and 256 a leaf are the
        # least slack that covered the measured peaks.
        read = file_size + 512 + 256 * len(leaf_indices)
    else:
        compressed = sum(
            row_group.column(leaf_idx).total_compressed_size
            for leaf_idx in leaf_indices
        )
        # A hole holds a skipped leaf and sits before a leaf read: no more of them
        # than of either.
        skipped = row_group.num_columns - len(leaf_indices)
        holes = _HOLE_BYTES * max(min(skipped, len(leaf_indices)), 0)
        read = _FOOTER_BYTES + compressed + holes
    return int(_SAFETY_FACTOR * (read + row_group_work))


def estimate_row_group_work(
    row_group: RowGroupMetaData,
    columns: Sequence[ColumnSchema],
    column_leaves: Sequence[Sequence[int]],
    leaves: Mapping[int, LeafChunk],
    decode_threads: int,
) -> tuple[int, int]:
    """One row group's decoded columns, and the buffers of the ``decode_threads``
    largest columns, or of every column if it is 0 or less. ``columns`` holds every
    leaf of the file's schema, by leaf index. ``column_leaves`` holds each top-level
    column read as its leaf indices, in the order PyArrow reads them: the filter's
    columns, then the projection's others. ``leaves`` holds the unpinned leaves
    modelled."""
    decoded, buffers = 0, []
    # The ancestors sized, which a column's leaves share.
    counted: set[int] = set()
    # A column the file lacks isn't read, and takes no position.
    for position, leaf_indices in enumerate(filter(None, column_leaves)):
        if _RESERVES_ROWS:
            reserved = row_group.num_rows
        else:
            reserved = row_group.column(position).num_values
        column_buffers = 0
        for leaf_idx in leaf_indices:
            # A leaf the estimator doesn't model is charged by the caller.
            if (leaf := leaves.get(leaf_idx)) is None:
                continue
            decoded += estimate_ancestors(leaf=leaf, counted=counted)
            # Sized here and not by estimate_row_group: the buffers need each
            # leaf's decoded bytes too.
            leaf_decoded = estimate_leaf(leaf=leaf)
            decoded += leaf_decoded
            column_buffers += estimate_leaf_buffers(
                leaf=leaf,
                column=columns[leaf_idx],
                decoded=leaf_decoded,
                reserved=reserved,
            )
        buffers.append(column_buffers)
    # Each CPU thread decodes one column at a time, and a column's buffers are
    # freed once it is read.
    if decode_threads > 0:
        buffers = sorted(buffers)[-decode_threads:]
    return decoded, sum(buffers)


def estimate_leaf_buffers(
    leaf: LeafChunk, column: ColumnSchema, decoded: int, reserved: int
) -> int:
    """The buffers PyArrow holds decoding one leaf, past its ``decoded`` bytes: a
    sum of each buffer at its largest. ``reserved`` is the slots PyArrow reserves."""
    chunk = leaf.chunk
    encodings = set(chunk.encodings)
    repeated = column.max_repetition_level > 0
    nullable = column.max_definition_level > 0
    # Buffers start at the slots reserved and grow by doubling: at most twice the
    # larger of those and the leaf's values.
    slots = 2 * max(chunk.num_values, reserved)
    # int16 definition and repetition levels.
    buffers = 2 * slots * (nullable + repeated)
    if chunk.compression != "UNCOMPRESSED":
        # The buffer pages decompress into, at most the chunk.
        buffers += chunk.total_uncompressed_size
    if repeated:
        # A list's offsets, for every slot reserved.
        buffers += leaf.profile.list_offset_width * reserved
    dictionary = _dictionary_bytes(chunk=chunk, encodings=encodings)
    if chunk.physical_type == "BYTE_ARRAY":
        # The builder's copy of the values and its 4-byte offsets. The dictionary
        # page, and parquet-cpp's 16-byte length and pointer per entry: an entry
        # takes at least 4 bytes of the page, so at most 4 times the page.
        buffers += decoded + 4 * slots + 5 * dictionary
    else:
        if chunk.physical_type == "FIXED_LEN_BYTE_ARRAY":
            values = column.length * slots
        else:
            values = _PHYSICAL_WIDTH[chunk.physical_type] * slots
            if leaf.profile.nodes[-1].keeps_bitmap:
                # Handed over as the array, not converted: the decoded values
                # are part of this buffer.
                values = max(values - decoded, 0)
        buffers += values + dictionary
    if _DELTA_ENCODINGS & encodings:
        # DELTA decoders unpack a page's int32 lengths, and DELTA_BYTE_ARRAY's
        # its prefix lengths and rebuilt values too.
        buffers += 4 * chunk.num_values
        if "DELTA_BYTE_ARRAY" in encodings:
            buffers += 4 * chunk.num_values + decoded
    return buffers


def estimate_cast(
    row_group: RowGroupMetaData,
    cast_leaves: Mapping[str, Mapping[int, LeafChunk]],
    file_schema: pa.Schema,
    pinned_schema: pa.Schema,
) -> int:
    """What a pin's cast allocates for one row group, times 1.5 as a crude bound.
    ``cast_leaves`` holds each column read as its leaves by index, profiled as
    pinned."""
    cast = 0
    for name, leaves in cast_leaves.items():
        stored, pinned = file_schema.field(name).type, pinned_schema.field(name).type
        if stored == pinned or _cast_reuses_buffers(stored=stored, pinned=pinned):
            continue
        cast += estimate_row_group(
            row_group=row_group, leaves=leaves.values(), read=Read()
        )
    return int(_CAST_FACTOR * cast)


def _cast_reuses_buffers(stored: pa.DataType, pinned: pa.DataType) -> bool:
    """Whether a cast allocates nothing, as measured on PyArrow 23 and 26: a
    timestamp to its unit in another timezone, and a signed integer to or from a
    temporal type of its width (int32 to date32, a timestamp to int64)."""
    if pa.types.is_timestamp(stored) and pa.types.is_timestamp(pinned):
        return stored.unit == pinned.unit
    for integer, temporal in ((stored, pinned), (pinned, stored)):
        if pa.types.is_signed_integer(integer) and pa.types.is_temporal(temporal):
            return integer.bit_width == temporal.bit_width
    return False


def _dictionary_bytes(chunk: ColumnChunkMetaData, encodings: set[str]) -> int:
    """The dictionary page's uncompressed bytes, at most, from its gap."""
    if chunk.has_dictionary_page:
        gap = chunk.data_page_offset - (chunk.dictionary_page_offset or 0)
        if 0 < gap <= chunk.total_compressed_size:
            saved = chunk.total_uncompressed_size - chunk.total_compressed_size
            ratio = chunk.total_uncompressed_size / max(chunk.total_compressed_size, 1)
            # The page saved no more by compressing than its chunk did. Past 2 MiB
            # that is a chunk whose dictionary overflowed, and whose PLAIN pages
            # saved the rest: the page at the chunk's ratio.
            scaled = max(int(gap * max(1.0, ratio)), _DICTIONARY_BYTES)
            return min(gap + max(saved, 0), scaled)
    elif not _DICTIONARY_ENCODINGS & encodings:
        return 0
    # Offsets that don't bound the page, or a writer that left the dictionary's
    # unset: the chunk's bytes, at most 2 MiB.
    return min(chunk.total_uncompressed_size, _DICTIONARY_BYTES)
