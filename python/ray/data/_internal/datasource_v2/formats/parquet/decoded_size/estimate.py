"""The decoded bytes of a row group, from its leaves' profiles and counts."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import pyarrow as pa
from pyarrow.parquet import FileMetaData, ParquetSchema, RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.decoded_size.arrow_layout import (
    _BINARY,
    _BOOLEAN,
    _FIXED_WIDTH,
    _LIST,
    _NULL,
    _UNSUPPORTED,
    _ZERO_COPY_PHYSICAL_TYPES,
    _bytes_for_bits,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.schema_walk import (
    LeafProfile,
    _ArrowNode,
    _matches,
    _walk,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.schema_pin import (
    _cast_keeps_bitmap,
    _pinned_walk,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.footer_counts import (
    LeafChunk,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.added_columns import (
    Read,
    estimate_added_columns,
)


def build_leaf_profiles(
    parquet_schema: ParquetSchema,
    *,
    pinned_schema: pa.Schema | None = None,
    format_kwargs: Mapping[str, Any] | None = None,
    file_metadata: FileMetaData | None = None,
) -> list[LeafProfile | None] | None:
    """One :class:`LeafProfile` per leaf, in row group column order, ``None`` for a
    leaf the estimator doesn't model.

    Args:
        parquet_schema: The file's schema.
        pinned_schema: The schema the read casts every batch to, if any: the
            reader's ``FileReader._file_dataset_schema``, not the dataset schema.
        format_kwargs: The ``ParquetFileFormat`` kwargs the read task passes.
        file_metadata: The file's footer, for its writer.

    Returns:
        The profiles, or ``None`` for a file the estimator doesn't model at all.
    """
    format_kwargs = format_kwargs or {}
    # Rare reads. ``binary_type`` and ``list_type`` retype every such column, which
    # leaves the file unmodelled; ``dictionary_columns``, only the BYTE_ARRAY
    # leaves it names, by column path.
    if detect_binary_or_list_type(format_kwargs):
        return None
    dictionary_columns = frozenset(format_kwargs.get("dictionary_columns") or ())
    try:
        arrow_schema = parquet_schema.to_arrow_schema()
        file_walk = walk = _walk(arrow_schema)
        if len(walk.leaves) != len(parquet_schema):
            return None
        unmapped: set[int] = set()
        if pinned_schema is not None:
            walk, unmapped = _pinned_walk(
                file_walk=file_walk,
                arrow_schema=arrow_schema,
                pinned_schema=pinned_schema,
                parquet_schema=parquet_schema,
            )
    except (pa.ArrowException, ValueError, TypeError):
        # A schema the walk can't handle falls back rather than failing the read.
        return None
    writer = "" if file_metadata is None else file_metadata.created_by or ""
    # Only parquet-java and arrow-rs are known to count every DELTA_BYTE_ARRAY
    # value's bytes. Arrow C++ leaves out each value equal to, or a prefix of, the
    # one before: 1,000 copies of an 8-byte string report 8 bytes, where arrow-rs
    # reports 8,000.
    counts_delta = writer.startswith(("parquet-mr", "parquet-rs"))

    profiles: list[LeafProfile | None] = []
    for leaf_idx, (chain, max_repetition_level) in enumerate(walk.leaves):
        column = parquet_schema.column(leaf_idx)
        leaf = chain[-1]
        if (
            leaf_idx in unmapped
            or (
                column.path in dictionary_columns
                and column.physical_type == "BYTE_ARRAY"
            )
            or leaf.kind == _UNSUPPORTED
            or not _matches(leaf=leaf, rep=max_repetition_level, column=column)
        ):
            profiles.append(None)
            continue
        # The decode keeps a null-free bitmap, and the pin's cast, if any, keeps it.
        keeps_bitmap = (
            file_walk.leaves[leaf_idx][0][-1].keeps_bitmap
            and column.physical_type in _ZERO_COPY_PHYSICAL_TYPES
            and _cast_keeps_bitmap(
                stored=file_walk.leaf_types[leaf_idx], pinned=walk.leaf_types[leaf_idx]
            )
        )
        if leaf.keeps_bitmap != keeps_bitmap:
            leaf = leaf._replace(keeps_bitmap=keeps_bitmap)
        profiles.append(
            LeafProfile(
                nodes=chain[:-1] + (leaf,),
                max_definition_level=column.max_definition_level,
                max_repetition_level=max_repetition_level,
                counts_delta_byte_array=counts_delta,
            )
        )
    return profiles


def estimate_row_group(
    row_group: RowGroupMetaData, leaves: Iterable[LeafChunk], read: Read
) -> int:
    """Decoded bytes of ``leaves``, chunks of ``row_group``."""
    size = 0
    counted: set[int] = set()
    for leaf in leaves:
        size += estimate_ancestors(leaf=leaf, counted=counted)
        size += estimate_leaf(leaf)
    return size + estimate_added_columns(row_group=row_group, read=read)


def estimate_leaf(leaf: LeafChunk) -> int:
    """Decoded bytes of one chunk's leaf array."""
    if leaf.profile.nodes[-1].kind == _BINARY:
        string_bytes = estimate_string_bytes(leaf)
        return estimate_string_type(leaf=leaf, string_bytes=string_bytes)
    return estimate_fixed_width(leaf)


# Fixed-width, boolean and null leaves, and the arrays above every leaf.
def estimate_fixed_width(leaf: LeafChunk) -> int:
    """Decoded bytes of a fixed-width, boolean or null leaf."""
    return _array_bytes(node=leaf.profile.nodes[-1], leaf=leaf)


def estimate_ancestors(leaf: LeafChunk, counted: set[int]) -> int:
    """The arrays above ``leaf`` not yet in ``counted``; adds them to it."""
    size = 0
    for node in leaf.profile.nodes[:-1]:
        if node.node_id not in counted:
            counted.add(node.node_id)
            size += _array_bytes(node=node, leaf=leaf)
    return size


def _array_bytes(node: _ArrowNode, leaf: LeafChunk) -> int:
    """A validity bitmap, and a list's offsets or a fixed-width leaf's values."""
    if node.kind == _NULL:
        return 0
    length = leaf.array_length(node)
    size = leaf.bitmap(node)
    if node.kind == _LIST:
        size += node.width * (length + 1)
    elif node.kind == _FIXED_WIDTH:
        size += node.width * length
    elif node.kind == _BOOLEAN:
        size += _bytes_for_bits(length)
    return size


# The ``binary_type`` / ``list_type`` read options, for wider strings or lists.
def detect_binary_or_list_type(format_kwargs: Mapping[str, Any]) -> bool:
    large_list = format_kwargs.get("list_type") not in (None, pa.ListType)
    return format_kwargs.get("binary_type") not in (None, pa.binary()) or large_list


def estimate_string_type(leaf: LeafChunk, string_bytes: int) -> int:
    """An offset per element, plus one, and the values' bytes."""
    offsets = leaf.profile.nodes[-1].width * (leaf.array_length() + 1)
    return leaf.bitmap() + offsets + string_bytes


# Where a string leaf's bytes come from.
def estimate_string_bytes(leaf: LeafChunk) -> int:
    """A BYTE_ARRAY chunk's value bytes, without their length prefixes."""
    # ``LeafChunk.modelled`` keeps only string leaves with these bytes.
    assert leaf.string_bytes is not None
    return leaf.string_bytes
