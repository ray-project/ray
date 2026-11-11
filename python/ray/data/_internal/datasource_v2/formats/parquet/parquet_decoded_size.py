"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

The Arrow schema is walked into one chain of arrays per Parquet leaf: its structs
and lists, then the leaf. Each array is sized from the footer's counts as validity
bitmap plus offsets plus values, exactly. A leaf whose counts aren't exact, or whose
layout it doesn't model (views and dictionaries among them), is left to the caller.
The estimate is for the file's own schema; a read that casts the columns to
another schema allocates more, which the caller prices.
"""

from __future__ import annotations

from typing import Any, Iterable, NamedTuple, Sequence

import pyarrow as pa
from pyarrow.parquet import (
    ColumnChunkMetaData,
    ColumnSchema,
    FileMetaData,
    ParquetSchema,
    RowGroupMetaData,
)

from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
)

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


class _ArrowNode(NamedTuple):
    """An Arrow array a leaf decodes into: the leaf, or an ancestor it shares."""

    node_id: int
    kind: str
    # No bitmap when False, even under a null parent; but see ``_SchemaWalk.visit``.
    nullable: bool
    # The definition level at which an element of this array is non-null.
    def_level: int
    # Its length: ``scale`` (above 1 only under a fixed_size_list) times the level
    # entries at repetition level ``length_rep_level`` or less and definition
    # level ``length_def_level`` or more.
    length_rep_level: int
    length_def_level: int
    scale: int
    # Bytes per element: a list's or string's offset, a fixed-width leaf's value.
    width: int = 0
    # A bitmap even when null-free: a zero-copy read keeps the one it decoded.
    keeps_bitmap: bool = False


class LeafProfile(NamedTuple):
    """One leaf's schema facts, the same in every row group of the file."""

    # The leaf's Arrow arrays from its top-level column down, the leaf last.
    nodes: tuple[_ArrowNode, ...]
    max_definition_level: int
    max_repetition_level: int
    # The writer counts every DELTA_BYTE_ARRAY value in SizeStatistics bytes.
    counts_delta_byte_array: bool = False


class _Position(NamedTuple):
    """The level context the schema walk hands a child array."""

    # Repeated ancestors so far, which is a leaf's max repetition level.
    depth: int
    # The definition level at which the parent is non-null.
    parent_def: int
    length_rep_level: int
    length_def_level: int
    scale: int


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


class _SchemaWalk:
    """Flattens an Arrow schema into one node chain per Parquet leaf."""

    def __init__(self) -> None:
        # ``(chain, max_repetition_level)`` per leaf, in schema order.
        self.leaves: list[tuple[tuple[_ArrowNode, ...], int]] = []
        self._next_id = 0

    def _node(self, pos: _Position, **fields: Any) -> _ArrowNode:
        """A new array node at the walk's position ``pos``."""
        self._next_id += 1
        return _ArrowNode(
            node_id=self._next_id,
            length_rep_level=pos.length_rep_level,
            length_def_level=pos.length_def_level,
            scale=pos.scale,
            **fields,
        )

    def visit(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        """Record the chain of arrays down to each leaf under ``arrow_type``."""
        if isinstance(arrow_type, pa.BaseExtensionType):
            arrow_type = arrow_type.storage_type
        # An optional Parquet node adds one definition level, a required one none.
        def_level = pos.parent_def + int(nullable)
        if pa.types.is_struct(arrow_type):
            self._visit_struct(
                arrow_type=arrow_type,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )
        elif pa.types.is_map(arrow_type):
            # A map is a list of non-nullable key/value structs.
            self._visit_list(
                arrow_type=arrow_type,
                child_type=pa.struct([arrow_type.key_field, arrow_type.item_field]),
                child_nullable=False,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )
        elif _is_list(arrow_type):
            self._visit_list(
                arrow_type=arrow_type,
                child_type=arrow_type.value_type,
                child_nullable=arrow_type.value_field.nullable,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )
        else:
            self._visit_leaf(
                arrow_type=arrow_type,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )

    def _visit_struct(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        def_level: int,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        # PyArrow builds a struct's bitmap from a non-repeated child's levels.
        # With none, even a non-nullable struct gets one, carrying its ancestors'
        # nulls up to the enclosing list.
        hides_nulls = _is_repeated(arrow_type) and def_level > pos.length_def_level
        node = self._node(
            pos=pos,
            kind=_STRUCT,
            nullable=nullable or hides_nulls,
            def_level=def_level,
        )
        for field in arrow_type:
            self.visit(
                arrow_type=field.type,
                nullable=field.nullable,
                pos=pos._replace(parent_def=def_level),
                chain=chain + (node,),
            )

    def _visit_list(
        self,
        arrow_type: pa.DataType,
        child_type: pa.DataType,
        child_nullable: bool,
        nullable: bool,
        def_level: int,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        # Parquet writes every list type as a repeated group, which adds a
        # definition level (the list is non-empty) and a repetition level.
        rep_def, depth = def_level + 1, pos.depth + 1
        if pa.types.is_fixed_size_list(arrow_type):
            node = self._node(
                pos=pos, kind=_FIXED_SIZE_LIST, nullable=nullable, def_level=def_level
            )
            # Its children are counted at its own depth, ``list_size`` per element.
            inner = pos._replace(
                depth=depth, parent_def=rep_def, scale=pos.scale * arrow_type.list_size
            )
        else:
            node = self._node(
                pos=pos,
                kind=_LIST,
                nullable=nullable,
                def_level=def_level,
                width=8 if pa.types.is_large_list(arrow_type) else 4,
            )
            inner = _Position(
                depth=depth,
                parent_def=rep_def,
                length_rep_level=depth,
                length_def_level=rep_def,
                scale=1,
            )
        self.visit(
            arrow_type=child_type,
            nullable=child_nullable,
            pos=inner,
            chain=chain + (node,),
        )

    def _visit_leaf(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        def_level: int,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        kind, width = _leaf_layout(arrow_type)
        leaf = self._node(
            pos=pos,
            kind=kind,
            nullable=nullable,
            def_level=def_level,
            width=width,
            keeps_bitmap=_is_zero_copy_type(arrow_type),
        )
        self.leaves.append((chain + (leaf,), pos.depth))


def _walk(arrow_schema: pa.Schema) -> _SchemaWalk:
    """Walk ``arrow_schema`` into leaf chains."""
    walk = _SchemaWalk()
    top = _Position(
        depth=0, parent_def=0, length_rep_level=0, length_def_level=0, scale=1
    )
    for field in arrow_schema:
        walk.visit(arrow_type=field.type, nullable=field.nullable, pos=top, chain=())
    return walk


def _matches(leaf: _ArrowNode, rep: int, column: ColumnSchema) -> bool:
    """Whether a walked leaf has ``column``'s levels and can decode its values."""
    return (
        leaf.def_level == column.max_definition_level
        and rep == column.max_repetition_level
        and (leaf.kind != _BINARY or column.physical_type == "BYTE_ARRAY")
    )


def build_leaf_profiles(
    parquet_schema: ParquetSchema, file_metadata: FileMetaData | None = None
) -> list[LeafProfile | None] | None:
    """One :class:`LeafProfile` per leaf, in row group column order, ``None`` for a
    leaf this module doesn't model.

    Args:
        parquet_schema: The file's schema.
        file_metadata: The file's footer, for its writer.

    Returns:
        The profiles, or ``None`` for a file this module doesn't model at all.
    """
    try:
        walk = _walk(parquet_schema.to_arrow_schema())
    except (pa.ArrowException, ValueError, TypeError):
        # A schema the walk can't handle falls back rather than failing the read.
        return None
    if len(walk.leaves) != len(parquet_schema):
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
        if leaf.kind == _UNSUPPORTED or not _matches(
            leaf=leaf, rep=max_repetition_level, column=column
        ):
            profiles.append(None)
            continue
        if leaf.keeps_bitmap and column.physical_type not in _ZERO_COPY_PHYSICAL_TYPES:
            leaf = leaf._replace(keeps_bitmap=False)
        profiles.append(
            LeafProfile(
                nodes=chain[:-1] + (leaf,),
                max_definition_level=column.max_definition_level,
                max_repetition_level=max_repetition_level,
                counts_delta_byte_array=counts_delta,
            )
        )
    return profiles


# One chunk's counts.
class LeafChunk:
    """One leaf's chunk in a row group, with its counts from the footer."""

    def __init__(
        self,
        index: int,
        profile: LeafProfile,
        chunk: ColumnChunkMetaData,
        stats: LeafSizeStats | None,
        num_rows: int,
    ):
        self.index = index
        self.profile = profile
        self.chunk = chunk
        self.num_rows = num_rows
        self._max_def = profile.max_definition_level
        self._max_rep = profile.max_repetition_level
        # Below a struct, list or map.
        self._nested = len(profile.nodes) > 1
        # SizeStatistics' string bytes, unless the writer undercounts them.
        self.size_statistics_bytes = (
            None if stats is None else stats.unencoded_byte_array_data_bytes
        )
        if (
            self.size_statistics_bytes is not None
            and not profile.counts_delta_byte_array
            and detect_delta_byte_array(chunk)
        ):
            self.size_statistics_bytes = None
        # Histograms that disagree with the footer mean the walk drifted or the
        # writer miscounted (PyArrow with a write_batch_size above the rows); the
        # string bytes from the same statistics go with them.
        if stats is not None and not self._histograms_agree(stats=stats):
            stats = None
            self.size_statistics_bytes = None
        self._repetition = () if stats is None else stats.repetition_level_histogram
        self._definition = () if stats is None else stats.definition_level_histogram
        # Entries below the max definition level, read only for a flat leaf without
        # a definition histogram: building ``Statistics`` costs two thirds of an
        # estimate, and a nested leaf can't use it.
        self._null_count = None
        if self._max_def and not self._definition and not self._nested:
            statistics = chunk.statistics
            if (
                statistics is not None
                and statistics.has_null_count
                and 0 <= statistics.null_count <= chunk.num_values
            ):
                self._null_count = statistics.null_count

    def modelled(self) -> bool:
        """Whether every count is exact; a string leaf needs SizeStatistics bytes."""
        strings_known = (
            self.profile.nodes[-1].kind != _BINARY
            or self.size_statistics_bytes is not None
        )
        return strings_known and self.levels_known()

    def levels_known(self) -> bool:
        """Whether every array length and null count is exact."""
        repetition = bool(self._repetition) or self._max_rep <= 1
        definition = (
            bool(self._definition)
            or self._max_def == 0
            or (not self._nested and self._null_count is not None)
        )
        return repetition and definition

    def array_length(self, node: _ArrowNode | None = None) -> int:
        """An array's length, the leaf's by default."""
        node = self.profile.nodes[-1] if node is None else node
        if node.length_rep_level == 0:
            return self.num_rows * node.scale
        opened = self._opened(node.length_rep_level) - self._below(
            node.length_def_level
        )
        # Histograms that contradict each other can make this negative.
        return max(opened, 0) * node.scale

    def bitmap(self, node: _ArrowNode | None = None) -> int:
        """Bytes of an array's validity bitmap, the leaf's by default."""
        node = self.profile.nodes[-1] if node is None else node
        if not node.nullable or node.kind == _NULL:
            return 0
        if (
            not node.keeps_bitmap
            and self._nulls(
                length_def_level=node.length_def_level, def_level=node.def_level
            )
            == 0
        ):
            return 0
        return _bytes_for_bits(self.array_length(node))

    def _histograms_agree(self, stats: LeafSizeStats) -> bool:
        """Whether the writer's histograms fit the footer's own counts."""
        repetition = stats.repetition_level_histogram
        definition = stats.definition_level_histogram
        if repetition and (
            len(repetition) != self._max_rep + 1
            or repetition[0] != self.num_rows
            or sum(repetition) != self.chunk.num_values
        ):
            return False
        return not definition or (
            len(definition) == self._max_def + 1
            and sum(definition) == self.chunk.num_values
        )

    def _opened(self, depth: int) -> int:
        """Entries at repetition level ``depth`` or less, or every entry."""
        if self._repetition:
            return sum(self._repetition[: depth + 1])
        return self.chunk.num_values

    def _below(self, def_level: int) -> int:
        """Entries under definition level ``def_level``."""
        if self._definition:
            return sum(self._definition[:def_level])
        # A flat leaf, whose one level below the max the null count holds.
        return 0 if def_level <= 0 else self._null_count or 0

    def _nulls(self, length_def_level: int, def_level: int) -> int:
        """Elements that exist but are null (level in
        ``[length_def_level, def_level)``)."""
        return self._below(def_level) - self._below(length_def_level)


def _bytes_for_bits(count: int) -> int:
    """Bytes holding ``count`` bits: a validity bitmap or boolean values."""
    return (count + 7) // 8


def leaf_chunks(
    row_group: RowGroupMetaData,
    leaf_profiles: Sequence[LeafProfile | None],
    leaf_indices: Iterable[int],
    size_stats: Sequence[LeafSizeStats | None] | None,
) -> dict[int, LeafChunk]:
    """A :class:`LeafChunk` by leaf index for each of ``leaf_indices`` in
    ``row_group`` this module models: a missing index means that leaf needs the
    caller's fallback."""
    stats = size_stats or ()
    chunks = (
        LeafChunk(
            index=leaf_idx,
            profile=profile,
            chunk=row_group.column(leaf_idx),
            stats=stats[leaf_idx] if leaf_idx < len(stats) else None,
            num_rows=row_group.num_rows,
        )
        for leaf_idx in leaf_indices
        if (profile := leaf_profiles[leaf_idx]) is not None
    )
    return {leaf.index: leaf for leaf in chunks if leaf.modelled()}


# The estimate.
def estimate_row_group(leaves: Iterable[LeafChunk]) -> int:
    """Decoded bytes of ``leaves``, chunks of one row group."""
    size = 0
    counted: set[int] = set()
    for leaf in leaves:
        size += estimate_ancestors(leaf=leaf, counted=counted)
        size += estimate_leaf(leaf)
    return size


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


# Plain string and binary leaves.
def detect_string_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_large_binary(arrow_type)
    )


def estimate_string_type(leaf: LeafChunk, string_bytes: int) -> int:
    """An offset per element, plus one, and the values' bytes."""
    offsets = leaf.profile.nodes[-1].width * (leaf.array_length() + 1)
    return leaf.bitmap() + offsets + string_bytes


def _offset_width(arrow_type: pa.DataType) -> int:
    """Bytes of a string or binary value's offset."""
    if pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 8
    return 4


# Where a string leaf's bytes come from.
def estimate_string_bytes(leaf: LeafChunk) -> int:
    """A BYTE_ARRAY chunk's value bytes, without their length prefixes."""
    # ``LeafChunk.modelled`` keeps only string leaves with these bytes.
    assert leaf.size_statistics_bytes is not None
    return leaf.size_statistics_bytes


# Any DELTA_BYTE_ARRAY page.
def detect_delta_byte_array(chunk: ColumnChunkMetaData) -> bool:
    return "DELTA_BYTE_ARRAY" in chunk.encodings
