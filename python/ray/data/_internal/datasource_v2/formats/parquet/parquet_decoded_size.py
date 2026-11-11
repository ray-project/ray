"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

The Arrow schema is walked into one chain of arrays per Parquet leaf: its structs
and lists, then the leaf. Each array is sized from the footer's counts as validity
bitmap plus offsets plus values, exactly. A leaf whose counts aren't exact, or whose
layout it doesn't model (views and dictionaries among them), is left to the caller.
The estimate is for the file's own schema; a read that casts the columns to
another schema allocates more, which the caller prices.
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, NamedTuple, Sequence

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
    parquet_schema: ParquetSchema,
    pinned_schema: pa.Schema | None = None,
    format_kwargs: Mapping[str, Any] | None = None,
    file_metadata: FileMetaData | None = None,
) -> list[LeafProfile | None] | None:
    """One :class:`LeafProfile` per leaf, in row group column order, ``None`` for a
    leaf this module doesn't model.

    Args:
        parquet_schema: The file's schema.
        pinned_schema: The schema the read casts every batch to, if any.
        format_kwargs: The read's ``ParquetFileFormat`` kwargs.
        file_metadata: The file's footer, for its writer.

    Returns:
        The profiles, or ``None`` for a file this module doesn't model at all.
    """
    format_kwargs = format_kwargs or {}
    # Rare reads: the reader drops these options when it casts to the schema.
    if format_kwargs.get("dictionary_columns") or detect_binary_or_list_type(
        format_kwargs
    ):
        return None
    try:
        arrow_schema = parquet_schema.to_arrow_schema()
        walk = _walk(arrow_schema)
        if len(walk.leaves) != len(parquet_schema):
            return None
        unmapped: set[int] = set()
        if pinned_schema is not None:
            walk, unmapped = _pinned_walk(
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
            or leaf.kind == _UNSUPPORTED
            or not _matches(leaf=leaf, rep=max_repetition_level, column=column)
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
class Read(NamedTuple):
    """What a read task does besides decoding the file's columns."""

    # Bytes per row of the columns Ray appends, and once per row group on top.
    added_row_bytes: float = 0
    added_row_group_bytes: int = 0


def estimate_row_group(
    row_group: RowGroupMetaData, leaves: Iterable[LeafChunk], read: Read
) -> int:
    """Decoded bytes of ``leaves``, chunks of ``row_group``."""
    size = 0
    counted: set[int] = set()
    for leaf in leaves:
        size += estimate_ancestors(leaf=leaf, counted=counted)
        size += estimate_leaf(leaf)
    if detect_added_columns(read):
        size += estimate_added_columns(row_group=row_group, read=read)
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


# The ``binary_type`` / ``list_type`` read options, for wider strings or lists.
def detect_binary_or_list_type(format_kwargs: Mapping[str, Any]) -> bool:
    large_list = format_kwargs.get("list_type") not in (None, pa.ListType)
    return format_kwargs.get("binary_type") not in (None, pa.binary()) or large_list


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


# Ray's schema pin: every batch is cast to the dataset schema, unless that holds
# an extension type (``readers.file_reader.casts_to_schema``).
def pinned_type(field: pa.Field, pinned_schema: pa.Schema | None) -> pa.DataType | None:
    """The type the pin casts a file column to, or ``None`` if it keeps its own."""
    if pinned_schema is None:
        return None
    idx = pinned_schema.get_field_index(field.name)
    if idx < 0 or pinned_schema.field(idx).type == field.type:
        return None
    return pinned_schema.field(idx).type


def _pinned_walk(
    arrow_schema: pa.Schema, pinned_schema: pa.Schema, parquet_schema: ParquetSchema
) -> tuple[_SchemaWalk, set[int]]:
    """The pinned walk, and the leaves of columns whose cast changes their shape:
    a struct that gained or reordered fields, a nested field made nullable,
    numbers cast to text. Those are walked as stored and left unmodelled."""
    unmapped: set[int] = set()
    fields, start = [], 0
    for field in arrow_schema:
        end = start + len(_walk(pa.schema([field])).leaves)
        leaves = range(start, end)
        # An all-null column keeps its type: its nulls count as an added column.
        arrow_type = (
            None
            if pa.types.is_null(field.type)
            else pinned_type(field=field, pinned_schema=pinned_schema)
        )
        pinned = field if arrow_type is None else field.with_type(arrow_type)
        # The cast matches struct fields by name, the walk by position.
        if pinned is not field and (
            _struct_names(pinned.type) != _struct_names(field.type)
            or not _cast_maps(
                walk=_walk(pa.schema([pinned])),
                columns=[parquet_schema.column(j) for j in leaves],
            )
        ):
            unmapped.update(leaves)
            pinned = field
        fields.append(pinned)
        start = end
    return _walk(pa.schema(fields)), unmapped


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


def _cast_maps(walk: _SchemaWalk, columns: Sequence[ColumnSchema]) -> bool:
    """Whether a column's leaves follow its pinned walk one for one."""
    return len(walk.leaves) == len(columns) and all(
        _matches(leaf=chain[-1], rep=rep, column=column)
        for (chain, rep), column in zip(walk.leaves, columns)
    )


def _is_view(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string_view(arrow_type) or pa.types.is_binary_view(arrow_type)


def _is_text(arrow_type: pa.DataType) -> bool:
    return detect_string_type(arrow_type) or _is_view(arrow_type)


# Columns Ray adds after decoding: the file's path, Hive partition values, a row
# hash, a checkpoint ID, and every dataset column the file lacks, filled with
# nulls by the pin.
def detect_added_columns(read: Read) -> bool:
    return read.added_row_bytes > 0 or read.added_row_group_bytes > 0


def estimate_added_columns(row_group: RowGroupMetaData, read: Read) -> int:
    """Bytes of the columns Ray appends to one row group."""
    per_row = math.ceil(row_group.num_rows * read.added_row_bytes)
    return per_row + read.added_row_group_bytes


# Bytes an added array takes once per row group: its bitmap's last byte and the
# offset past its last element.
_ADDED_ROW_GROUP_BYTES = 9


def added_bytes(added: Sequence[pa.Field], path: str) -> tuple[float, int]:
    """Bytes per row of the ``added`` columns, and bytes once per row group. Only
    the path and the Hive values cut from it hold text: ``year=2024/`` gives
    ``year`` 4 bytes a row."""
    path_bytes = len(path.encode())
    partitions = dict(part.split("=", 1) for part in path.split("/") if "=" in part)
    per_row, per_row_group = 0.0, 0
    for field in added:
        if field.name == "path":
            text_bytes = path_bytes
        else:
            text_bytes = len(partitions.get(field.name, "").encode())
        row, once = _added_array_bytes(
            arrow_type=field.type, text_bytes=text_bytes, path_bytes=path_bytes
        )
        per_row += row
        per_row_group += once
    return per_row, per_row_group


def _added_array_bytes(
    arrow_type: pa.DataType, text_bytes: int, path_bytes: int
) -> tuple[float, int]:
    """Bytes per element of an added array and its children, and once per row group."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_null(arrow_type):
        return 0, 0

    def below(child_type: pa.DataType, child_text: int) -> tuple[float, int]:
        return _added_array_bytes(
            arrow_type=child_type, text_bytes=child_text, path_bytes=path_bytes
        )

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
        value_row, value_once = below(arrow_type.value_type, path_bytes)
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
