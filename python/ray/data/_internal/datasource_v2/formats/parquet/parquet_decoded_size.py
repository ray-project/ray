"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

The Arrow schema is walked into one chain of arrays per Parquet leaf: its structs
and lists, then the leaf. Each array is sized from the footer's counts as validity
bitmap plus offsets plus values: exact with SizeStatistics, a bound without.
Unsupported schemas give ``None``.
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Iterator, Mapping, NamedTuple, Sequence

import pyarrow as pa
from pyarrow.parquet import (
    ColumnChunkMetaData,
    ColumnSchema,
    FileMetaData,
    ParquetSchema,
    RowGroupMetaData,
)

from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    DICTIONARY_PAGE_HEADER_READ_BYTES,
    DictionaryPage,
    LeafSizeStats,
)

# Array kinds. ``_LIST`` has one offsets buffer: a list, large_list or map. A
# fixed_size_list has none; its children hold ``list_size`` slots per slot.
_STRUCT = "struct"
_LIST = "list"
_FIXED_SIZE_LIST = "fixed_size_list"
_FIXED_WIDTH = "fixed_width"
_BOOLEAN = "boolean"
_NULL = "null"  # Arrow's null type allocates no buffers.
# The leaf kinds only BYTE_ARRAY values decode to.
_BINARY = "binary"
_DICTIONARY = "dictionary"
_VIEW = "view"
_TEXT_KINDS = (_BINARY, _DICTIONARY, _VIEW)

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
    # The definition level at which this array's slot is non-null.
    def_level: int
    # Its length: ``scale`` (above 1 only under a fixed_size_list) times the level
    # entries at repetition level ``slot_depth`` or less and definition level
    # ``slot_def`` or more.
    slot_depth: int
    slot_def: int
    scale: int
    # Bytes per slot: a list's offset, a fixed-width leaf's value; else 0.
    width: int = 0
    # A bitmap even when null-free: a zero-copy read keeps the one it decoded,
    # and a cast out of a dictionary writes one.
    keeps_bitmap: bool = False
    # A leaf's Arrow type, an extension type's storage type; ``None`` above it.
    arrow_type: pa.DataType | None = None


class LeafProfile(NamedTuple):
    """One leaf's schema facts, the same in every row group of the file."""

    # The leaf's Arrow arrays from its top-level column down, the leaf last.
    nodes: tuple[_ArrowNode, ...]
    max_definition_level: int
    max_repetition_level: int
    # The writer counts every DELTA_BYTE_ARRAY value in SizeStatistics bytes.
    counts_delta_byte_array: bool = False
    # The column's leaves and :func:`cast_slot_bytes`, when the pin casts it so
    # its leaves can't follow, which leaves them walked as stored.
    unmapped_cast: tuple[tuple[int, ...], int] | None = None
    # The pin casts it out of a non-nullable dictionary under a nullable struct.
    hides_dictionary_slots: bool = False
    # The pin casts its values to text, which replaces them.
    cast_to_text: bool = False


class _UnsupportedLayout(Exception):
    """An Arrow type this module cannot size, which sends the file to fallback."""


class _Position(NamedTuple):
    """The level context the schema walk hands a child array."""

    # Repeated ancestors so far, which is a leaf's max repetition level.
    depth: int
    # The definition level at which the parent is non-null.
    parent_def: int
    slot_depth: int
    slot_def: int
    scale: int


def _leaf_layout(arrow_type: pa.DataType) -> tuple[str, int]:
    """``(kind, width)`` of a leaf's Arrow type."""
    if pa.types.is_null(arrow_type):
        return _NULL, 0
    if pa.types.is_boolean(arrow_type):
        return _BOOLEAN, 0
    if detect_string_type(arrow_type):
        return _BINARY, 0
    if detect_view_type(arrow_type):
        return _VIEW, 0
    if detect_dictionary_type(arrow_type):
        # PyArrow restores a dictionary over string or binary values only.
        if not detect_string_type(arrow_type.value_type):
            raise _UnsupportedLayout(arrow_type)
        return _DICTIONARY, 0
    try:
        bit_width = arrow_type.bit_width
    except ValueError:
        raise _UnsupportedLayout(arrow_type) from None
    if bit_width % 8:
        raise _UnsupportedLayout(arrow_type)
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
            slot_depth=pos.slot_depth,
            slot_def=pos.slot_def,
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
            # PyArrow builds a struct's bitmap from a non-repeated child's levels.
            # With none, even a non-nullable struct gets one, carrying its
            # ancestors' nulls up to the enclosing list.
            hides_nulls = _is_repeated(arrow_type) and def_level > pos.slot_def
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
            return

        if pa.types.is_map(arrow_type):
            # A map is a list of non-nullable key/value structs.
            child_type = pa.struct([arrow_type.key_field, arrow_type.item_field])
            child_nullable = False
        elif _is_list(arrow_type):
            child_type = arrow_type.value_type
            child_nullable = arrow_type.value_field.nullable
        else:
            kind, width = _leaf_layout(arrow_type)
            leaf = self._node(
                pos=pos,
                kind=kind,
                nullable=nullable,
                def_level=def_level,
                width=width,
                keeps_bitmap=_is_zero_copy_type(arrow_type),
                arrow_type=arrow_type,
            )
            self.leaves.append((chain + (leaf,), pos.depth))
            return

        # Parquet writes every list type as a repeated group, which adds a
        # definition level (the list is non-empty) and a repetition level.
        rep_def, depth = def_level + 1, pos.depth + 1
        if pa.types.is_fixed_size_list(arrow_type):
            node = self._node(
                pos=pos, kind=_FIXED_SIZE_LIST, nullable=nullable, def_level=def_level
            )
            # Its children are counted at its own depth, ``list_size`` per slot.
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
                slot_depth=depth,
                slot_def=rep_def,
                scale=1,
            )
        self.visit(
            arrow_type=child_type,
            nullable=child_nullable,
            pos=inner,
            chain=chain + (node,),
        )


def _walk(arrow_schema: pa.Schema) -> _SchemaWalk | None:
    """Walk ``arrow_schema`` into leaf chains, or ``None`` if it can't be sized."""
    walk = _SchemaWalk()
    top = _Position(depth=0, parent_def=0, slot_depth=0, slot_def=0, scale=1)
    try:
        for field in arrow_schema:
            walk.visit(
                arrow_type=field.type, nullable=field.nullable, pos=top, chain=()
            )
    except _UnsupportedLayout:
        return None
    return walk


def _matches(leaf: _ArrowNode, rep: int, column: ColumnSchema) -> bool:
    """Whether a walked leaf has ``column``'s levels and can decode its values."""
    return (
        leaf.def_level == column.max_definition_level
        and rep == column.max_repetition_level
        and (leaf.kind not in _TEXT_KINDS or column.physical_type == "BYTE_ARRAY")
    )


def build_leaf_profiles(
    parquet_schema: ParquetSchema,
    pinned_schema: pa.Schema | None = None,
    format_kwargs: Mapping[str, Any] | None = None,
    file_metadata: FileMetaData | None = None,
) -> list[LeafProfile] | None:
    """One :class:`LeafProfile` per leaf, in row group column order.

    Args:
        parquet_schema: The file's schema.
        pinned_schema: The schema the read casts every batch to, if any.
        format_kwargs: The read's ``ParquetFileFormat`` kwargs.
        file_metadata: The file's footer: its writer and stored Arrow schema.

    Returns:
        The profiles, or ``None`` for a schema this module doesn't model.
    """
    try:
        arrow_schema = parquet_schema.to_arrow_schema()
    except (pa.ArrowException, ValueError, TypeError):
        return None
    stored = _walk(arrow_schema)
    if stored is None or len(stored.leaves) != len(parquet_schema):
        return None
    writer = "" if file_metadata is None else file_metadata.created_by or ""
    # Arrow C++ writers leave out of a DELTA_BYTE_ARRAY chunk's count each value
    # equal to, or a prefix of, the one before: 1,000 copies of an 8-byte string
    # report 8 bytes, where arrow-rs reports 8,000.
    counts_delta = writer != "" and not writer.startswith("parquet-cpp")
    format_kwargs = format_kwargs or {}
    if pinned_schema is None and detect_binary_or_list_type(format_kwargs):
        stores = file_metadata is not None and b"ARROW:schema" in (
            file_metadata.metadata or {}
        )
        pinned_schema = type_option_schema(
            arrow_schema=arrow_schema,
            format_kwargs=format_kwargs,
            stores_arrow_schema=stores,
        )
    walk, unmapped, to_text = stored, {}, set()
    if pinned_schema is not None:
        walk, unmapped, to_text = _pinned_walk(
            arrow_schema=arrow_schema,
            pinned_schema=pinned_schema,
            parquet_schema=parquet_schema,
            stored=stored,
            file_metadata=file_metadata,
        )

    profiles: list[LeafProfile] = []
    for leaf_idx, (chain, max_repetition_level) in enumerate(walk.leaves):
        column = parquet_schema.column(leaf_idx)
        leaf = chain[-1]
        if not _matches(leaf=leaf, rep=max_repetition_level, column=column):
            return None
        if leaf.keeps_bitmap and column.physical_type not in _ZERO_COPY_PHYSICAL_TYPES:
            leaf = leaf._replace(keeps_bitmap=False)
        file_leaf = stored.leaves[leaf_idx][0][-1]
        from_dictionary = leaf.kind != _DICTIONARY and file_leaf.kind == _DICTIONARY
        if from_dictionary:
            # The cast gives every value a validity bit, even in a required column.
            leaf = leaf._replace(nullable=True, keeps_bitmap=True)
        profiles.append(
            LeafProfile(
                nodes=chain[:-1] + (leaf,),
                max_definition_level=column.max_definition_level,
                max_repetition_level=max_repetition_level,
                counts_delta_byte_array=counts_delta,
                unmapped_cast=unmapped.get(leaf_idx),
                hides_dictionary_slots=from_dictionary
                and not file_leaf.nullable
                and file_leaf.def_level > file_leaf.slot_def,
                cast_to_text=leaf_idx in to_text,
            )
        )
    return profiles


def dictionary_page_read_size(
    profile: LeafProfile, chunk: ColumnChunkMetaData, stats: LeafSizeStats | None
) -> int:
    """Bytes of this chunk's dictionary page header to read, or 0 if not needed."""
    if (
        profile.nodes[-1].kind in (_BINARY, _VIEW)
        and detect_dictionary_indexed(chunk)
        and not detect_delta_byte_array(chunk)
        and (stats is None or stats.unencoded_byte_array_data_bytes is None)
    ):
        return DICTIONARY_PAGE_HEADER_READ_BYTES
    return 0


# One chunk's counts.
class LeafChunk:
    """One leaf's chunk in a row group: exact counts from SizeStatistics, else bounds."""

    def __init__(
        self,
        index: int,
        profile: LeafProfile,
        chunk: ColumnChunkMetaData,
        stats: LeafSizeStats | None,
        num_rows: int,
        dictionary_page: DictionaryPage | None = None,
    ):
        self.index = index
        self.profile = profile
        self.chunk = chunk
        self.num_rows = num_rows
        # The header :func:`dictionary_page_read_size` asks for, if it does.
        self.dictionary_page = dictionary_page
        self._max_def = profile.max_definition_level
        self._max_rep = profile.max_repetition_level
        # Below a struct, list or map.
        self._nested = len(profile.nodes) > 1
        # SizeStatistics' string bytes, unless the writer undercounts them.
        self.size_statistics_bytes = (
            None if stats is None else stats.unencoded_byte_array_data_bytes
        )
        if detect_delta_byte_array(chunk) and not profile.counts_delta_byte_array:
            self.size_statistics_bytes = None
        # Histograms that disagree with the footer mean the walk drifted or the
        # writer miscounted (PyArrow 24 with a write_batch_size above the rows).
        if stats is not None and not self._histograms_agree(stats=stats):
            stats = None
        self._repetition = () if stats is None else stats.repetition_level_histogram
        self._definition = () if stats is None else stats.definition_level_histogram
        # Entries below the max definition level, read only without a definition
        # histogram: building ``Statistics`` costs two thirds of an estimate.
        self._null_count = None
        if self._max_def and not self._definition:
            statistics = chunk.statistics
            if (
                statistics is not None
                and statistics.has_null_count
                and 0 <= statistics.null_count <= chunk.num_values
            ):
                self._null_count = statistics.null_count

    @property
    def type(self) -> pa.DataType:
        """The leaf array's Arrow type; an extension type's storage type."""
        return self.profile.nodes[-1].arrow_type

    def levels_known(self) -> bool:
        """Whether every slot and null count is exact, not a bound."""
        repetition = bool(self._repetition) or self._max_rep <= 1
        definition = (
            bool(self._definition)
            or self._max_def == 0
            or (not self._nested and self._null_count is not None)
        )
        return repetition and definition

    def slots(self, node: _ArrowNode | None = None) -> int:
        """An array's length, the leaf's by default; a bound when a count is unknown."""
        node = self.profile.nodes[-1] if node is None else node
        if node.slot_depth == 0:
            return self.num_rows * node.scale
        opened = self._opened(node.slot_depth) - (self._below(node.slot_def) or 0)
        return opened * node.scale

    def bitmap(self, node: _ArrowNode | None = None) -> int:
        """Bytes of an array's validity bitmap, the leaf's by default."""
        node = self.profile.nodes[-1] if node is None else node
        if not node.nullable or node.kind == _NULL:
            return 0
        # Unknown counts as present: undersizing is what overfills a read task.
        if (
            not node.keeps_bitmap
            and self._nulls(slot_def=node.slot_def, def_level=node.def_level) == 0
        ):
            return 0
        return _bitmap_size(self.slots(node))

    def values(self) -> int:
        """Non-null leaf values, or every level entry when that is unknown."""
        return self.chunk.num_values - (self._below(self._max_def) or 0)

    def fewest_values(self) -> int:
        """Non-null leaf values, or none when that is unknown."""
        below = self._below(self._max_def)
        return 0 if below is None else self.chunk.num_values - below

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

    def _below(self, def_level: int) -> int | None:
        """Entries under definition level ``def_level``, or ``None`` if unknown."""
        if self._definition:
            return sum(self._definition[:def_level])
        # The null count stands in, and can run low inside a list.
        if def_level <= 0 or self._null_count == 0:
            return 0
        if def_level >= self._max_def:
            return self._null_count
        return None

    def _nulls(self, slot_def: int, def_level: int) -> int | None:
        """Slots that exist but are null (level in ``[slot_def, def_level)``)."""
        if self._nested and not self._definition:
            # PyArrow writes a null count of 0 for some nested leaves under null
            # ancestors (from an Arrow dictionary, or with a nullable struct
            # below the list), so only the histogram can rule nulls out there.
            return None
        below = self._below(def_level)
        if below == 0:
            return 0
        above = self._below(slot_def)
        if below is None or above is None:
            return None
        return below - above


def _bitmap_size(length: int) -> int:
    return (length + 7) // 8


def leaf_chunks(
    row_group: RowGroupMetaData,
    leaf_profiles: Sequence[LeafProfile],
    leaf_indices: Iterable[int],
    size_stats: Sequence[LeafSizeStats | None] | None,
    dictionary_pages: Mapping[int, DictionaryPage] | None = None,
) -> list[LeafChunk]:
    """A :class:`LeafChunk` for each of ``leaf_indices`` in ``row_group``."""
    pages, stats = dictionary_pages or {}, size_stats or ()
    return [
        LeafChunk(
            index=leaf_idx,
            profile=leaf_profiles[leaf_idx],
            chunk=row_group.column(leaf_idx),
            stats=stats[leaf_idx] if leaf_idx < len(stats) else None,
            num_rows=row_group.num_rows,
            dictionary_page=pages.get(leaf_idx),
        )
        for leaf_idx in leaf_indices
    ]


# The estimate.
class Read(NamedTuple):
    """What a read task does besides decoding the file's columns."""

    # The ``ParquetFileFormat`` kwargs it passes, so an option the reader drops
    # never reaches here.
    format_kwargs: Mapping[str, Any]
    # Bytes per row of the columns Ray appends, and once per row group on top.
    added_row_bytes: float = 0
    added_row_group_bytes: int = 0


def estimate_row_group(
    row_group: RowGroupMetaData, leaves: Sequence[LeafChunk], read: Read
) -> int:
    """Decoded bytes of ``leaves``, chunks of ``row_group``."""
    size = 0
    counted: set[int] = set()
    for leaf in leaves:
        # Counted from the leaf's levels, so a bound wherever those are.
        ancestors = estimate_ancestors(leaf=leaf, counted=counted)
        size += ancestors if leaf.levels_known() else shave_bound(ancestors)
        # Text a cast writes replaces the leaf: :func:`estimate_unmapped_casts`.
        if not leaf.profile.cast_to_text:
            size += estimate_leaf(leaf=leaf, read=read)
        if detect_hidden_dictionary_slots(leaf):
            size += estimate_hidden_dictionary_slots(leaf)
    if detect_unmapped_casts(leaves):
        size += shave_bound(estimate_unmapped_casts(leaves))
    if detect_added_columns(read):
        size += estimate_added_columns(row_group=row_group, read=read)
    return size


def estimate_leaf(leaf: LeafChunk, read: Read) -> int:
    """Decoded bytes of one chunk's leaf array, shaved where they are a bound."""
    arrow_type, bound = leaf.type, True
    if detect_dictionary_columns(chunk=leaf.chunk, format_kwargs=read.format_kwargs):
        size = estimate_dictionary_columns(leaf)
    elif detect_dictionary_type(arrow_type):
        size = estimate_dictionary(leaf=leaf, arrow_type=arrow_type)
    elif detect_view_type(arrow_type):
        size = estimate_view_type(leaf)
    elif detect_string_type(arrow_type):
        string_bytes, bound = _string_bytes(leaf)
        size = estimate_string_type(leaf=leaf, string_bytes=string_bytes)
    else:
        size, bound = estimate_fixed_width(leaf), False
    return shave_bound(size) if bound or not leaf.levels_known() else size


# Fixed-width, boolean and null leaves, and the arrays above every leaf: exact
# once the counts are.
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
    slots = leaf.slots(node)
    size = leaf.bitmap(node)
    if node.kind == _LIST:
        size += node.width * (slots + 1)
    elif node.kind == _FIXED_WIDTH:
        size += node.width * slots
    elif node.kind == _BOOLEAN:
        size += _bitmap_size(slots)
    return size


# What PyArrow returns for ``dictionary_columns``, whatever the type hints say.
_DICTIONARY_COLUMNS_TYPE = pa.dictionary(pa.int32(), pa.binary())


# The ``dictionary_columns`` read option: BYTE_ARRAY leaves, named by dotted path
# ("tags.list.element"), that PyArrow reads as dictionaries unless ``binary_type``
# is set.
def detect_dictionary_columns(
    chunk: ColumnChunkMetaData, format_kwargs: Mapping[str, Any]
) -> bool:
    return (
        chunk.physical_type == "BYTE_ARRAY"
        and chunk.path_in_schema in (format_kwargs.get("dictionary_columns") or ())
        and not _sets_binary_type(format_kwargs)
    )


# Under only when a row group's column holds over 256 MiB of distinct values.
_DICTIONARY_COLUMNS_CAP = 256 * 1024 * 1024


def estimate_dictionary_columns(leaf: LeafChunk) -> int:
    """:func:`estimate_dictionary`, its PLAIN-page bound capped at 256 MiB."""
    size = estimate_dictionary(leaf=leaf, arrow_type=_DICTIONARY_COLUMNS_TYPE)
    indices = leaf.bitmap() + 4 * leaf.slots()
    return min(size, indices + _DICTIONARY_COLUMNS_CAP)


# The ``binary_type`` / ``list_type`` read options: wider string, binary or list
# arrays, which the leaf profiles walk (:func:`type_option_schema`).
def detect_binary_or_list_type(format_kwargs: Mapping[str, Any]) -> bool:
    large_list = format_kwargs.get("list_type") not in (None, pa.ListType)
    return _sets_binary_type(format_kwargs) or large_list


def _sets_binary_type(format_kwargs: Mapping[str, Any]) -> bool:
    return format_kwargs.get("binary_type") not in (None, pa.binary())


def type_option_schema(
    arrow_schema: pa.Schema, format_kwargs: Mapping[str, Any], stores_arrow_schema: bool
) -> pa.Schema:
    """The schema a read with ``binary_type`` or ``list_type`` returns: with a
    stored Arrow schema only dictionary text changes; without one, as Spark writes,
    every string, binary and list does."""
    binary_type = format_kwargs.get("binary_type") or pa.binary()
    large_list = format_kwargs.get("list_type") not in (None, pa.ListType)

    def text(arrow_type: pa.DataType) -> pa.DataType:
        string = arrow_type in (pa.string(), pa.large_string(), pa.string_view())
        if pa.types.is_binary_view(binary_type):
            return pa.string_view() if string else pa.binary_view()
        if pa.types.is_large_binary(binary_type):
            return pa.large_string() if string else pa.large_binary()
        return arrow_type

    def retype(arrow_type: pa.DataType) -> pa.DataType:
        if pa.types.is_dictionary(arrow_type) and _sets_binary_type(format_kwargs):
            return text(arrow_type.value_type)
        if not stores_arrow_schema and (
            pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type)
        ):
            return text(arrow_type)
        if pa.types.is_struct(arrow_type):
            return pa.struct([f.with_type(retype(f.type)) for f in arrow_type])
        if pa.types.is_map(arrow_type):
            key, item = arrow_type.key_field, arrow_type.item_field
            return pa.map_(
                key.with_type(retype(key.type)), item.with_type(retype(item.type))
            )
        if _is_list(arrow_type):
            value = arrow_type.value_field.with_type(retype(arrow_type.value_type))
            if pa.types.is_large_list(arrow_type) or (
                large_list and not stores_arrow_schema
            ):
                return pa.large_list(value)
            if pa.types.is_fixed_size_list(arrow_type):
                return pa.list_(value, arrow_type.list_size)
            return pa.list_(value)
        return arrow_type

    return pa.schema([f.with_type(retype(f.type)) for f in arrow_schema])


# A dictionary type in the file's type hints, e.g. a pandas categorical.
def detect_dictionary_type(arrow_type: pa.DataType) -> bool:
    return pa.types.is_dictionary(arrow_type)


def estimate_dictionary(leaf: LeafChunk, arrow_type: pa.DictionaryType) -> int:
    """An upper bound: the pages hold every distinct value behind a 4-byte length."""
    offset_width = _offset_width(arrow_type.value_type)
    return (
        leaf.bitmap()
        + arrow_type.index_type.bit_width // 8 * leaf.slots()
        + offset_width * leaf.chunk.total_uncompressed_size // 4
        + 4 * offset_width
    )


# A view type in the type hints: ``string_view`` or ``binary_view``.
def detect_view_type(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string_view(arrow_type) or pa.types.is_binary_view(arrow_type)


def estimate_view_type(leaf: LeafChunk) -> int:
    """An upper bound: a 16-byte view per slot and every value's bytes. Exact past
    the 12 bytes a view holds inline; 1.75x when every value is 12."""
    return leaf.bitmap() + 16 * leaf.slots() + estimate_string_bytes(leaf)


# Plain string and binary leaves.
def detect_string_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_large_binary(arrow_type)
    )


def estimate_string_type(leaf: LeafChunk, string_bytes: int) -> int:
    """An offset per slot, plus one, and the values' bytes: exact once they are."""
    return leaf.bitmap() + _offset_width(leaf.type) * (leaf.slots() + 1) + string_bytes


def _offset_width(arrow_type: pa.DataType) -> int:
    """A text value's bytes besides its characters: an offset, or a view."""
    if pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
        return 4
    if pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 8
    return 16


_DICTIONARY_ENCODINGS = frozenset({"RLE_DICTIONARY", "PLAIN_DICTIONARY"})


# Where a string leaf's bytes come from.
def estimate_string_bytes(leaf: LeafChunk) -> int:
    """A BYTE_ARRAY chunk's value bytes, without their length prefixes."""
    return _string_bytes(leaf)[0]


def _string_bytes(leaf: LeafChunk) -> tuple[int, bool]:
    """The value bytes, and whether they are the upper bound from plain pages."""
    if leaf.size_statistics_bytes is not None:
        return leaf.size_statistics_bytes, False
    if detect_delta_byte_array(leaf.chunk):
        return estimate_delta_byte_array(leaf.chunk), False
    if detect_dictionary_indexed(leaf.chunk):
        return estimate_dictionary_indexed(leaf), False
    fewest_values = leaf.fewest_values()
    return estimate_plain_bytes(chunk=leaf.chunk, fewest_values=fewest_values), True


# Any DELTA_BYTE_ARRAY page without SizeStatistics bytes to trust: none written,
# as by Spark 3.5, or PyArrow's.
def detect_delta_byte_array(chunk: ColumnChunkMetaData) -> bool:
    return "DELTA_BYTE_ARRAY" in chunk.encodings


# Above the most any measured column decoded to: values in sorted runs, at 76
# times their chunk's uncompressed bytes, which 64 would have put under.
_DELTA_BYTE_ARRAY_EXPANSION = 128


def estimate_delta_byte_array(chunk: ColumnChunkMetaData) -> int:
    """A guess: the values at 128 times the chunk's uncompressed bytes. Under for
    long sorted runs: a million rows of 2 KB URLs, each repeated 1,000 times."""
    expansion = min(chunk.num_values, _DELTA_BYTE_ARRAY_EXPANSION)
    return expansion * chunk.total_uncompressed_size


# Data pages that index a dictionary page, without SizeStatistics bytes.
def detect_dictionary_indexed(chunk: ColumnChunkMetaData) -> bool:
    # A format 1.0 file names its dictionary page PLAIN_DICTIONARY too, so a
    # page no data page uses lands here as well.
    return chunk.has_dictionary_page and not _DICTIONARY_ENCODINGS.isdisjoint(
        chunk.encodings
    )


# The rows' values can run longer than the dictionary's entries on average; 1.2
# measured a median of 1.15 to 1.18 times the decoded size.
_DICTIONARY_MEAN_FACTOR = 1.2


def estimate_dictionary_indexed(leaf: LeafChunk) -> int:
    """A guess: every value at 1.2 times the dictionary's mean length. Under when
    the common values are longer: "COMPLETED_SUCCESSFULLY" in 97% of rows beside
    "OK" comes out at half. Without the header, a crude bound."""
    page = leaf.dictionary_page
    if page is None:
        # No mean: every value as long as the chunk.
        return (leaf.slots() + 1) * leaf.chunk.total_uncompressed_size
    entries = page.num_values
    mean = max(page.uncompressed_page_size - 4 * entries, 0) / max(entries, 1)
    # The footer's null count can be wrong (Polars gives each page the chunk's),
    # so the values are no fewer than the data pages hold indices for.
    values = max(leaf.values(), _indexed_values(leaf=leaf, page=page))
    at_mean = math.ceil(_DICTIONARY_MEAN_FACTOR * mean * values)
    # A writer that gave up on the dictionary wrote the rest as PLAIN values.
    plain = estimate_plain_bytes(chunk=leaf.chunk, fewest_values=leaf.fewest_values())
    return max(at_mean, plain)


def _indexed_values(leaf: LeafChunk, page: DictionaryPage) -> int:
    """The values the data pages hold at least an index each for."""
    chunk, profile = leaf.chunk, leaf.profile
    # The data pages' bytes, less the dictionary page and the bit-packed levels.
    level_bits = (
        profile.max_definition_level.bit_length()
        + profile.max_repetition_level.bit_length()
    )
    data = chunk.total_uncompressed_size - page.uncompressed_page_size
    data -= math.ceil(chunk.num_values * level_bits / 8)
    # RLE runs only shrink indices, and page headers add a little. A PLAIN
    # fallback's values would count many times, so it stops at the chunk's.
    index_bits = max(page.num_values - 1, 1).bit_length()
    return min(chunk.num_values, max(data, 0) * 8 // index_bits)


# Every value in PLAIN or DELTA_LENGTH_BYTE_ARRAY pages.
def estimate_plain_bytes(chunk: ColumnChunkMetaData, fewest_values: int) -> int:
    """An upper bound: those pages hold every value's bytes, uncompressed."""
    prefix = 0 if "DELTA_LENGTH_BYTE_ARRAY" in chunk.encodings else 4
    return max(chunk.total_uncompressed_size - prefix * fewest_values, 0)


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
    arrow_schema: pa.Schema,
    pinned_schema: pa.Schema,
    parquet_schema: ParquetSchema,
    stored: _SchemaWalk,
    file_metadata: FileMetaData | None,
) -> tuple[_SchemaWalk, dict[int, tuple[tuple[int, ...], int]], set[int]]:
    """The pinned walk, each leaf's unmapped cast, and the leaves cast to text."""
    unmapped: dict[int, tuple[tuple[int, ...], int]] = {}
    to_text: set[int] = set()
    fields, start = [], 0
    for field in arrow_schema:
        field_walk = _walk(pa.schema([field]))
        # The whole schema walked, so each of its fields does.
        assert field_walk is not None
        end = start + len(field_walk.leaves)
        leaves = tuple(range(start, end))
        # An all-null column keeps its type: its nulls count as an added column.
        arrow_type = (
            None
            if pa.types.is_null(field.type)
            else pinned_type(field=field, pinned_schema=pinned_schema)
        )
        pinned = field if arrow_type is None else field.with_type(arrow_type)
        if pinned is not field and not _cast_maps(
            pinned=pinned,
            columns=[parquet_schema.column(j) for j in leaves],
            file_leaves=stored.leaves[start:end],
        ):
            slot_bytes = cast_slot_bytes(
                file_type=field.type,
                pinned_type=pinned.type,
                leaves=leaves,
                file_metadata=file_metadata,
            )
            unmapped.update(dict.fromkeys(leaves, (leaves, slot_bytes)))
            texts = text_cast_leaves(file_type=field.type, pinned_type=pinned.type)
            if len(texts) == len(leaves):
                to_text.update(j for j, text in zip(leaves, texts) if text)
            pinned = field
        fields.append(pinned)
        start = end
    walk = _walk(pa.schema(fields))
    # Each field is the stored one or a pin :func:`_cast_maps` walked.
    assert walk is not None
    return walk, unmapped, to_text


def _cast_maps(
    pinned: pa.Field,
    columns: Sequence[ColumnSchema],
    file_leaves: Sequence[tuple[tuple[_ArrowNode, ...], int]],
) -> bool:
    """Whether a column's leaves follow the cast to ``pinned`` one for one."""
    walk = _walk(pa.schema([pinned]))
    if walk is None or len(walk.leaves) != len(columns):
        return False
    return all(
        _matches(leaf=chain[-1], rep=rep, column=column)
        and (chain[-1].kind != _DICTIONARY or file_chain[-1].kind == _DICTIONARY)
        for (chain, rep), column, (file_chain, _) in zip(
            walk.leaves, columns, file_leaves
        )
    )


# A pinned column whose leaves the cast doesn't keep one for one: a struct that
# gained a field, a nested field made nullable, numbers cast to text, plain
# strings cast to a dictionary.
def detect_unmapped_casts(leaves: Sequence[LeafChunk]) -> bool:
    return any(leaf.profile.unmapped_cast for leaf in leaves)


def estimate_unmapped_casts(leaves: Sequence[LeafChunk]) -> int:
    """A crude bound: :func:`cast_slot_bytes` for each value of the longest leaf."""
    chosen = {leaf.index: leaf.chunk for leaf in leaves}
    casts = {
        leaf.profile.unmapped_cast
        for leaf in leaves
        if leaf.profile.unmapped_cast is not None
    }
    return sum(
        max(chosen[j].num_values for j in cast_leaves if j in chosen) * slot_bytes
        for cast_leaves, slot_bytes in casts
    )


def cast_slot_bytes(
    file_type: pa.DataType,
    pinned_type: pa.DataType,
    leaves: Sequence[int | None] = (),
    file_metadata: FileMetaData | None = None,
) -> int:
    """Bytes per element of every array of ``pinned_type``, summed. Text the cast
    writes counts at its longest: from the leaf's min and max
    (:func:`statistics_text_bytes`), else the type's (:func:`_text_bytes`)."""
    texts = text_cast_leaves(file_type=file_type, pinned_type=pinned_type)
    if len(leaves) != len(texts) or file_metadata is None:
        leaves = (None,) * len(texts)
    text_bytes = 0
    for (_, stored), text, leaf_idx in zip(_leaf_types(file_type), texts, leaves):
        if not text:
            continue
        recorded = None
        if leaf_idx is not None and file_metadata is not None:
            row_groups = range(file_metadata.num_row_groups)
            chunks = [file_metadata.row_group(i).column(leaf_idx) for i in row_groups]
            recorded = statistics_text_bytes(arrow_type=stored, chunks=chunks)
        longest = _text_bytes(stored) if recorded is None else recorded
        text_bytes = max(text_bytes, longest)
    return _row_bytes(arrow_type=pinned_type, text_bytes=text_bytes)


def statistics_text_bytes(
    arrow_type: pa.DataType, chunks: Sequence[ColumnChunkMetaData]
) -> int | None:
    """The longest text an integer or decimal leaf casts to, from its min and max:
    ids 0 to 49,999 write 5 bytes, not 20. ``None`` when a chunk lacks them."""
    decimal = pa.types.is_decimal(arrow_type) and arrow_type.scale >= 0
    if not (pa.types.is_integer(arrow_type) or decimal) or not chunks:
        return None
    longest = 0
    try:
        for chunk in chunks:
            statistics = chunk.statistics
            if statistics is None or not statistics.has_min_max:
                return None
            for value in (statistics.min, statistics.max):
                if value is None:
                    return None
                if decimal:
                    # "-0.05": the unscaled digits, at least one before the point.
                    scale = arrow_type.scale
                    unscaled = int(value.scaleb(scale))
                    width = max(len(str(abs(unscaled))), scale + 1) + (scale > 0)
                    longest = max(longest, (unscaled < 0) + width)
                else:
                    longest = max(longest, len(str(value)))
    except (AttributeError, TypeError, ValueError):
        return None
    return longest


def text_cast_leaves(
    file_type: pa.DataType, pinned_type: pa.DataType
) -> tuple[bool, ...]:
    """For each of ``file_type``'s leaves, whether the cast makes its values text."""
    pinned = dict(_leaf_types(pinned_type))
    return tuple(
        _is_text(pinned.get(path, pa.null()))
        and not (_is_text(stored) or pa.types.is_null(stored))
        for path, stored in _leaf_types(file_type)
    )


# Digits after the seconds' point, and the point, for each time unit.
_FRACTION_BYTES = {"s": 0, "ms": 4, "us": 7, "ns": 10}
# What :func:`_text_bytes` gives a type it has no width for, such as an interval
# or a decimal with a negative scale ("-9.9999E+6" for a decimal(5, -2)).
_CAST_TEXT_BYTES = 80


def _text_bytes(arrow_type: pa.DataType) -> int:
    """The longest text PyArrow's cast writes for one value of ``arrow_type``."""
    if pa.types.is_boolean(arrow_type):
        return 5  # "false"
    if pa.types.is_signed_integer(arrow_type):
        return len(str(-(2 ** (arrow_type.bit_width - 1))))  # "-128"
    if pa.types.is_unsigned_integer(arrow_type):
        return len(str(2**arrow_type.bit_width - 1))  # "255"
    if pa.types.is_floating(arrow_type):
        # "-0.0000075447247": 9 significant digits for a float, 17 otherwise.
        return 17 if arrow_type.bit_width == 32 else 25
    if pa.types.is_decimal(arrow_type) and arrow_type.scale >= 0:
        # "-0.99999" for a decimal(5, 5), "-0.0000099999" for a decimal(5, 10).
        return max(arrow_type.precision, arrow_type.scale) + 3
    if pa.types.is_date(arrow_type):
        return 10  # "2024-01-31"
    if pa.types.is_timestamp(arrow_type):
        # "2024-01-31 23:59:59.999999", then "Z" or "+0530" with a time zone.
        tz_bytes = 5 if arrow_type.tz else 0
        return 19 + _FRACTION_BYTES[arrow_type.unit] + tz_bytes
    if pa.types.is_time(arrow_type):
        return 8 + _FRACTION_BYTES[arrow_type.unit]  # "23:59:59.999"
    if pa.types.is_duration(arrow_type):
        return 20  # the count of units, an int64
    return _CAST_TEXT_BYTES


def _is_text(arrow_type: pa.DataType) -> bool:
    return detect_string_type(arrow_type) or detect_view_type(arrow_type)


def _leaf_types(
    arrow_type: pa.DataType, path: tuple[str, ...] = ()
) -> Iterator[tuple[tuple[str, ...], pa.DataType]]:
    """``(path, type)`` of each leaf by field names, ``[]`` for list elements."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        for field in arrow_type:
            yield from _leaf_types(arrow_type=field.type, path=path + (field.name,))
    elif pa.types.is_map(arrow_type):
        yield from _leaf_types(
            arrow_type=arrow_type.key_type, path=path + ("[]", "key")
        )
        yield from _leaf_types(
            arrow_type=arrow_type.item_type, path=path + ("[]", "value")
        )
    elif _is_list(arrow_type):
        yield from _leaf_types(arrow_type=arrow_type.value_type, path=path + ("[]",))
    elif pa.types.is_dictionary(arrow_type):
        yield path, arrow_type.value_type
    else:
        yield path, arrow_type


def _row_bytes(arrow_type: pa.DataType, text_bytes: int) -> int:
    """Bytes per element of ``arrow_type``'s array and each array below it."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type

    def below(child_type: pa.DataType) -> int:
        return _row_bytes(arrow_type=child_type, text_bytes=text_bytes)

    # A bitmap byte, then a list's 8-byte offset, the values, and the children.
    if pa.types.is_null(arrow_type):
        return 0
    if pa.types.is_struct(arrow_type):
        return 1 + sum(below(field.type) for field in arrow_type)
    if pa.types.is_fixed_size_list(arrow_type):
        return 1 + arrow_type.list_size * below(arrow_type.value_type)
    if pa.types.is_map(arrow_type):
        entry = pa.struct([arrow_type.key_field, arrow_type.item_field])
        return 1 + 8 + below(entry)
    if _is_list(arrow_type):
        return 1 + 8 + below(arrow_type.value_type)
    if pa.types.is_dictionary(arrow_type):
        index_bytes = arrow_type.index_type.bit_width // 8
        return 1 + index_bytes + below(arrow_type.value_type)
    try:
        return 1 + math.ceil(arrow_type.bit_width / 8)
    except ValueError:
        return 1 + _offset_width(arrow_type) + text_bytes


# A dictionary the pin casts to plain text, in a non-nullable field of a nullable
# struct, e.g. a pandas categorical in an optional struct pinned to string. Each
# slot a null struct hides holds index 0, and the cast copies that value into it.
def detect_hidden_dictionary_slots(leaf: LeafChunk) -> bool:
    return leaf.profile.hides_dictionary_slots and _hidden_slots(leaf) > 0


def estimate_hidden_dictionary_slots(leaf: LeafChunk) -> int:
    """A crude upper bound: no copied value is longer than the uncompressed chunk."""
    stored = leaf.size_statistics_bytes
    if stored is None:
        stored = leaf.chunk.total_uncompressed_size
    return _hidden_slots(leaf) * stored


def _hidden_slots(leaf: LeafChunk) -> int:
    """The leaf's slots no value fills, or every slot when the footer can't say."""
    if not leaf.levels_known():
        # The null count can read 0 under a null struct: see ``LeafChunk._nulls``.
        return leaf.slots()
    return leaf.slots() - leaf.fewest_values()


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
        row += _offset_width(arrow_type) + text_bytes
    else:
        try:
            row += arrow_type.bit_width / 8
        except ValueError:
            # No fixed width, as a list view or a union: a crude 16 bytes a row.
            row += 16
    return row, once


# Some column meets each bound exactly (no nulls, every value long and distinct):
# 0.95 puts that column 5% under and every other column nearer.
_BOUND_FACTOR = 0.95


def shave_bound(size: int) -> int:
    """Scale an upper bound by 0.95; a column that meets it exactly lands at 0.95."""
    return math.ceil(_BOUND_FACTOR * size)
