"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

The Arrow schema is walked into one chain of arrays per Parquet leaf: its structs
and lists, then the leaf. Each array is sized from the footer's counts as validity
bitmap plus offsets plus values: exact with SizeStatistics, a bound without.
Leaves it doesn't model, views and dictionaries among them, get no profile.
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
    # The definition level at which this array's slot is non-null.
    def_level: int
    # Its length: ``scale`` (above 1 only under a fixed_size_list) times the level
    # entries at repetition level ``slot_depth`` or less and definition level
    # ``slot_def`` or more.
    slot_depth: int
    slot_def: int
    scale: int
    # Bytes per slot: a list's or string's offset, a fixed-width leaf's value.
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


def _walk(arrow_schema: pa.Schema) -> _SchemaWalk:
    """Walk ``arrow_schema`` into leaf chains."""
    walk = _SchemaWalk()
    top = _Position(depth=0, parent_def=0, slot_depth=0, slot_def=0, scale=1)
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
    except (pa.ArrowException, ValueError, TypeError):
        return None
    walk = _walk(arrow_schema)
    if len(walk.leaves) != len(parquet_schema):
        return None
    writer = "" if file_metadata is None else file_metadata.created_by or ""
    # Arrow C++ writers leave out of a DELTA_BYTE_ARRAY chunk's count each value
    # equal to, or a prefix of, the one before: 1,000 copies of an 8-byte string
    # report 8 bytes, where arrow-rs reports 8,000.
    counts_delta = writer != "" and not writer.startswith("parquet-cpp")
    unmapped: set[int] = set()
    if pinned_schema is not None:
        walk, unmapped = _pinned_walk(
            arrow_schema=arrow_schema,
            pinned_schema=pinned_schema,
            parquet_schema=parquet_schema,
        )

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


def dictionary_page_read_size(
    profile: LeafProfile, chunk: ColumnChunkMetaData, stats: LeafSizeStats | None
) -> int:
    """Bytes of this chunk's dictionary page header to read, or 0 if not needed."""
    if (
        profile.nodes[-1].kind == _BINARY
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

    def modelled(self) -> bool:
        """False for DELTA_BYTE_ARRAY strings without SizeStatistics bytes to trust."""
        return not (
            self.profile.nodes[-1].kind == _BINARY
            and self.size_statistics_bytes is None
            and detect_delta_byte_array(self.chunk)
        )

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
    leaf_profiles: Sequence[LeafProfile | None],
    leaf_indices: Iterable[int],
    size_stats: Sequence[LeafSizeStats | None] | None,
    dictionary_pages: Mapping[int, DictionaryPage] | None = None,
) -> list[LeafChunk]:
    """A :class:`LeafChunk` for each of ``leaf_indices`` in ``row_group`` this module
    models: fewer than asked for means a leaf needs the caller's fallback."""
    pages, stats = dictionary_pages or {}, size_stats or ()
    chunks = [
        LeafChunk(
            index=leaf_idx,
            profile=profile,
            chunk=row_group.column(leaf_idx),
            stats=stats[leaf_idx] if leaf_idx < len(stats) else None,
            num_rows=row_group.num_rows,
            dictionary_page=pages.get(leaf_idx),
        )
        for leaf_idx in leaf_indices
        if (profile := leaf_profiles[leaf_idx]) is not None
    ]
    return [leaf for leaf in chunks if leaf.modelled()]


# The estimate.
class Read(NamedTuple):
    """What a read task does besides decoding the file's columns."""

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
        size += estimate_leaf(leaf)
    if detect_added_columns(read):
        size += estimate_added_columns(row_group=row_group, read=read)
    return size


def estimate_leaf(leaf: LeafChunk) -> int:
    """Decoded bytes of one chunk's leaf array, shaved where they are a bound."""
    if leaf.profile.nodes[-1].kind == _BINARY:
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
    """An offset per slot, plus one, and the values' bytes: exact once they are."""
    offsets = leaf.profile.nodes[-1].width * (leaf.slots() + 1)
    return leaf.bitmap() + offsets + string_bytes


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
    if detect_dictionary_indexed(leaf.chunk):
        return estimate_dictionary_indexed(leaf), False
    fewest_values = leaf.fewest_values()
    return estimate_plain_bytes(chunk=leaf.chunk, fewest_values=fewest_values), True


# Any DELTA_BYTE_ARRAY page. Without SizeStatistics bytes to trust (none written,
# as by Spark 3.5, or PyArrow's) its strings aren't modelled: prefixes shared
# across sorted runs can decode to 76 times the chunk.
def detect_delta_byte_array(chunk: ColumnChunkMetaData) -> bool:
    return "DELTA_BYTE_ARRAY" in chunk.encodings


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
    arrow_schema: pa.Schema, pinned_schema: pa.Schema, parquet_schema: ParquetSchema
) -> tuple[_SchemaWalk, set[int]]:
    """The pinned walk, and the leaves of columns whose cast changes their shape:
    a struct that gained a field, a nested field made nullable, numbers cast to
    text. Those are walked as stored and left unmodelled."""
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
        if pinned is not field and not _cast_maps(
            walk=_walk(pa.schema([pinned])),
            columns=[parquet_schema.column(j) for j in leaves],
        ):
            unmapped.update(leaves)
            pinned = field
        fields.append(pinned)
        start = end
    return _walk(pa.schema(fields)), unmapped


def _cast_maps(walk: _SchemaWalk, columns: Sequence[ColumnSchema]) -> bool:
    """Whether a column's leaves follow its pinned walk one for one."""
    return len(walk.leaves) == len(columns) and all(
        _matches(leaf=chain[-1], rep=rep, column=column)
        for (chain, rep), column in zip(walk.leaves, columns)
    )


def _is_text(arrow_type: pa.DataType) -> bool:
    view = pa.types.is_string_view(arrow_type) or pa.types.is_binary_view(arrow_type)
    return detect_string_type(arrow_type) or view


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
