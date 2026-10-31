"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

Per file, walk the Arrow schema into one chain of arrays per Parquet leaf: its
structs and lists, then the leaf itself. Per row group, read each array's length
and nulls from the footer (exact with SizeStatistics, a bound without), size it
as validity bitmap plus offsets plus values, and sum over the leaves read,
counting arrays that leaves share once. Unsupported schemas give ``None``.
"""

from __future__ import annotations

import math
from typing import (
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa
from pyarrow.parquet import (
    ColumnChunkMetaData,
    FileMetaData,
    ParquetSchema,
    RowGroupMetaData,
)

from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    DICTIONARY_PAGE_HEADER_READ_BYTES,
    DictionaryPage,
    LeafSizeStats,
)

# The schema walk: one chain of Arrow arrays per Parquet leaf.

# Array kinds. ``_LIST`` covers list, large_list and map, which carry one
# offsets buffer; a fixed_size_list has none, and its children hold
# ``list_size`` slots per parent slot instead.
_STRUCT = "struct"
_LIST = "list"
_FIXED_SIZE_LIST = "fixed_size_list"
_FIXED_WIDTH = "fixed_width"
_BOOLEAN = "boolean"
# Arrow's null type allocates no buffers at all.
_NULL = "null"
# The leaf kinds only BYTE_ARRAY values decode to.
_BINARY = "binary"
_DICTIONARY = "dictionary"
_VIEW = "view"
_TEXT_KINDS = (_BINARY, _DICTIONARY, _VIEW)

# PyArrow hands these leaf types to Arrow zero-copy from the decoder's buffers,
# keeping the validity bitmap it decoded for an optional leaf even when the
# leaf holds no null; every other type goes through a builder that drops an
# all-valid bitmap. INT96 timestamps are converted, not zero-copied, hence the
# physical-type check alongside the Arrow one. Measured on PyArrow 24.
_ZERO_COPY_PHYSICAL_TYPES = frozenset({"INT32", "INT64", "FLOAT", "DOUBLE"})


def _is_zero_copy_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_int32(arrow_type)
        or pa.types.is_int64(arrow_type)
        or pa.types.is_float32(arrow_type)
        or pa.types.is_float64(arrow_type)
        or pa.types.is_timestamp(arrow_type)
    )


class _ArrowNode(NamedTuple):
    """An Arrow array a leaf decodes into: the leaf, or an ancestor it shares."""

    node_id: int
    kind: str
    # A non-nullable array has no validity bitmap, even under a null parent --
    # except a struct whose every child is a list (see ``_SchemaWalk.visit``).
    nullable: bool
    # The definition level at which this array's slot is non-null.
    def_level: int
    # The array's length is ``scale`` times the level entries that open a slot
    # at repetition depth ``slot_depth``: those whose definition level is
    # ``slot_def`` or more. ``scale`` is above 1 only below a fixed_size_list.
    slot_depth: int
    slot_def: int
    scale: int
    # Bytes per slot: a list's offset, a fixed-width leaf's value; else 0.
    width: int = 0
    # Has a bitmap even when null-free: a zero-copy read keeps the all-valid
    # one it decoded.
    keeps_bitmap: bool = False
    # A leaf's Arrow type, an extension type's storage type; ``None`` above it.
    arrow_type: Optional[pa.DataType] = None


class LeafProfile(NamedTuple):
    """One leaf's schema facts, the same in every row group of the file."""

    # The leaf's Arrow arrays from its top-level column down, the leaf last.
    nodes: Tuple[_ArrowNode, ...]
    max_definition_level: int
    max_repetition_level: int
    # The writer counts every value of a DELTA_BYTE_ARRAY chunk in its
    # SizeStatistics bytes (see ``build_leaf_profiles``).
    counts_delta_byte_array: bool = False


class _UnsupportedLayout(Exception):
    """An Arrow type this module cannot size, which sends the file to fallback."""


class _Position(NamedTuple):
    """Where the schema walk is: the level context a child array inherits."""

    # Repeated ancestors so far, which is a leaf's max repetition level.
    depth: int
    # The definition level at which the parent is non-null.
    parent_def: int
    slot_depth: int
    slot_def: int
    scale: int


def _leaf_layout(arrow_type: pa.DataType) -> Tuple[str, int]:
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
        # The reader restores a dictionary type over string or binary values
        # only; a dictionary of anything else round-trips as its value type.
        if not detect_string_type(arrow_type.value_type):
            raise _UnsupportedLayout(arrow_type)
        return _DICTIONARY, 0
    try:
        bit_width = arrow_type.bit_width
    except ValueError:
        # Not fixed-width: anything else this module does not model.
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
        self.leaves: List[Tuple[Tuple[_ArrowNode, ...], int]] = []
        self._next_id = 0

    def _node(
        self,
        kind: str,
        nullable: bool,
        def_level: int,
        pos: _Position,
        width: int = 0,
        keeps_bitmap: bool = False,
        arrow_type: Optional[pa.DataType] = None,
    ) -> _ArrowNode:
        """A new array node at the walk's position ``pos``."""
        self._next_id += 1
        return _ArrowNode(
            node_id=self._next_id,
            kind=kind,
            nullable=nullable,
            def_level=def_level,
            slot_depth=pos.slot_depth,
            slot_def=pos.slot_def,
            scale=pos.scale,
            width=width,
            keeps_bitmap=keeps_bitmap,
            arrow_type=arrow_type,
        )

    def visit(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        pos: _Position,
        chain: Tuple[_ArrowNode, ...],
    ) -> None:
        """Record the chain of arrays down to each leaf under ``arrow_type``."""
        if isinstance(arrow_type, pa.BaseExtensionType):
            arrow_type = arrow_type.storage_type
        # An optional Parquet node adds one definition level, a required one none.
        def_level = pos.parent_def + int(nullable)

        if pa.types.is_struct(arrow_type):
            # PyArrow builds a struct's bitmap from a non-repeated child's
            # levels when it has one. Without one, it gives even a non-nullable
            # struct a bitmap carrying the nulls of its ancestors up to the
            # enclosing list, so such a struct is sized as nullable.
            node = self._node(
                kind=_STRUCT,
                nullable=nullable
                or (_is_repeated(arrow_type) and def_level > pos.slot_def),
                def_level=def_level,
                pos=pos,
            )
            inner = pos._replace(parent_def=def_level)
            for field in arrow_type:
                self.visit(
                    arrow_type=field.type,
                    nullable=field.nullable,
                    pos=inner,
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
                kind=kind,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                width=width,
                keeps_bitmap=_is_zero_copy_type(arrow_type),
                arrow_type=arrow_type,
            )
            self.leaves.append((chain + (leaf,), pos.depth))
            return

        # Parquet writes every list type as a repeated group, which adds a
        # definition level (the list is non-empty) and a repetition level.
        rep_def = def_level + 1
        depth = pos.depth + 1
        if pa.types.is_fixed_size_list(arrow_type):
            node = self._node(
                kind=_FIXED_SIZE_LIST, nullable=nullable, def_level=def_level, pos=pos
            )
            # Its children's length is counted at the list's own depth, times
            # ``list_size``.
            inner = pos._replace(
                depth=depth,
                parent_def=rep_def,
                scale=pos.scale * arrow_type.list_size,
            )
        else:
            width = 8 if pa.types.is_large_list(arrow_type) else 4
            node = self._node(
                kind=_LIST, nullable=nullable, def_level=def_level, pos=pos, width=width
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


def _walk(arrow_schema: pa.Schema) -> Optional[_SchemaWalk]:
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


def build_leaf_profiles(
    parquet_schema: ParquetSchema,
    file_metadata: Optional[FileMetaData] = None,
) -> Optional[List[LeafProfile]]:
    """One :class:`LeafProfile` per leaf, in row group column order.

    Args:
        parquet_schema: The file's schema.
        file_metadata: The file's footer, which names its writer.

    Returns:
        The profiles, or ``None`` when the schema holds a type or levels this
        module doesn't model.
    """
    try:
        arrow_schema = parquet_schema.to_arrow_schema()
    except (pa.ArrowException, ValueError, TypeError):
        return None
    walk = _walk(arrow_schema)
    if walk is None or len(walk.leaves) != len(parquet_schema):
        return None
    writer = "" if file_metadata is None else file_metadata.created_by or ""
    # Arrow C++ -- PyArrow, and the writers built on it -- leaves out of a
    # DELTA_BYTE_ARRAY chunk's count every value equal to, or a prefix of, the
    # one before it (``DeltaByteArrayEncoder::PutBinaryArray``), so 1,000
    # copies of an 8-byte string report 8 bytes; arrow-rs reports 8,000.
    counts_delta = writer != "" and not writer.startswith("parquet-cpp")

    profiles: List[LeafProfile] = []
    for leaf_idx, (chain, max_repetition_level) in enumerate(walk.leaves):
        column = parquet_schema.column(leaf_idx)
        leaf = chain[-1]
        if (
            leaf.def_level != column.max_definition_level
            or max_repetition_level != column.max_repetition_level
        ):
            return None
        if leaf.kind in _TEXT_KINDS and column.physical_type != "BYTE_ARRAY":
            return None
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
    profile: LeafProfile,
    chunk: ColumnChunkMetaData,
    stats: Optional[LeafSizeStats],
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
    """One leaf's column chunk in one row group, and the counts its footer gives.

    Exact from the ``SizeStatistics`` level histograms; without them, bounds from
    the footer's level and null counts.
    """

    def __init__(
        self,
        index: int,
        profile: LeafProfile,
        chunk: ColumnChunkMetaData,
        stats: Optional[LeafSizeStats],
        num_rows: int,
        dictionary_page: Optional[DictionaryPage] = None,
    ):
        self.index = index
        self.profile = profile
        self.chunk = chunk
        self.num_rows = num_rows
        # Its header, for the chunks :func:`dictionary_page_read_size` names.
        self.dictionary_page = dictionary_page
        self._max_def = profile.max_definition_level
        self._max_rep = profile.max_repetition_level
        # Below a struct, list or map.
        self._nested = len(profile.nodes) > 1
        # The string bytes SizeStatistics records, or ``None`` where the writer
        # undercounts DELTA_BYTE_ARRAY.
        self.size_statistics_bytes = (
            None if stats is None else stats.unencoded_byte_array_data_bytes
        )
        if detect_delta_byte_array(chunk) and not profile.counts_delta_byte_array:
            self.size_statistics_bytes = None
        # Histograms that disagree with the footer mean the walk drifted, or
        # the writer miscounted (PyArrow 24 with a ``write_batch_size`` above
        # the row group's rows), so the counts come from the rest of the footer.
        if stats is not None and not _histograms_agree(
            stats=stats,
            chunk=chunk,
            num_rows=num_rows,
            max_def=self._max_def,
            max_rep=self._max_rep,
        ):
            stats = None
        self._repetition = () if stats is None else stats.repetition_level_histogram
        self._definition = () if stats is None else stats.definition_level_histogram
        # The ``Statistics`` null count, entries below the max definition
        # level, looked up only when no definition histogram counts them:
        # building a ``Statistics`` costs about two thirds of a leaf's estimate.
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

    def slots(self, node: Optional[_ArrowNode] = None) -> int:
        """An array's length, the leaf's by default; a bound when a count is unknown."""
        node = self.profile.nodes[-1] if node is None else node
        if node.slot_depth == 0:
            return self.num_rows * node.scale
        opened = self._opened(node.slot_depth) - (self._below(node.slot_def) or 0)
        return opened * node.scale

    def bitmap(self, node: Optional[_ArrowNode] = None) -> int:
        """Bytes of an array's validity bitmap, the leaf's by default."""
        node = self.profile.nodes[-1] if node is None else node
        if not node.nullable or node.kind == _NULL:
            return 0
        # Unknown counts as present: undersizing a bin is what overfills a
        # read task.
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

    def dictionary_mean_length(self) -> Optional[float]:
        """Mean bytes of a dictionary entry's value, or ``None`` without the header."""
        page = self.dictionary_page
        if page is None:
            return None
        entries = page.num_values
        value_bytes = max(page.uncompressed_page_size - 4 * entries, 0)
        return value_bytes / max(entries, 1)

    def _opened(self, depth: int) -> int:
        """Entries at repetition level ``depth`` or less, or an upper bound."""
        if self._repetition:
            return sum(self._repetition[: depth + 1])
        # At the max repetition level that is every entry, and short of it no
        # more than every entry.
        return self.chunk.num_values

    def _below(self, def_level: int) -> Optional[int]:
        """Entries whose definition level is under ``def_level``, or ``None``.

        Without a definition histogram the null count stands in, and can run low inside
        a list.
        """
        if self._definition:
            return sum(self._definition[:def_level])
        if def_level <= 0 or self._null_count == 0:
            return 0
        if def_level >= self._max_def:
            return self._null_count
        return None

    def _nulls(self, slot_def: int, def_level: int) -> Optional[int]:
        """Slots that exist but are null, or ``None``.

        Their level is at least ``slot_def`` and under ``def_level``.
        """
        if self._nested and not self._definition:
            # PyArrow writes a null count of 0 for some nested leaves whose
            # ancestors hold nulls -- one written from an Arrow dictionary, or
            # one with a nullable struct between it and its list -- so below
            # the top level only the definition histogram can rule nulls out.
            return None
        below = self._below(def_level)
        if below == 0:
            return 0
        above = self._below(slot_def)
        if below is None or above is None:
            return None
        return below - above


def _histograms_agree(
    stats: LeafSizeStats,
    chunk: ColumnChunkMetaData,
    num_rows: int,
    max_def: int,
    max_rep: int,
) -> bool:
    """Whether the histograms the writer recorded fit the footer's own counts.

    PyArrow 24 sometimes counts more rows than the row group holds.
    """
    repetition = stats.repetition_level_histogram
    definition = stats.definition_level_histogram
    if repetition and (
        len(repetition) != max_rep + 1
        or repetition[0] != num_rows
        or sum(repetition) != chunk.num_values
    ):
        return False
    return not definition or (
        len(definition) == max_def + 1 and sum(definition) == chunk.num_values
    )


def _bitmap_size(length: int) -> int:
    return (length + 7) // 8


def leaf_chunks(
    row_group: RowGroupMetaData,
    leaf_profiles: Sequence[LeafProfile],
    leaf_indices: Iterable[int],
    size_stats: Optional[Sequence[Optional[LeafSizeStats]]],
    dictionary_pages: Optional[Mapping[int, DictionaryPage]] = None,
) -> List[LeafChunk]:
    """A :class:`LeafChunk` for each of ``leaf_indices`` in ``row_group``."""
    pages = dictionary_pages or {}
    return [
        LeafChunk(
            index=leaf_idx,
            profile=leaf_profiles[leaf_idx],
            chunk=row_group.column(leaf_idx),
            stats=size_stats[leaf_idx]
            if size_stats is not None and leaf_idx < len(size_stats)
            else None,
            num_rows=row_group.num_rows,
            dictionary_page=pages.get(leaf_idx),
        )
        for leaf_idx in leaf_indices
    ]


# The estimate.


def estimate_row_group_decoded_size(
    row_group: RowGroupMetaData,
    leaf_profiles: Optional[List[LeafProfile]],
    leaf_indices: Optional[Sequence[int]],
    size_stats: Optional[List[Optional[LeafSizeStats]]],
    dictionary_pages: Optional[Mapping[int, DictionaryPage]] = None,
) -> Optional[int]:
    """Decoded Arrow bytes of one row group's read set, or ``None`` to fall back.

    Args:
        row_group: The row group to size.
        leaf_profiles: The file's :func:`build_leaf_profiles`.
        leaf_indices: The leaves the read decodes, or ``None`` for all.
        size_stats: This row group's ``SizeStatistics`` by leaf, or ``None``.
        dictionary_pages: Dictionary page headers by leaf index.

    Returns:
        The bytes, or ``None`` when ``leaf_profiles`` is ``None`` or lacks a leaf.
    """
    indices = range(row_group.num_columns) if leaf_indices is None else leaf_indices
    if leaf_profiles is None or any(i >= len(leaf_profiles) for i in indices):
        return None
    leaves = leaf_chunks(
        row_group=row_group,
        leaf_profiles=leaf_profiles,
        leaf_indices=indices,
        size_stats=size_stats,
        dictionary_pages=dictionary_pages,
    )
    return estimate_row_group(leaves)


def estimate_row_group(leaves: Sequence[LeafChunk]) -> int:
    """Decoded bytes of ``leaves``, chunks of one row group."""
    size = 0
    counted: Set[int] = set()
    for leaf in leaves:
        # Counted from the leaf's levels, so a bound wherever those are.
        ancestors = estimate_ancestors(leaf=leaf, counted=counted)
        size += ancestors if leaf.levels_known() else shave_bound(ancestors)
        size += estimate_leaf(leaf)
    return size


def estimate_leaf(leaf: LeafChunk) -> int:
    """Decoded bytes of one chunk's leaf array, shaved where they are a bound."""
    size, bound = _leaf_bytes(leaf)
    if bound or not leaf.levels_known():
        return shave_bound(size)
    return size


def _leaf_bytes(leaf: LeafChunk) -> Tuple[int, bool]:
    """The leaf's bytes, and whether the branch that sized them is a bound."""
    arrow_type = leaf.type
    if detect_dictionary_type(arrow_type):
        return estimate_dictionary(leaf=leaf, arrow_type=arrow_type), True
    if detect_view_type(arrow_type):
        return estimate_view_type(leaf), True
    if detect_string_type(arrow_type):
        string_bytes, bound = _string_bytes(leaf)
        return estimate_string_type(leaf=leaf, string_bytes=string_bytes), bound
    return estimate_fixed_width(leaf), False


# Fixed-width, boolean and null leaves, and the struct and list arrays above
# every leaf: exact once the counts are.


def estimate_fixed_width(leaf: LeafChunk) -> int:
    """Decoded bytes of a fixed-width, boolean or null leaf."""
    return _array_bytes(node=leaf.profile.nodes[-1], leaf=leaf)


def estimate_ancestors(leaf: LeafChunk, counted: Set[int]) -> int:
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


# A dictionary type in the file's type hints, e.g. a pandas categorical.


def detect_dictionary_type(arrow_type: pa.DataType) -> bool:
    return pa.types.is_dictionary(arrow_type)


def estimate_dictionary(leaf: LeafChunk, arrow_type: pa.DictionaryType) -> int:
    """An upper bound for a leaf PyArrow reads as a dictionary.

    The chunk's uncompressed pages hold every distinct value behind a 4-byte length,
    so they bound the dictionary's offsets and values.
    """
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
    """An upper bound: a 16-byte view per slot and every value's bytes.

    Exact once values pass the 12 bytes a view holds inline; 1.75x when all are 12.
    """
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


# Where a string leaf's bytes come from.

_DICTIONARY_ENCODINGS = frozenset({"RLE_DICTIONARY", "PLAIN_DICTIONARY"})


def estimate_string_bytes(leaf: LeafChunk) -> int:
    """A BYTE_ARRAY chunk's value bytes, without their length prefixes."""
    return _string_bytes(leaf)[0]


def _string_bytes(leaf: LeafChunk) -> Tuple[int, bool]:
    """The value bytes, and whether they are the upper bound from plain pages."""
    # SizeStatistics written: the bytes, exactly. :class:`LeafChunk` drops
    # PyArrow's count for a DELTA_BYTE_ARRAY chunk, which leaves values out.
    if leaf.size_statistics_bytes is not None:
        return leaf.size_statistics_bytes, False
    if detect_delta_byte_array(leaf.chunk):
        return estimate_delta_byte_array(leaf.chunk), False
    if detect_dictionary_indexed(leaf.chunk):
        return estimate_dictionary_indexed(leaf), False
    fewest_values = leaf.fewest_values()
    return estimate_plain_bytes(chunk=leaf.chunk, fewest_values=fewest_values), True


# Any DELTA_BYTE_ARRAY page, without SizeStatistics bytes to trust: none
# written, as by Spark 3.5, or PyArrow's.


def detect_delta_byte_array(chunk: ColumnChunkMetaData) -> bool:
    return "DELTA_BYTE_ARRAY" in chunk.encodings


_DELTA_BYTE_ARRAY_EXPANSION = 128


def estimate_delta_byte_array(chunk: ColumnChunkMetaData) -> int:
    """A guess: the values at 128 times the chunk's uncompressed bytes.

    Each value is stored as its suffix after the one before, so it goes under for
    long sorted runs: a million rows of 2 KB URLs, each repeated 1,000 times.
    """
    return (
        min(chunk.num_values, _DELTA_BYTE_ARRAY_EXPANSION)
        * chunk.total_uncompressed_size
    )


# Data pages that index a dictionary page, without SizeStatistics bytes.


def detect_dictionary_indexed(chunk: ColumnChunkMetaData) -> bool:
    """Whether the chunk's data pages index a dictionary page."""
    # A format 1.0 file names its dictionary page PLAIN_DICTIONARY too, so a
    # page no data page uses lands here as well.
    return chunk.has_dictionary_page and not _DICTIONARY_ENCODINGS.isdisjoint(
        chunk.encodings
    )


_DICTIONARY_MEAN_FACTOR = 1.2


def estimate_dictionary_indexed(leaf: LeafChunk) -> int:
    """A guess: every value at 1.2 times the dictionary's mean length.

    It goes under when the common values are longer: "COMPLETED_SUCCESSFULLY" in
    97% of rows beside "OK" comes out at half. Without the header, a crude bound.
    """
    mean = leaf.dictionary_mean_length()
    if mean is None:
        return (leaf.slots() + 1) * leaf.chunk.total_uncompressed_size
    at_mean = math.ceil(_DICTIONARY_MEAN_FACTOR * mean * leaf.values())
    plain = estimate_plain_bytes(chunk=leaf.chunk, fewest_values=leaf.fewest_values())
    return max(at_mean, plain)


# Every value in PLAIN or DELTA_LENGTH_BYTE_ARRAY pages.


def estimate_plain_bytes(chunk: ColumnChunkMetaData, fewest_values: int) -> int:
    """An upper bound: those pages hold every value's bytes, uncompressed."""
    prefix = 0 if "DELTA_LENGTH_BYTE_ARRAY" in chunk.encodings else 4
    return max(chunk.total_uncompressed_size - prefix * fewest_values, 0)


_BOUND_FACTOR = 0.95


def shave_bound(size: int) -> int:
    """Scale an upper bound by 0.95; a column that meets it exactly lands at 0.95."""
    return math.ceil(_BOUND_FACTOR * size)
