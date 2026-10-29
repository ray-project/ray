"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

Parquet's uncompressed size is still dictionary/RLE/bit-packed, so decoded size
runs from 0.87x of it for plain ``int64`` to 133x for a nullable
dictionary-encoded string column. A schema this module doesn't model gives
``None``, and the caller keeps uncompressed bytes times 5.

Variables, each a ``detect_*`` / ``estimate_*`` pair:

1. ``dictionary_columns`` read option: :func:`detect_dictionary_columns`.
2. Nulls: :meth:`LeafChunk.bitmap`.
3. ``binary_type`` / ``list_type`` read options: :func:`type_option_schema`.
4. A dictionary type in the file's type hints: :func:`detect_dictionary_type`.
5. A view type in the type hints: :func:`detect_view_type`.
6. Where a string leaf's bytes come from: :func:`estimate_string_bytes`.
7. Ray's schema pin: :func:`pinned_type`, :func:`detect_unmapped_casts` and
   :func:`detect_hidden_dictionary_slots`.
8. Columns Ray adds after decoding: :func:`detect_added_columns`.
9. An upper bound some column meets exactly, shaved: :func:`shave_bound`.
"""

from __future__ import annotations

import math
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
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
    # ``slot_def`` or more. ``scale`` passes 1 only below a fixed_size_list.
    slot_depth: int
    slot_def: int
    scale: int
    # Bytes per slot: a list's offset, a fixed-width leaf's value; else 0.
    width: int = 0
    # Has a bitmap even when null-free: a zero-copy read keeps the all-valid
    # one it decoded, and PyArrow's cast out of a dictionary writes one.
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
    # The column's leaves and :func:`cast_slot_bytes` when the pin casts it in
    # a way its leaves can't follow, which leaves them walked as stored (7b).
    unmapped_cast: Optional[Tuple[Tuple[int, ...], int]] = None
    # The pin casts the leaf out of a non-nullable dictionary below a nullable
    # struct, so the slots a null struct hides take a dictionary value (7c).
    hides_dictionary_slots: bool = False
    # The pin casts the leaf's values to text, which replaces them (7b).
    cast_to_text: bool = False


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

    def _node(self, kind: str, nullable: bool, def_level: int, pos: _Position, **kw):
        self._next_id += 1
        return _ArrowNode(
            self._next_id,
            kind,
            nullable,
            def_level,
            pos.slot_depth,
            pos.slot_def,
            pos.scale,
            **kw,
        )

    def visit(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        pos: _Position,
        chain: Tuple[_ArrowNode, ...],
    ) -> None:
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
                _STRUCT,
                nullable or (_is_repeated(arrow_type) and def_level > pos.slot_def),
                def_level,
                pos,
            )
            inner = pos._replace(parent_def=def_level)
            for field in arrow_type:
                self.visit(field.type, field.nullable, inner, chain + (node,))
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
                kind,
                nullable,
                def_level,
                pos,
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
            node = self._node(_FIXED_SIZE_LIST, nullable, def_level, pos)
            # Its children's length is counted at the list's own depth, times
            # ``list_size``.
            inner = pos._replace(
                depth=depth,
                parent_def=rep_def,
                scale=pos.scale * arrow_type.list_size,
            )
        else:
            width = 8 if pa.types.is_large_list(arrow_type) else 4
            node = self._node(_LIST, nullable, def_level, pos, width=width)
            inner = _Position(depth, rep_def, depth, rep_def, 1)
        self.visit(child_type, child_nullable, inner, chain + (node,))


def _walk(arrow_schema: pa.Schema) -> Optional[_SchemaWalk]:
    walk = _SchemaWalk()
    try:
        for field in arrow_schema:
            walk.visit(field.type, field.nullable, _Position(0, 0, 0, 0, 1), ())
    except _UnsupportedLayout:
        return None
    return walk


def build_leaf_profiles(
    parquet_schema: ParquetSchema,
    pinned_schema: Optional[pa.Schema] = None,
    format_kwargs: Optional[Mapping[str, Any]] = None,
    file_metadata: Optional[FileMetaData] = None,
) -> Optional[List[LeafProfile]]:
    """One :class:`LeafProfile` per leaf, in row group column order.

    Args:
        parquet_schema: The file's schema.
        pinned_schema: The schema the read casts every batch to, if any (7).
        format_kwargs: The read's ``ParquetFileFormat`` kwargs (3).
        file_metadata: The file's footer: its writer and stored Arrow schema.

    Returns:
        The profiles, or ``None`` when the schema holds a type or levels this
        module doesn't model.
    """
    try:
        arrow_schema = parquet_schema.to_arrow_schema()
    except (pa.ArrowException, ValueError, TypeError):
        return None
    stored = _walk(arrow_schema)
    if stored is None or len(stored.leaves) != len(parquet_schema):
        return None
    writer = "" if file_metadata is None else file_metadata.created_by or ""
    # Arrow C++ -- PyArrow, and the writers built on it -- leaves out of a
    # DELTA_BYTE_ARRAY chunk's count every value equal to, or a prefix of, the
    # one before it (``DeltaByteArrayEncoder::PutBinaryArray``), so 1,000
    # copies of an 8-byte string report 8 bytes; arrow-rs reports 8,000.
    counts_delta = writer != "" and not writer.startswith("parquet-cpp")
    if (
        pinned_schema is None
        and format_kwargs is not None
        and detect_binary_or_list_type(format_kwargs)
    ):
        stores = file_metadata is not None and b"ARROW:schema" in (
            file_metadata.metadata or {}
        )
        pinned_schema = type_option_schema(arrow_schema, format_kwargs, stores)
    walk, unmapped, to_text = stored, {}, set()
    if pinned_schema is not None:
        walk, unmapped, to_text = _pinned_walk(
            arrow_schema, pinned_schema, parquet_schema, stored, file_metadata
        )

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
        file_leaf = stored.leaves[leaf_idx][0][-1]
        from_dictionary = leaf.kind != _DICTIONARY and file_leaf.kind == _DICTIONARY
        if from_dictionary:
            # A dictionary the pin casts to plain values: the cast gives every
            # value a validity bit, even in a column that can't hold nulls.
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
        # A BYTE_ARRAY chunk's string bytes as its SizeStatistics record them,
        # but for a DELTA_BYTE_ARRAY count from a writer that leaves values out.
        self.size_statistics_bytes = (
            None if stats is None else stats.unencoded_byte_array_data_bytes
        )
        if detect_delta_byte_array(chunk) and not profile.counts_delta_byte_array:
            self.size_statistics_bytes = None
        # Histograms that disagree with the footer mean the walk drifted, or
        # the writer miscounted (PyArrow 24 with a ``write_batch_size`` above
        # the row group's rows), so the counts come from the rest of the footer.
        if stats is not None and not _histograms_agree(
            stats, chunk, num_rows, self._max_def, self._max_rep
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
        """Bytes of an array's validity bitmap, the leaf's by default (2)."""
        node = self.profile.nodes[-1] if node is None else node
        if not node.nullable or node.kind == _NULL:
            return 0
        # Unknown counts as present: undersizing a bin is what overfills a
        # read task.
        if not node.keeps_bitmap and self._nulls(node.slot_def, node.def_level) == 0:
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
            leaf_idx,
            leaf_profiles[leaf_idx],
            row_group.column(leaf_idx),
            size_stats[leaf_idx]
            if size_stats is not None and leaf_idx < len(size_stats)
            else None,
            row_group.num_rows,
            pages.get(leaf_idx),
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
    format_kwargs: Optional[Mapping[str, Any]] = None,
    added: Tuple[float, int] = (0, 0),
) -> Optional[int]:
    """Decoded Arrow bytes of one row group's read set, or ``None`` to fall back.

    Args:
        row_group: The row group to size.
        leaf_profiles: The file's :func:`build_leaf_profiles`.
        leaf_indices: The leaves the read decodes, or ``None`` for all.
        size_stats: This row group's ``SizeStatistics`` by leaf, or ``None``.
        dictionary_pages: Dictionary page headers by leaf index.
        format_kwargs: The read's ``ParquetFileFormat`` kwargs (3).
        added: :func:`added_bytes` of the columns the read appends (8).

    Returns:
        The bytes, or ``None`` when ``leaf_profiles`` is ``None`` or lacks a leaf.
    """
    indices = range(row_group.num_columns) if leaf_indices is None else leaf_indices
    if leaf_profiles is None or any(i >= len(leaf_profiles) for i in indices):
        return None
    leaves = leaf_chunks(
        row_group, leaf_profiles, indices, size_stats, dictionary_pages
    )
    return estimate_row_group(row_group, leaves, Read(format_kwargs or {}, *added))


class Read(NamedTuple):
    """What a read task does besides decoding the file's columns."""

    # The ``ParquetFileFormat`` kwargs it passes
    # (``parquet_file_reader.read_format_kwargs``), so an option the reader
    # drops never reaches here.
    format_kwargs: Mapping[str, Any]
    # Bytes per row of the columns Ray appends after decoding, and bytes once
    # per row group on top (:func:`added_bytes`).
    added_row_bytes: float = 0
    added_row_group_bytes: int = 0


def estimate_row_group(
    row_group: RowGroupMetaData, leaves: Sequence[LeafChunk], read: Read
) -> int:
    """Decoded bytes of ``leaves``, chunks of ``row_group``."""
    size = 0
    counted: Set[int] = set()
    for leaf in leaves:
        # Counted from the leaf's levels, so a bound wherever those are.
        ancestors = estimate_ancestors(leaf, counted)
        size += ancestors if leaf.levels_known() else shave_bound(ancestors)
        # The text a cast writes replaces the leaf: :func:`estimate_unmapped_casts`.
        if not leaf.profile.cast_to_text:
            size += estimate_leaf(leaf, read)
        if detect_hidden_dictionary_slots(leaf):
            size += estimate_hidden_dictionary_slots(leaf)
    if detect_unmapped_casts(leaves):
        size += shave_bound(estimate_unmapped_casts(leaves))
    if detect_added_columns(read):
        size += estimate_added_columns(row_group, read)
    return size


def estimate_leaf(leaf: LeafChunk, read: Read) -> int:
    """Decoded bytes of one chunk's leaf array, shaved where they are a bound (9)."""
    size, bound = _leaf_bytes(leaf, read)
    if bound or not leaf.levels_known():
        return shave_bound(size)
    return size


def _leaf_bytes(leaf: LeafChunk, read: Read) -> Tuple[int, bool]:
    """The leaf's bytes, and whether the branch that sized them is a bound."""
    arrow_type = leaf.type
    if detect_dictionary_columns(leaf.chunk, read.format_kwargs):
        return estimate_dictionary_columns(leaf), True
    if detect_dictionary_type(arrow_type):
        return estimate_dictionary(leaf, arrow_type), True
    if detect_view_type(arrow_type):
        return estimate_view_type(leaf), True
    if detect_string_type(arrow_type):
        return estimate_string_type(leaf), detect_plain_bytes(leaf)
    return estimate_fixed_width(leaf), False


# Fixed-width, boolean and null leaves, and the struct and list arrays above
# every leaf: exact once the counts are.


def estimate_fixed_width(leaf: LeafChunk) -> int:
    return _array_bytes(leaf.profile.nodes[-1], leaf)


def estimate_ancestors(leaf: LeafChunk, counted: Set[int]) -> int:
    """The arrays above ``leaf`` not yet in ``counted``; adds them to it."""
    size = 0
    for node in leaf.profile.nodes[:-1]:
        if node.node_id not in counted:
            counted.add(node.node_id)
            size += _array_bytes(node, leaf)
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


# 1. ``dictionary_columns``: leaves PyArrow reads as dictionaries.

# What PyArrow returns for them, whatever type the file's type hints give.
_DICTIONARY_COLUMNS_TYPE = pa.dictionary(pa.int32(), pa.binary())


def detect_dictionary_columns(
    chunk: ColumnChunkMetaData, format_kwargs: Mapping[str, Any]
) -> bool:
    # PyArrow applies the option to BYTE_ARRAY leaves only, matched by their
    # dotted Parquet path ("tags.list.element"); other leaves ignore it, and so
    # does every leaf under a ``binary_type``.
    return (
        chunk.physical_type == "BYTE_ARRAY"
        and chunk.path_in_schema in (format_kwargs.get("dictionary_columns") or ())
        and not _sets_binary_type(format_kwargs)
    )


# The most bytes :func:`estimate_dictionary_columns` gives a leaf's dictionary.
_DICTIONARY_COLUMNS_CAP = 256 * 1024 * 1024


def estimate_dictionary_columns(leaf: LeafChunk) -> int:
    """:func:`estimate_dictionary`, its dictionary capped at 256 MiB.

    Nothing in the footer counts distinct values, so PLAIN pages bound them; the cap
    keeps that bound from running into gigabytes.
    """
    size = estimate_dictionary(leaf, _DICTIONARY_COLUMNS_TYPE)
    indices = leaf.bitmap() + 4 * leaf.slots()
    return min(size, indices + _DICTIONARY_COLUMNS_CAP)


# 3. ``binary_type`` / ``list_type``: wider string, binary or list arrays. The
# leaf profiles are built for the types they give (:func:`type_option_schema`),
# so every pair sizes those.


def detect_binary_or_list_type(format_kwargs: Mapping[str, Any]) -> bool:
    return _sets_binary_type(format_kwargs) or format_kwargs.get("list_type") not in (
        None,
        pa.ListType,
    )


def _sets_binary_type(format_kwargs: Mapping[str, Any]) -> bool:
    return format_kwargs.get("binary_type") not in (None, pa.binary())


def type_option_schema(
    arrow_schema: pa.Schema,
    format_kwargs: Mapping[str, Any],
    stores_arrow_schema: bool,
) -> pa.Schema:
    """The types a read with ``binary_type`` or ``list_type`` returns.

    A file with a stored Arrow schema keeps its types but for dictionary text; one
    without, as Spark writes, takes the options' types throughout.
    """
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


# 4. A dictionary type in the file's type hints, e.g. a pandas categorical.
# Sized by :func:`estimate_dictionary`, with the hints' index and value types.


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


# 5. A view type in the type hints: ``string_view`` or ``binary_view``.


def detect_view_type(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string_view(arrow_type) or pa.types.is_binary_view(arrow_type)


def estimate_view_type(leaf: LeafChunk) -> int:
    """An upper bound: a 16-byte view per slot and every value's bytes.

    Exact once values pass the 12 bytes a view holds inline; 1.75x when all are 12.
    """
    return leaf.bitmap() + 16 * leaf.slots() + estimate_string_bytes(leaf)


# Plain strings, the case every string leaf not claimed above takes.


def detect_string_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_large_binary(arrow_type)
    )


def estimate_string_type(leaf: LeafChunk) -> int:
    """An offset per slot, plus one, and the values' bytes: exact once they are."""
    return (
        leaf.bitmap()
        + _offset_width(leaf.type) * (leaf.slots() + 1)
        + estimate_string_bytes(leaf)
    )


def _offset_width(arrow_type: pa.DataType) -> int:
    """A text value's bytes besides its characters: an offset, or a view."""
    if pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
        return 4
    if pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 8
    return 16


# 6. Where a string leaf's bytes come from.

_DICTIONARY_ENCODINGS = frozenset({"RLE_DICTIONARY", "PLAIN_DICTIONARY"})


def estimate_string_bytes(leaf: LeafChunk) -> int:
    """A BYTE_ARRAY chunk's value bytes, without their length prefixes."""
    # 6b. SizeStatistics written: the bytes, exactly. :class:`LeafChunk` drops
    # PyArrow's count for a DELTA_BYTE_ARRAY chunk, which leaves values out.
    if leaf.size_statistics_bytes is not None:
        return leaf.size_statistics_bytes
    if detect_delta_byte_array(leaf.chunk):
        return estimate_delta_byte_array(leaf.chunk)
    if detect_dictionary_indexed(leaf.chunk):
        return estimate_dictionary_indexed(leaf)
    return estimate_plain_bytes(leaf.chunk, leaf.fewest_values())


# 6a. Any DELTA_BYTE_ARRAY page, without SizeStatistics bytes to trust: none
# written, as by Spark 3.5, or PyArrow's.


def detect_delta_byte_array(chunk: ColumnChunkMetaData) -> bool:
    return "DELTA_BYTE_ARRAY" in chunk.encodings


# How many times its uncompressed bytes :func:`estimate_delta_byte_array` lets
# a chunk's values decode to.
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


# 6c. Data pages that index a dictionary page, without SizeStatistics bytes.


def detect_dictionary_indexed(chunk: ColumnChunkMetaData) -> bool:
    # A format 1.0 file names its dictionary page PLAIN_DICTIONARY too, so a
    # page no data page uses lands here as well.
    return chunk.has_dictionary_page and not _DICTIONARY_ENCODINGS.isdisjoint(
        chunk.encodings
    )


# How far past the dictionary's mean length :func:`estimate_dictionary_indexed`
# takes each value.
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
    return max(at_mean, estimate_plain_bytes(leaf.chunk, leaf.fewest_values()))


# 6d. Every value in PLAIN or DELTA_LENGTH_BYTE_ARRAY pages.


def estimate_plain_bytes(chunk: ColumnChunkMetaData, fewest_values: int) -> int:
    """An upper bound: those pages hold every value's bytes, uncompressed."""
    prefix = 0 if "DELTA_LENGTH_BYTE_ARRAY" in chunk.encodings else 4
    return max(chunk.total_uncompressed_size - prefix * fewest_values, 0)


def detect_plain_bytes(leaf: LeafChunk) -> bool:
    """Whether :func:`estimate_string_bytes` takes this bound (6d)."""
    return (
        leaf.size_statistics_bytes is None
        and not detect_delta_byte_array(leaf.chunk)
        and not detect_dictionary_indexed(leaf.chunk)
    )


# 7. Ray's schema pin: every batch is cast to the dataset schema, unless that
# holds an extension type (``readers.file_reader.casts_to_schema``).


def pinned_type(
    field: pa.Field, pinned_schema: Optional[pa.Schema]
) -> Optional[pa.DataType]:
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
    file_metadata: Optional[FileMetaData],
) -> Tuple[_SchemaWalk, Dict[int, Tuple[Tuple[int, ...], int]], Set[int]]:
    """The pinned walk, each leaf's unmapped cast (7b), and the leaves cast to text."""
    unmapped: Dict[int, Tuple[Tuple[int, ...], int]] = {}
    to_text: Set[int] = set()
    fields, start = [], 0
    for field in arrow_schema:
        field_walk = _walk(pa.schema([field]))
        # The whole schema walked, so each of its fields does.
        assert field_walk is not None
        end = start + len(field_walk.leaves)
        leaves = tuple(range(start, end))
        # An all-null column keeps its type: the read sizes the nulls the cast
        # fills it with as an added column (8).
        arrow_type = (
            None if pa.types.is_null(field.type) else pinned_type(field, pinned_schema)
        )
        pinned = field if arrow_type is None else field.with_type(arrow_type)
        if pinned is not field and not _cast_maps(
            pinned,
            [parquet_schema.column(j) for j in leaves],
            stored.leaves[start:end],
        ):
            slot_bytes = cast_slot_bytes(field.type, pinned.type, leaves, file_metadata)
            unmapped.update(dict.fromkeys(leaves, (leaves, slot_bytes)))
            texts = text_cast_leaves(field.type, pinned.type)
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
    file_leaves: Sequence[Tuple[Tuple[_ArrowNode, ...], int]],
) -> bool:
    """Whether a column's leaves follow the cast to ``pinned`` one for one."""
    walk = _walk(pa.schema([pinned]))
    if walk is None or len(walk.leaves) != len(columns):
        return False
    for (chain, rep), column, (file_chain, _) in zip(walk.leaves, columns, file_leaves):
        leaf = chain[-1]
        if (
            leaf.def_level != column.max_definition_level
            or rep != column.max_repetition_level
            or (leaf.kind in _TEXT_KINDS and column.physical_type != "BYTE_ARRAY")
            or (leaf.kind == _DICTIONARY and file_chain[-1].kind != _DICTIONARY)
        ):
            return False
    return True


# 7b. A pinned column whose leaves the cast doesn't keep one for one: a struct
# that gained a field, a nested field that became nullable, numbers cast to
# text, plain strings cast to a dictionary.


def detect_unmapped_casts(leaves: Sequence[LeafChunk]) -> bool:
    return any(leaf.profile.unmapped_cast for leaf in leaves)


def estimate_unmapped_casts(leaves: Sequence[LeafChunk]) -> int:
    """A crude upper bound on what the cast adds to such a column.

    No array of the pinned type is longer than the column's longest leaf, so that
    many :func:`cast_slot_bytes` bound it.
    """
    chosen = {leaf.index: leaf.chunk for leaf in leaves}
    size = 0
    casts = {
        leaf.profile.unmapped_cast
        for leaf in leaves
        if leaf.profile.unmapped_cast is not None
    }
    for cast_leaves, slot_bytes in casts:
        values = max(chosen[j].num_values for j in cast_leaves if j in chosen)
        size += values * slot_bytes
    return size


def cast_slot_bytes(
    file_type: pa.DataType,
    pinned_type: pa.DataType,
    leaves: Sequence[Optional[int]] = (),
    file_metadata: Optional[FileMetaData] = None,
) -> int:
    """Bytes per element of every array of ``pinned_type``, summed.

    Text the cast writes counts at its longest: from the leaf's min and max, else
    the type's (:func:`_text_bytes`).
    """
    texts = text_cast_leaves(file_type, pinned_type)
    if len(leaves) != len(texts) or file_metadata is None:
        leaves = (None,) * len(texts)
    text_bytes = max(
        (
            _cast_text_bytes(stored, leaf_idx, file_metadata)
            for (_, stored), text, leaf_idx in zip(
                _leaf_types(file_type), texts, leaves
            )
            if text
        ),
        default=0,
    )
    return _row_bytes(pinned_type, text_bytes)


def _cast_text_bytes(
    arrow_type: pa.DataType,
    leaf_idx: Optional[int],
    file_metadata: Optional[FileMetaData],
) -> int:
    if leaf_idx is not None and file_metadata is not None:
        chunks = [
            file_metadata.row_group(i).column(leaf_idx)
            for i in range(file_metadata.num_row_groups)
        ]
        recorded = statistics_text_bytes(arrow_type, chunks)
        if recorded is not None:
            return recorded
    return _text_bytes(arrow_type)


def statistics_text_bytes(
    arrow_type: pa.DataType, chunks: Sequence[ColumnChunkMetaData]
) -> Optional[int]:
    """The longest text an integer or decimal leaf casts to, or ``None``.

    From the min and max every chunk records: ids 0 to 49,999 write 5 bytes, not 20.
    """
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
) -> Tuple[bool, ...]:
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
    arrow_type: pa.DataType, path: Tuple[str, ...] = ()
) -> Iterator[Tuple[Tuple[str, ...], pa.DataType]]:
    """``(path, type)`` of each leaf by field names, ``[]`` for list elements."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_struct(arrow_type):
        for field in arrow_type:
            yield from _leaf_types(field.type, path + (field.name,))
    elif pa.types.is_map(arrow_type):
        yield from _leaf_types(arrow_type.key_type, path + ("[]", "key"))
        yield from _leaf_types(arrow_type.item_type, path + ("[]", "value"))
    elif _is_list(arrow_type):
        yield from _leaf_types(arrow_type.value_type, path + ("[]",))
    elif pa.types.is_dictionary(arrow_type):
        yield path, arrow_type.value_type
    else:
        yield path, arrow_type


def _row_bytes(arrow_type: pa.DataType, text_bytes: int) -> int:
    """Bytes per element of ``arrow_type``'s array and each array below it."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_null(arrow_type):
        return 0
    if pa.types.is_struct(arrow_type):
        return 1 + sum(_row_bytes(field.type, text_bytes) for field in arrow_type)
    if pa.types.is_fixed_size_list(arrow_type):
        return 1 + arrow_type.list_size * _row_bytes(arrow_type.value_type, text_bytes)
    if pa.types.is_map(arrow_type):
        entry = pa.struct([arrow_type.key_field, arrow_type.item_field])
        return 1 + 8 + _row_bytes(entry, text_bytes)
    if _is_list(arrow_type):
        return 1 + 8 + _row_bytes(arrow_type.value_type, text_bytes)
    if pa.types.is_dictionary(arrow_type):
        return (
            1
            + arrow_type.index_type.bit_width // 8
            + _row_bytes(arrow_type.value_type, text_bytes)
        )
    try:
        return 1 + math.ceil(arrow_type.bit_width / 8)
    except ValueError:
        return 1 + _offset_width(arrow_type) + text_bytes


# 7c. A dictionary the pin casts to plain text, in a non-nullable field of a
# nullable struct: a pandas categorical inside an optional struct, pinned to
# string. Each slot a null struct hides still holds index 0, and the cast copies
# that dictionary value into it.


def detect_hidden_dictionary_slots(leaf: LeafChunk) -> bool:
    return leaf.profile.hides_dictionary_slots and _hidden_slots(leaf) > 0


def estimate_hidden_dictionary_slots(leaf: LeafChunk) -> int:
    """A crude upper bound: no copied value is longer than the uncompressed chunk."""
    stored = leaf.size_statistics_bytes
    if stored is None:
        stored = leaf.chunk.total_uncompressed_size
    return _hidden_slots(leaf) * stored


def _hidden_slots(leaf: LeafChunk) -> int:
    """The leaf's slots no value fills, or more when the footer can't say."""
    return leaf.slots() - leaf.fewest_values()


# 8. Columns Ray adds after decoding: the file's path, Hive partition values, a
# row hash, a checkpoint ID, and every dataset column the file lacks, which the
# pin fills with nulls.


def detect_added_columns(read: Read) -> bool:
    return read.added_row_bytes > 0 or read.added_row_group_bytes > 0


def estimate_added_columns(row_group: RowGroupMetaData, read: Read) -> int:
    return math.ceil(row_group.num_rows * read.added_row_bytes) + (
        read.added_row_group_bytes
    )


# Bytes an added array takes once per row group: its bitmap's last byte and the
# offset past its last element.
_ADDED_ROW_GROUP_BYTES = 9


def added_bytes(added: Sequence[pa.Field], path: str) -> Tuple[float, int]:
    """Bytes per row of the ``added`` columns, and bytes once per row group.

    Only the path and the Hive values cut from it hold text: ``year=2024/`` gives
    ``year`` 4 bytes a row.
    """
    path_bytes = len(path.encode())
    partitions = dict(part.split("=", 1) for part in path.split("/") if "=" in part)
    per_row, per_row_group = 0.0, 0
    for field in added:
        if field.name == "path":
            text_bytes = path_bytes
        else:
            text_bytes = len(partitions.get(field.name, "").encode())
        row, once = _added_array_bytes(field.type, text_bytes, path_bytes)
        per_row += row
        per_row_group += once
    return per_row, per_row_group


def _added_array_bytes(
    arrow_type: pa.DataType, text_bytes: int, path_bytes: int
) -> Tuple[float, int]:
    """Bytes per element of an added array and its children, and once per row group."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_null(arrow_type):
        return 0, 0
    row, once = 1 / 8, _ADDED_ROW_GROUP_BYTES
    if pa.types.is_struct(arrow_type):
        for field in arrow_type:
            child_row, child_once = _added_array_bytes(
                field.type, text_bytes, path_bytes
            )
            row, once = row + child_row, once + child_once
    elif pa.types.is_fixed_size_list(arrow_type):
        child_row, child_once = _added_array_bytes(
            arrow_type.value_type, text_bytes, path_bytes
        )
        row, once = row + arrow_type.list_size * child_row, once + child_once
    elif pa.types.is_map(arrow_type) or _is_list(arrow_type):
        row += 8 if pa.types.is_large_list(arrow_type) else 4
        child = (
            pa.struct([arrow_type.key_field, arrow_type.item_field])
            if pa.types.is_map(arrow_type)
            else arrow_type.value_type
        )
        once += _added_array_bytes(child, 0, path_bytes)[1]
    elif pa.types.is_dictionary(arrow_type):
        row += arrow_type.index_type.bit_width / 8
        value_row, value_once = _added_array_bytes(
            arrow_type.value_type, path_bytes, path_bytes
        )
        once += math.ceil(value_row) + value_once
    elif _is_text(arrow_type):
        row += _offset_width(arrow_type) + text_bytes
    else:
        row += arrow_type.bit_width / 8
    return row, once


# 9. An upper bound that some column meets exactly -- no nulls, every value
# long, every value distinct -- comes out at 1.0 for that column and above it
# for the rest. Taking 0.95 of it puts its worst case at 0.95 times the bytes
# held and brings the rest nearer.

_BOUND_FACTOR = 0.95


def shave_bound(size: int) -> int:
    return math.ceil(_BOUND_FACTOR * size)
