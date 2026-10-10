"""Estimate the Arrow decoded size of a Parquet row group from its footer.

``total_uncompressed_size`` is Parquet's *encoded* size: decompressed, but still
dictionary/RLE/bit-packed. It is a poor proxy for the Arrow block a read task
materializes -- measured decoded/uncompressed ratios span 0.87x for plain
``int64`` to 133x for a nullable dictionary-encoded string column. Sizing read
tasks on it (whether raw or scaled by a fixed ratio) therefore either floods the
scheduler with tiny tasks or overfills blocks to the point of OOM.

The estimate sums the buffers of the Arrow arrays a read decodes to: one array
per leaf, plus one per struct, list and map above it. Each buffer is an array's
length times a per-slot width, and every input comes from the footer:

* The Arrow layout -- ``string`` vs ``large_string`` offsets, dictionary types,
  ``fixed_size_list``, extension storage -- comes from
  ``ParquetSchema.to_arrow_schema()``, the conversion the reader itself runs, so
  it honors the ``ARROW:schema`` a PyArrow writer embeds.
* Array lengths and null counts come from the level histograms in
  ``SizeStatistics``. A Parquet column stores one (repetition, definition) level
  pair per entry, and the histograms count entries per level, which is enough to
  recover how many slots each nested array has and whether any of them is null.
* The character bytes of ``BYTE_ARRAY`` leaves come from
  ``unencoded_byte_array_data_bytes``.

Two inputs are not in the footer, and both live in a chunk's dictionary page.
An Arrow dictionary column's decoded dictionary is that page, whose size the
footer folds into the whole chunk's. A view column's size turns on how many of
its bytes sit in values too long to fit inside their views, which the footer's
byte total cannot say and the page's value lengths mostly can. The caller may
read the page -- just its header for a dictionary column, all of it for a view
column -- and pass it in (:func:`dictionary_page_read_size` says which chunks
are worth it and how many bytes); without it both fall back to a bound.

One case needs the whole chunk. When the writer gives up on the dictionary
partway through a chunk, the Arrow dictionary a read builds is the page's values
plus every new value in the PLAIN pages after it, and only the values can say
which are new. :func:`dictionary_decode_size` names those chunks; the caller may
decode them and pass the dictionary's bytes in on the page.

A chunk without ``SizeStatistics`` -- from a writer that predates them, PyArrow
17 among them -- is sized by footer rules instead. Array lengths and null
counts come from the chunk's level-entry count and its ``Statistics`` null
count: exact outside any list, an upper bound inside one. A ``BYTE_ARRAY``
leaf's bytes come from its uncompressed size less each value's length prefix,
or, where the data pages hold dictionary indices, from the dictionary page's
mean value length -- exact for values of one length, approximate otherwise
(:func:`_footer_byte_array_bytes`).

:mod:`.parquet_size_statistics` recovers ``SizeStatistics`` from the footer
bytes. A leaf the footer cannot size even so -- DELTA_BYTE_ARRAY values, or a
dictionary page that could not be read -- keeps the pre-existing estimate, its
uncompressed size times a fixed ratio, without taking the rest of its row
group with it. ``None`` is left for a schema this module does not model, the
caller's signal to keep that estimate for the whole file.
"""

from __future__ import annotations

import math
from typing import Iterable, List, Mapping, NamedTuple, Optional, Sequence, Tuple

import pyarrow as pa
from pyarrow.parquet import ColumnChunkMetaData, ParquetSchema, RowGroupMetaData

from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
    DICTIONARY_PAGE_HEADER_READ_BYTES,
    DictionaryPage,
    LeafSizeStats,
)

# The V2 copy, which is what the reader's own fallback sizing uses
# (parquet_file_reader.py); importing the same definition keeps this module's
# fallback and the reader's from ever diverging.
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)

# Array kinds. ``_LIST`` covers list, large_list and map, which all carry one
# offsets buffer; a fixed_size_list has none, and its children hold ``list_size``
# slots per parent slot instead.
_STRUCT = "struct"
_LIST = "list"
_FIXED_SIZE_LIST = "fixed_size_list"
_FIXED_WIDTH = "fixed_width"
_BOOLEAN = "boolean"
_BINARY = "binary"
_DICTIONARY = "dictionary"
# string_view / binary_view: a 16-byte view per slot, plus the bytes of every
# value too long to fit inside its view.
_VIEW = "view"
_VIEW_WIDTH = 16
# The longest value a view holds inline, in the 12 bytes after its length.
_VIEW_INLINE_BYTES = 12
# Arrow's null type allocates no buffers at all.
_NULL = "null"

# Offsets buffers hold one more entry than there are slots, so the trailing end
# offset is included. The large_* variants use 64-bit offsets.
_OFFSET_WIDTH = 4
_LARGE_OFFSET_WIDTH = 8
# A dictionary leaf with no slots in a row group -- every enclosing list empty or
# null -- decodes to 8 bytes whatever its index width, value type or dictionary
# page: the reader never reads that page, leaving a 4-byte indices buffer and an
# empty dictionary whose offsets and data share one 4-byte allocation. Measured
# on PyArrow 24.
_EMPTY_DICTIONARY_BYTES = 8

_BYTE_ARRAY = "BYTE_ARRAY"
_DELTA_BYTE_ARRAY = "DELTA_BYTE_ARRAY"
_DELTA_LENGTH_BYTE_ARRAY = "DELTA_LENGTH_BYTE_ARRAY"
_RLE_DICTIONARY = "RLE_DICTIONARY"
_PLAIN_DICTIONARY = "PLAIN_DICTIONARY"
# The same two as ``enum Encoding`` values, which is how ``encoding_stats``
# carries them.
_DICTIONARY_ENCODING_IDS = frozenset({2, 8})

# PyArrow hands these leaf types to Arrow zero-copy from the decoder's buffers,
# and that path keeps the validity bitmap the decoder allocated for an optional
# leaf even when the leaf holds no null. Every other type is rebuilt through a
# builder that drops an all-valid bitmap. INT96 timestamps are converted rather
# than zero-copied, hence the physical-type check alongside the Arrow one.
# Measured on PyArrow 24.
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
    """One Arrow array a leaf column decodes into: the leaf or an ancestor.

    An ancestor shared by several leaves is the same node in each of their
    chains, so the estimator counts a struct's validity bitmap or a list's
    offsets once, however many of its leaves the read set includes.
    """

    node_id: int
    kind: str
    # A non-nullable array never carries a validity bitmap, even when a parent
    # is null.
    nullable: bool
    # The definition level at which this array's slot is non-null.
    def_level: int
    # The array's length is ``scale`` times the number of level entries that
    # open a slot at repetition depth ``slot_depth``, the ones whose definition
    # level is ``slot_def`` or more. ``scale`` exceeds 1 only below a
    # fixed_size_list.
    slot_depth: int
    slot_def: int
    scale: int
    # Offsets width of a list or variable-width leaf (of a dictionary's values),
    # else 0.
    offset_width: int = 0
    # Bytes per value of a fixed-width leaf; index width of a dictionary.
    value_width: int = 0
    # Keeps an all-valid bitmap when null-free; see _ZERO_COPY_PHYSICAL_TYPES.
    zero_copy: bool = False


class LeafProfile(NamedTuple):
    """The per-leaf schema facts the estimator consumes, all constant per file.

    Hoisted out of the per-row-group loop because deriving them walks the Arrow
    schema, and the estimator runs once per row group.
    """

    # The leaf's Arrow arrays from its top-level column down, the leaf last.
    nodes: Tuple[_ArrowNode, ...]
    max_definition_level: int
    max_repetition_level: int


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


def _leaf_layout(arrow_type: pa.DataType) -> Tuple[str, int, int]:
    """``(kind, offset_width, value_width)`` of a leaf's Arrow type."""
    if pa.types.is_null(arrow_type):
        return _NULL, 0, 0
    if pa.types.is_boolean(arrow_type):
        return _BOOLEAN, 0, 0
    if pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
        return _BINARY, _OFFSET_WIDTH, 0
    if pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return _BINARY, _LARGE_OFFSET_WIDTH, 0
    if pa.types.is_string_view(arrow_type) or pa.types.is_binary_view(arrow_type):
        return _VIEW, 0, _VIEW_WIDTH
    if pa.types.is_dictionary(arrow_type):
        # The reader restores a dictionary type only over BYTE_ARRAY values; a
        # dictionary of anything else round-trips as its value type.
        kind, offset_width, _ = _leaf_layout(arrow_type.value_type)
        if kind != _BINARY:
            raise _UnsupportedLayout(arrow_type)
        return _DICTIONARY, offset_width, arrow_type.index_type.bit_width // 8
    try:
        bit_width = arrow_type.bit_width
    except ValueError:
        # Not fixed-width: anything else this module does not model.
        raise _UnsupportedLayout(arrow_type) from None
    if bit_width % 8:
        raise _UnsupportedLayout(arrow_type)
    return _FIXED_WIDTH, 0, bit_width // 8


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
            node = self._node(_STRUCT, nullable, def_level, pos)
            inner = pos._replace(parent_def=def_level)
            for field in arrow_type:
                self.visit(field.type, field.nullable, inner, chain + (node,))
            return

        if pa.types.is_map(arrow_type):
            # A map is a list of non-nullable key/value structs.
            child_type = pa.struct([arrow_type.key_field, arrow_type.item_field])
            child_nullable = False
        elif (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            child_type = arrow_type.value_type
            child_nullable = arrow_type.value_field.nullable
        else:
            kind, offset_width, value_width = _leaf_layout(arrow_type)
            leaf = self._node(
                kind,
                nullable,
                def_level,
                pos,
                offset_width=offset_width,
                value_width=value_width,
                zero_copy=_is_zero_copy_type(arrow_type),
            )
            self.leaves.append((chain + (leaf,), pos.depth))
            return

        # Parquet writes every list type as a repeated group, which adds a
        # definition level (the list is non-empty) and a repetition level.
        rep_def = def_level + 1
        depth = pos.depth + 1
        if pa.types.is_fixed_size_list(arrow_type):
            node = self._node(_FIXED_SIZE_LIST, nullable, def_level, pos)
            # Arrow gives a fixed_size_list ``list_size`` child slots per slot,
            # so its children's length is counted at the list's own depth.
            inner = pos._replace(
                depth=depth,
                parent_def=rep_def,
                scale=pos.scale * arrow_type.list_size,
            )
        else:
            offset_width = (
                _LARGE_OFFSET_WIDTH
                if pa.types.is_large_list(arrow_type)
                else _OFFSET_WIDTH
            )
            node = self._node(
                _LIST, nullable, def_level, pos, offset_width=offset_width
            )
            inner = _Position(depth, rep_def, depth, rep_def, 1)
        self.visit(child_type, child_nullable, inner, chain + (node,))


def build_leaf_profiles(parquet_schema: ParquetSchema) -> Optional[List[LeafProfile]]:
    """One :class:`LeafProfile` per leaf, in schema (== row group) column order.

    Called once per file; the result feeds every row group's
    :func:`estimate_row_group_decoded_size` call. ``None`` when the schema holds
    a type this module does not model, or when the levels the Arrow walk derives
    disagree with the Parquet schema's -- either way nothing derived from the
    walk is trusted, and the file falls back.
    """
    try:
        arrow_schema = parquet_schema.to_arrow_schema()
    except (pa.ArrowException, ValueError, TypeError):
        return None
    walk = _SchemaWalk()
    try:
        for field in arrow_schema:
            walk.visit(field.type, field.nullable, _Position(0, 0, 0, 0, 1), ())
    except _UnsupportedLayout:
        return None
    if len(walk.leaves) != len(parquet_schema):
        return None

    profiles: List[LeafProfile] = []
    for leaf_idx, (chain, max_repetition_level) in enumerate(walk.leaves):
        column = parquet_schema.column(leaf_idx)
        leaf = chain[-1]
        if (
            leaf.def_level != column.max_definition_level
            or max_repetition_level != column.max_repetition_level
        ):
            return None
        physical_type = column.physical_type
        if leaf.kind in (_BINARY, _DICTIONARY, _VIEW) and physical_type != _BYTE_ARRAY:
            return None
        if leaf.zero_copy and physical_type not in _ZERO_COPY_PHYSICAL_TYPES:
            chain = chain[:-1] + (leaf._replace(zero_copy=False),)
        profiles.append(
            LeafProfile(
                nodes=chain,
                max_definition_level=column.max_definition_level,
                max_repetition_level=max_repetition_level,
            )
        )
    return profiles


# A dictionary page is capped at 1 MiB by default in parquet-cpp, parquet-mr and
# arrow-rs, and runs past the cap by at most one write batch. A view chunk whose
# page spans more than this is left at its bound rather than fetched.
_DICTIONARY_PAGE_READ_LIMIT = 2 * 1024 * 1024


def dictionary_page_read_size(
    profile: LeafProfile,
    chunk: ColumnChunkMetaData,
    stats: Optional[LeafSizeStats],
) -> int:
    """Bytes at ``chunk.dictionary_page_offset`` that
    :func:`estimate_row_group_decoded_size` wants read, or 0 for none.

    An Arrow dictionary leaf wants its dictionary page's header. A view leaf
    wants the whole page, header and values, when some data page indexes it:
    the values' lengths say how many bytes fall outside their views (see
    :func:`_view_out_of_line_bytes`). The page spans up to the first data page.
    Any other ``BYTE_ARRAY`` leaf whose footer lacks its character bytes wants
    the header too, which :func:`_footer_byte_array_bytes` sizes them from.
    Every other chunk is sized from the footer alone.
    """
    if not chunk.has_dictionary_page:
        return 0
    kind = profile.nodes[-1].kind
    if kind == _DICTIONARY:
        return DICTIONARY_PAGE_HEADER_READ_BYTES
    encodings = None if stats is None else stats.data_page_encodings
    if kind == _VIEW and encodings and encodings & _DICTIONARY_ENCODING_IDS:
        page_bytes = chunk.data_page_offset - chunk.dictionary_page_offset
        if 0 < page_bytes <= _DICTIONARY_PAGE_READ_LIMIT:
            return page_bytes
    if (
        kind in (_BINARY, _VIEW)
        and (stats is None or stats.unencoded_byte_array_data_bytes is None)
        and _DELTA_BYTE_ARRAY not in chunk.encodings
    ):
        return DICTIONARY_PAGE_HEADER_READ_BYTES
    return 0


# A PyArrow-written row group holds at most 1Mi rows by default, which for a
# categorical column of values up to 100 bytes compresses to under 9 MiB. A chunk
# past this keeps its bound rather than be decoded at planning time.
_DICTIONARY_DECODE_LIMIT = 16 * 1024 * 1024


def dictionary_decode_size(
    profile: LeafProfile,
    chunk: ColumnChunkMetaData,
    stats: Optional[LeafSizeStats],
) -> int:
    """Compressed bytes of a chunk :func:`estimate_row_group_decoded_size` wants
    decoded whole, or 0 for none.

    That is an Arrow dictionary leaf whose writer gave up on the dictionary
    partway: some data pages index the dictionary page and the rest hold their
    values. A read decodes it to the page's values plus each new value in those
    other pages, which the footer cannot tell from the repeats, so the bound can
    run 50x over. PyArrow writes this when an Arrow column's chunks carry
    different dictionaries -- ``pq.write_table`` of concatenated or compacted
    tables, though not Ray Data's ``write_parquet``. A chunk over
    :data:`_DICTIONARY_DECODE_LIMIT` keeps the bound.
    """
    if (
        not chunk.has_dictionary_page
        or profile.nodes[-1].kind != _DICTIONARY
        or stats is None
        or not stats.data_page_encodings
    ):
        return 0
    encodings = stats.data_page_encodings
    if not (
        encodings & _DICTIONARY_ENCODING_IDS and encodings - _DICTIONARY_ENCODING_IDS
    ):
        return 0
    compressed = chunk.total_compressed_size
    return compressed if 0 < compressed <= _DICTIONARY_DECODE_LIMIT else 0


def _dictionaries(array) -> Iterable[pa.Array]:
    if isinstance(array, pa.ChunkedArray):
        for chunk in array.chunks:
            yield from _dictionaries(chunk)
    elif isinstance(array, pa.ExtensionArray):
        yield from _dictionaries(array.storage)
    elif isinstance(array, pa.DictionaryArray):
        yield array.dictionary
    elif isinstance(array, pa.StructArray):
        for i in range(array.type.num_fields):
            yield from _dictionaries(array.field(i))
    elif isinstance(array, (pa.ListArray, pa.LargeListArray, pa.FixedSizeListArray)):
        # A MapArray is a ListArray of key-value structs.
        yield from _dictionaries(array.values)


def arrow_dictionary_bytes(column) -> Optional[int]:
    """Buffer bytes of the Arrow dictionaries in a decoded column, or ``None``
    if it holds none -- a read that ignored the dictionary type, which must not
    pass for an empty dictionary."""
    dictionaries = list(_dictionaries(column))
    if not dictionaries:
        return None
    return sum(dictionary.get_total_buffer_size() for dictionary in dictionaries)


class _Levels(NamedTuple):
    """One leaf chunk's level counts, answering slot and null counts.

    The ``SizeStatistics`` histograms answer them exactly. Without them the
    footer still records how many level entries the chunk holds and, in its
    ``Statistics``, how many of those entries hold no value: enough outside
    any list, and inside one an upper bound, so the estimate errs high.
    """

    num_rows: int
    repetition: Tuple[int, ...]
    definition: Tuple[int, ...]
    unencoded_bytes: Optional[int]
    chunk: ColumnChunkMetaData
    data_page_encodings: Optional[frozenset] = None
    dictionary_page: Optional[DictionaryPage] = None
    max_definition_level: int = 0
    # The ``Statistics`` null count -- entries below the max definition level
    # -- looked up only when there is no definition histogram to count them.
    null_count: Optional[int] = None

    def _opened(self, depth: int) -> int:
        """Entries at repetition level ``depth`` or less, or an upper bound."""
        if self.repetition:
            return sum(self.repetition[: depth + 1])
        # At the max repetition level that is every entry, and short of it no
        # more than every entry.
        return self.chunk.num_values

    def _below(self, def_level: int) -> Optional[int]:
        """Entries whose definition level is under ``def_level``, or ``None``.

        Without a definition histogram the null count stands in for the
        entries under the max definition level. It can run low inside a list:
        PyArrow reports 0 for the ``int64`` leaf of a ``list<struct<int64>>``
        whose lists are a quarter empty. A low count only raises the slots and
        values it is subtracted from, and the 4 bytes per empty list it takes
        from a ``BYTE_ARRAY`` leaf's bytes come back as the extra slots'
        offsets.
        """
        if self.definition:
            return sum(self.definition[:def_level])
        if def_level <= 0 or self.null_count == 0:
            return 0
        if def_level >= self.max_definition_level:
            return self.null_count
        return None

    def dictionary_live(self) -> bool:
        """Whether some data page references the chunk's dictionary page."""
        if self.data_page_encodings is not None:
            return bool(self.data_page_encodings & _DICTIONARY_ENCODING_IDS)
        # Without ``encoding_stats`` only the chunk-wide list is left. Format 1.0
        # files name the dictionary page PLAIN_DICTIONARY too, so a dead page
        # reads as live here: the safe direction.
        encodings = self.chunk.encodings
        return _RLE_DICTIONARY in encodings or _PLAIN_DICTIONARY in encodings

    def slots(self, depth: int, slot_def: int) -> int:
        """Slots at repetition ``depth`` whose definition level reaches ``slot_def``.

        An entry opens a slot at depth ``k >= 1`` when its repetition level is
        at most ``k`` (it does not continue a deeper list) and its definition
        level reaches the ``k``-th repeated group (the list there is non-empty
        and non-null). An entry that stops short of that group has a repetition
        level below ``k`` -- it cannot repeat a list it never entered -- so the
        two marginal histograms suffice: entries at repetition level ``<= k``,
        minus entries below ``slot_def``. Either count unknown, its bound keeps
        the result an upper bound: every entry, minus none.
        """
        if depth == 0:
            return self.num_rows
        return self._opened(depth) - (self._below(slot_def) or 0)

    def nulls(self, slot_def: int, def_level: int) -> Optional[int]:
        """Slots that exist (level ``>= slot_def``) but are null (``< def_level``).

        A slot is null when this array or any ancestor up to the enclosing list
        is, and PyArrow marks it null in this array's own bitmap either way.
        ``None`` when unknown.
        """
        below = self._below(def_level)
        if below == 0:
            return 0
        above = self._below(slot_def)
        if below is None or above is None:
            return None
        return below - above

    def values(self) -> int:
        """Non-null leaf values, or every level entry when that is unknown."""
        return self.chunk.num_values - (self._below(self.max_definition_level) or 0)

    def fewest_values(self) -> int:
        """Non-null leaf values, or none when that is unknown."""
        below = self._below(self.max_definition_level)
        return 0 if below is None else self.chunk.num_values - below


def _histograms_agree(
    stats: LeafSizeStats,
    chunk: ColumnChunkMetaData,
    num_rows: int,
    max_def: int,
    max_rep: int,
) -> bool:
    """Whether the histograms the writer recorded fit the footer's own counts.

    Cheap, and the last line of defense against a cursor that drifted into a
    plausible-looking struct. Level 0 may exceed the row count: PyArrow 24 adds
    one level-0 entry, taken from level 1, per 16,384 values of a single record
    at max repetition level 1, so one 128x1024 tensor row reports (8, 131064).
    Slot counts sum level 0 with the levels above it, so a surplus there can
    only raise them.
    """
    repetition = stats.repetition_level_histogram
    definition = stats.definition_level_histogram
    if repetition and (
        len(repetition) != max_rep + 1
        or repetition[0] < num_rows
        or sum(repetition) != chunk.num_values
    ):
        return False
    return not definition or (
        len(definition) == max_def + 1 and sum(definition) == chunk.num_values
    )


def _levels(
    profile: LeafProfile,
    chunk: ColumnChunkMetaData,
    stats: Optional[LeafSizeStats],
    num_rows: int,
    dictionary_page: Optional[DictionaryPage] = None,
) -> _Levels:
    """The leaf chunk's level counts and character bytes.

    From its ``SizeStatistics`` where the writer recorded them, and from the
    rest of the footer where it did not: the spec lets a writer omit the
    repetition histogram at max repetition level 0 and the definition histogram
    at max definition level <= 1, PyArrow writes no ``SizeStatistics`` at all
    for a required, unrepeated leaf that is not ``BYTE_ARRAY``, and writers
    that predate them write none anywhere. Histograms that disagree with the
    footer mean the walk drifted, so nothing else it read for the chunk is used
    either.
    """
    max_def = profile.max_definition_level
    max_rep = profile.max_repetition_level
    if stats is not None and not _histograms_agree(
        stats, chunk, num_rows, max_def, max_rep
    ):
        stats = None
    repetition = definition = ()
    unencoded_bytes = data_page_encodings = None
    if stats is not None:
        repetition = stats.repetition_level_histogram
        definition = stats.definition_level_histogram
        unencoded_bytes = stats.unencoded_byte_array_data_bytes
        data_page_encodings = stats.data_page_encodings
    # PyArrow 24's DELTA_BYTE_ARRAY encoder leaves out of this count every value
    # whose suffix is empty -- one equal to, or a prefix of, the value before it
    # -- so 1,000 copies of an 8-byte string report 8 bytes.
    if unencoded_bytes is not None and _DELTA_BYTE_ARRAY in chunk.encodings:
        unencoded_bytes = None
    null_count = None
    if max_def and not definition:
        # Building a ``Statistics`` measured at roughly two thirds of this
        # module's cost per leaf, so only the leaves that need it pay: none in a
        # file with histograms.
        statistics = chunk.statistics
        if (
            statistics is not None
            and statistics.has_null_count
            and 0 <= statistics.null_count <= chunk.num_values
        ):
            null_count = statistics.null_count
    levels = _Levels(
        num_rows,
        repetition,
        definition,
        unencoded_bytes,
        chunk,
        data_page_encodings,
        dictionary_page,
        max_def,
        null_count,
    )
    if unencoded_bytes is None and profile.nodes[-1].kind in (
        _BINARY,
        _DICTIONARY,
        _VIEW,
    ):
        levels = levels._replace(unencoded_bytes=_footer_byte_array_bytes(levels))
    return levels


def _footer_byte_array_bytes(levels: _Levels) -> Optional[int]:
    """Character bytes of a ``BYTE_ARRAY`` chunk whose footer does not record them.

    The chunk's uncompressed size holds them, along with each PLAIN value's
    4-byte length prefix, which the value count sizes, and the pages' headers
    and level runs, which are small and only push the result up. A dictionary
    adds its page, whose header (see :func:`dictionary_page_read_size`) gives
    its size, and the index pages, which say which values the rows hold but
    not, from the footer, how long those are: each indexed value is taken at
    the dictionary's mean length. That is exact for values of one length, and
    otherwise off by however far the common values' mean strays from the
    dictionary's -- in either direction.

    When the writer gave up on the dictionary partway, the footer says neither
    how many values went to the PLAIN pages after it nor how long those are, so
    every value is still taken at the dictionary's mean length -- but at no
    less than the data pages hold when read as PLAIN values.

    ``None`` when the footer cannot bound them: a dictionary page whose header
    was not read, or DELTA_BYTE_ARRAY, which stores each value as the suffix
    that differs from the one before it.
    """
    chunk = levels.chunk
    encodings = chunk.encodings
    if _DELTA_BYTE_ARRAY in encodings:
        return None
    # DELTA_LENGTH_BYTE_ARRAY packs the lengths apart from the bytes, in as
    # little as nothing.
    prefix = 0 if _DELTA_LENGTH_BYTE_ARRAY in encodings else _OFFSET_WIDTH
    data = chunk.total_uncompressed_size
    if not chunk.has_dictionary_page:
        return max(data - prefix * levels.fewest_values(), 0)
    page = levels.dictionary_page
    if page is None:
        return None
    # The data pages, plus the dictionary page's header.
    rest = max(data - page.uncompressed_page_size, 0)
    page_encodings = levels.data_page_encodings
    if page_encodings is not None and not page_encodings & _DICTIONARY_ENCODING_IDS:
        # No data page indexes the dictionary: every value is in them, PLAIN.
        return max(rest - prefix * levels.fewest_values(), 0)
    entries = page.num_values
    mean = (
        max(page.uncompressed_page_size - _OFFSET_WIDTH * entries, 0) / entries
        if entries
        else 0
    )
    values = levels.values()
    at_mean = math.ceil(mean * values)
    if page_encodings is not None and not page_encodings - _DICTIONARY_ENCODING_IDS:
        return at_mean
    return max(at_mean, rest - prefix * values)


def _bitmap_size(length: int) -> int:
    return (length + 7) // 8


def _node_size(node: _ArrowNode, levels: _Levels) -> Optional[int]:
    """Decoded Arrow bytes of one array, or ``None`` if unknown."""
    kind = node.kind
    if kind == _NULL:
        return 0
    length = levels.slots(node.slot_depth, node.slot_def) * node.scale

    size = 0
    if node.nullable:
        if node.zero_copy:
            size += _bitmap_size(length)
        else:
            nulls = levels.nulls(node.slot_def, node.def_level)
            # Unknown counts as present: undersizing a bin is what overfills a
            # read task.
            if nulls is None or nulls > 0:
                size += _bitmap_size(length)

    if kind == _LIST:
        size += node.offset_width * (length + 1)
    elif kind == _FIXED_WIDTH:
        size += node.value_width * length
    elif kind == _BOOLEAN:
        size += _bitmap_size(length)
    elif kind == _DICTIONARY:
        if not length:
            size += _EMPTY_DICTIONARY_BYTES
        else:
            size += node.value_width * length + _dictionary_size(
                levels, node.offset_width
            )
    elif kind in (_BINARY, _VIEW):
        # The spec defines a BYTE_ARRAY leaf's character bytes as exclusive of
        # each value's length prefix.
        if levels.unencoded_bytes is None:
            return None
        if kind == _VIEW:
            return size + node.value_width * length + _view_out_of_line_bytes(levels)
        size += levels.unencoded_bytes + node.offset_width * (length + 1)
    return size


def _view_out_of_line_bytes(levels: _Levels) -> int:
    """Bytes a view chunk's values add outside their views.

    Values of up to 12 bytes live inside their views; each longer one adds its
    bytes to a data buffer, copied again for every repeat. The footer has the
    values' total bytes but not how they split, and counting all of them out of
    line is the bound: exact once every value passes 12 bytes, 1.75x at worst
    (all exactly 12).

    The dictionary page splits them when some data page indexes it. Its values
    are the chunk's distinct values -- or, when the dictionary filled up and the
    writer fell back to PLAIN for the rest of the chunk, its first distinct
    values -- and each is used at least once: its long values count once, its
    short ones not at all. The other uses -- repeats, and after a fallback the
    PLAIN values -- hold the remaining bytes. When the dictionary's values all
    fall on one side of 12 bytes, so do the repeats of a chunk whose every data
    page indexes it, which makes the size exact.

    Otherwise how the remaining bytes split turns on how often each value is
    used, which neither the footer nor the page records, so the page stands in
    as a sample, read two ways. *Proportional* gives the other uses the
    dictionary's share of bytes in long values. *Two-class* gives their short
    and long values the dictionary's mean short and long lengths, and lets their
    byte total settle how many are long. Proportional runs high when frequent
    values are shorter than the dictionary's average and low when they are
    longer; two-class the reverse. Taking the larger errs high when they
    disagree.

    After a fallback the PLAIN values need not resemble the page. When their
    bytes average more than its longest value they cannot all be short, and all
    of them count out of line.

    Only the sample can make the estimate undershoot, by at most 12 bytes per
    use beyond each value's first, so a column estimates no lower than 16/28 of
    its decoded size.
    """
    unencoded = levels.unencoded_bytes
    page = levels.dictionary_page
    encodings = levels.data_page_encodings
    if (
        page is None
        or not page.value_lengths
        or not encodings
        or not encodings & _DICTIONARY_ENCODING_IDS
    ):
        return unencoded
    lengths = page.value_lengths
    dictionary_values = dictionary_bytes = long_values = long_bytes = 0
    for value_length, count in lengths.items():
        dictionary_values += count
        dictionary_bytes += value_length * count
        if value_length > _VIEW_INLINE_BYTES:
            long_values += count
            long_bytes += value_length * count
    # The uses beyond each dictionary value's first, and their bytes.
    rest = levels.values() - dictionary_values
    rest_bytes = unencoded - dictionary_bytes
    if rest < 0 or rest_bytes < 0:
        return unencoded
    if rest_bytes > max(lengths) * rest:
        return long_bytes + rest_bytes
    short_values = dictionary_values - long_values
    if not long_values:
        return 0
    if not short_values:
        return long_bytes + rest_bytes
    proportional = rest_bytes * long_bytes / dictionary_bytes
    short_mean = (dictionary_bytes - long_bytes) / short_values
    long_mean = long_bytes / long_values
    rest_long = (rest_bytes - short_mean * rest) / (long_mean - short_mean)
    rest_long = min(max(rest_long, 0), rest)
    two_class = min(max(rest_bytes - short_mean * (rest - rest_long), 0), rest_bytes)
    return long_bytes + math.ceil(max(proportional, two_class))


def _dictionary_size(levels: _Levels, offset_width: int) -> int:
    """Bytes of the decoded Arrow dictionary of one column chunk.

    When a data page references the dictionary page, the decoded dictionary is
    that page whole: PyArrow writes an Arrow dictionary used or not, so a 1-row
    group can decode a 100-entry dictionary, well past the hydrated size. The page
    is the values PLAIN encoded, each behind a 4-byte length prefix where Arrow
    keeps an offset, so its header makes the size exact. Pages written PLAIN after
    the writer gave up on the dictionary add their new values to it, which the
    footer cannot separate from the repeats: the chunk decoded whole says (see
    :func:`dictionary_decode_size`), and without that the case takes every
    value's bytes on top, a bound.

    When no data page references it -- the writer fell back to PLAIN before the
    first data page, as PyArrow does when the Arrow dictionary holds a duplicate
    value -- the reader builds the dictionary from the values themselves, one
    entry per distinct non-null value. Every non-null value bounds that, and so
    does the unused page: PyArrow writes there the Arrow dictionary with its
    duplicates dropped, which holds every value the column uses, so the page is
    exact when each entry is used.

    Without the header, the chunk's uncompressed size bounds every case: it holds
    the dictionary page and, beyond it, the index pages. The hydrated size is
    an estimate itself where the footer lacks the values' bytes (see
    :func:`_footer_byte_array_bytes`), and is left out where it cannot say.
    """
    bound = (
        levels.chunk.total_uncompressed_size * offset_width // _OFFSET_WIDTH
        + offset_width
    )
    hydrated = (
        None
        if levels.unencoded_bytes is None
        else levels.unencoded_bytes + offset_width * (levels.values() + 1)
    )
    page = levels.dictionary_page
    dictionary = (
        None
        if page is None
        else page.uncompressed_page_size
        - _OFFSET_WIDTH * page.num_values
        + offset_width * (page.num_values + 1)
    )
    if not levels.dictionary_live():
        return min(size for size in (hydrated, bound, dictionary) if size is not None)
    encodings = levels.data_page_encodings
    if dictionary is None or encodings is None:
        return bound
    if encodings - _DICTIONARY_ENCODING_IDS:
        if page.decoded_dictionary_bytes is not None:
            return page.decoded_dictionary_bytes
        dictionary = bound if hydrated is None else min(bound, dictionary + hydrated)
    return dictionary


def estimate_row_group_decoded_size(
    row_group: RowGroupMetaData,
    leaf_profiles: Optional[List[LeafProfile]],
    leaf_indices: Optional[Sequence[int]],
    size_stats: Optional[List[Optional[LeafSizeStats]]],
    dictionary_pages: Optional[Mapping[int, DictionaryPage]] = None,
) -> Optional[int]:
    """Decoded Arrow size of one row group's read set, or ``None`` to fall back.

    Each leaf is sized from its ``SizeStatistics`` where the writer recorded
    them and by footer rules where it did not (see :func:`_levels`). A leaf
    whose values even the rules cannot size -- DELTA_BYTE_ARRAY, or a
    dictionary page that was not read -- takes the pre-existing estimate, its
    uncompressed size times :data:`PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT`,
    and the rest of the row group keeps its own sizes.

    Args:
        row_group: The ``RowGroupMetaData`` to size.
        leaf_profiles: The file's per-leaf profiles from
            :func:`build_leaf_profiles`, or ``None`` when its schema is not
            modeled (in which case the answer is ``None`` regardless).
        leaf_indices: Leaf-column indices the read task will decode, or ``None``
            for all of them.
        size_stats: This row group's per-leaf ``SizeStatistics``, as returned by
            :func:`~.parquet_size_statistics.read_size_statistics`, or ``None``
            when none were recovered, which sizes every leaf by the rules.
        dictionary_pages: This row group's dictionary pages by leaf index, for
            the leaves :func:`dictionary_page_read_size` names, carrying the
            decoded dictionary's bytes for those :func:`dictionary_decode_size`
            names. A missing one sizes that leaf by its bound instead.

    Returns:
        The summed decoded size in bytes, or ``None`` if it cannot be determined.
    """
    if leaf_profiles is None:
        return None
    indices = range(row_group.num_columns) if leaf_indices is None else leaf_indices
    num_rows = row_group.num_rows
    dictionary_pages = dictionary_pages or {}
    counted = set()
    total = 0
    for leaf_idx in indices:
        if leaf_idx >= len(leaf_profiles):
            return None
        profile = leaf_profiles[leaf_idx]
        chunk = row_group.column(leaf_idx)
        levels = _levels(
            profile,
            chunk,
            size_stats[leaf_idx]
            if size_stats is not None and leaf_idx < len(size_stats)
            else None,
            num_rows,
            dictionary_pages.get(leaf_idx),
        )
        # Any leaf below an array sees the same counts for it, so whichever leaf
        # reaches a shared ancestor first sizes it.
        for node in profile.nodes:
            if node.node_id in counted:
                continue
            counted.add(node.node_id)
            node_size = _node_size(node, levels)
            if node_size is None:
                # Only a BYTE_ARRAY leaf, the last node, can be unsizable.
                node_size = (
                    chunk.total_uncompressed_size
                    * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
                )
            total += node_size
    return total


def sum_exact(values: Iterable[Optional[int]]) -> Optional[int]:
    """Sum of ``values``, or ``None`` if any of them is ``None``.

    The one rule for combining decoded sizes: a total is exact only when every
    part is, and mixing exact parts with fallback estimates would produce a
    number no consumer could interpret. Shared by every site that aggregates
    decoded sizes (coalescing, bin sealing) so the rule has a single home.
    """
    total = 0
    for value in values:
        if value is None:
            return None
        total += value
    return total


def decoded_size_or_fallback(
    decoded_size: Optional[int], uncompressed_size: int
) -> int:
    """A chunk's decoded size, or the pre-existing estimate when unavailable.

    ``None`` means the footer could not yield an answer -- a schema the
    estimator does not model -- so this reproduces exactly what shipped before:
    uncompressed bytes scaled by the fixed encoding ratio. Shared by the bin
    packer and the reader's batch sizing so the two cannot drift.
    """
    if decoded_size is not None:
        return decoded_size
    return uncompressed_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
