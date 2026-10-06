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

:mod:`.parquet_size_statistics` recovers ``SizeStatistics`` from the footer
bytes. Every function here returns ``None`` rather than guessing when the inputs
cannot support an answer, which is the caller's signal to keep its existing
uncompressed-size behavior.
"""

from __future__ import annotations

from typing import Iterable, List, NamedTuple, Optional, Sequence, Tuple

import pyarrow as pa
from pyarrow.parquet import ColumnChunkMetaData, ParquetSchema, RowGroupMetaData

from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
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
# Arrow's null type allocates no buffers at all.
_NULL = "null"

# Offsets buffers hold one more entry than there are slots, so the trailing end
# offset is included. The large_* variants use 64-bit offsets.
_OFFSET_WIDTH = 4
_LARGE_OFFSET_WIDTH = 8

_BYTE_ARRAY = "BYTE_ARRAY"

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
        # Not fixed-width: view types and anything else this module does not
        # model.
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
        if isinstance(arrow_type, pa.ExtensionType):
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
        if leaf.kind in (_BINARY, _DICTIONARY) and physical_type != _BYTE_ARRAY:
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


class _Levels(NamedTuple):
    """One leaf chunk's level histograms, answering slot and null counts."""

    num_rows: int
    repetition: Tuple[int, ...]
    definition: Tuple[int, ...]
    unencoded_bytes: Optional[int]
    chunk: ColumnChunkMetaData

    def slots(self, depth: int, slot_def: int) -> int:
        """Slots at repetition ``depth`` whose definition level reaches ``slot_def``.

        An entry opens a slot at depth ``k >= 1`` when its repetition level is
        at most ``k`` (it does not continue a deeper list) and its definition
        level reaches the ``k``-th repeated group (the list there is non-empty
        and non-null). An entry that stops short of that group has a repetition
        level below ``k`` -- it cannot repeat a list it never entered -- so the
        two marginal histograms suffice: entries at repetition level ``<= k``,
        minus entries below ``slot_def``.
        """
        if depth == 0:
            return self.num_rows
        return sum(self.repetition[: depth + 1]) - sum(self.definition[:slot_def])

    def nulls(self, slot_def: int, def_level: int) -> Optional[int]:
        """Slots that exist (level ``>= slot_def``) but are null (``< def_level``).

        A slot is null when this array or any ancestor up to the enclosing list
        is, and PyArrow marks it null in this array's own bitmap either way.
        ``None`` when unknown.
        """
        if self.definition:
            return sum(self.definition[slot_def:def_level])
        # Reached only at max definition level 1, where the spec lets writers
        # omit the histogram: the one nullable level's count is the chunk's null
        # count. Cold for PyArrow-written files, which emit the histogram even
        # then -- worth knowing because building a ``Statistics`` per leaf
        # measured at roughly two thirds of this module's total cost.
        statistics = self.chunk.statistics
        if statistics is not None and statistics.has_null_count:
            return statistics.null_count
        return None


def _levels(
    profile: LeafProfile,
    chunk: ColumnChunkMetaData,
    stats: Optional[LeafSizeStats],
    num_rows: int,
) -> Optional[_Levels]:
    """The leaf chunk's histograms, or ``None`` if the ones it needs are absent."""
    max_def = profile.max_definition_level
    max_rep = profile.max_repetition_level
    if stats is None:
        # PyArrow writes no SizeStatistics at all for a required, unrepeated
        # leaf that is not BYTE_ARRAY: every count it would hold is the row
        # count. Anywhere else a missing entry means the writer or the walk gave
        # up, which must not turn into a number.
        if max_def or max_rep:
            return None
        return _Levels(num_rows, (), (), None, chunk)

    repetition = stats.repetition_level_histogram
    definition = stats.definition_level_histogram
    # The spec lets a writer omit the repetition histogram at max repetition
    # level 0 and the definition histogram at max definition level <= 1. Beyond
    # that, slot and null counts have nothing to come from.
    if max_rep and not repetition:
        return None
    if (max_rep or max_def > 1) and not definition:
        return None
    # Shape and totals against the footer's own counts: cheap, and the last line
    # of defense against a cursor that drifted into a plausible-looking struct.
    if repetition and (len(repetition) != max_rep + 1 or repetition[0] != num_rows):
        return None
    if definition and (
        len(definition) != max_def + 1 or sum(definition) != chunk.num_values
    ):
        return None
    return _Levels(
        num_rows, repetition, definition, stats.unencoded_byte_array_data_bytes, chunk
    )


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
    elif kind in (_BINARY, _DICTIONARY):
        # A BYTE_ARRAY leaf's character bytes exist nowhere else in the footer.
        # The spec defines them as exclusive of each value's length prefix.
        if levels.unencoded_bytes is None:
            return None
        hydrated = levels.unencoded_bytes + node.offset_width * (length + 1)
        if kind == _BINARY:
            size += hydrated
        else:
            # The dictionary holds each distinct value once, so it is at most
            # the hydrated size. It is also at most the chunk's uncompressed
            # size, which contains the dictionary page: the same values, PLAIN
            # encoded with a 4-byte length prefix where Arrow keeps an offset
            # (twice that for large offsets). The bound overshoots by the index
            # pages, which are small next to the values whenever the dictionary
            # matters.
            bound = (
                levels.chunk.total_uncompressed_size
                * node.offset_width
                // _OFFSET_WIDTH
                + node.offset_width
            )
            size += node.value_width * length + min(hydrated, bound)
    return size


def estimate_row_group_decoded_size(
    row_group: RowGroupMetaData,
    leaf_profiles: Optional[List[LeafProfile]],
    leaf_indices: Optional[Sequence[int]],
    size_stats: Optional[List[Optional[LeafSizeStats]]],
) -> Optional[int]:
    """Decoded Arrow size of one row group's read set, or ``None`` to fall back.

    The decision is all-or-nothing per row group: mixing exact and fallback
    sizing within one group produces a number that is hard to reason about, and
    the mixed case only arises with writers that partially emit size statistics.

    ``None`` when a ``BYTE_ARRAY`` leaf lacks ``unencoded_byte_array_data_bytes``,
    or when a leaf lacks a level histogram its nesting needs. Fixed-width leaves
    legitimately omit the former -- the spec restricts it to ``BYTE_ARRAY`` --
    and a required, unrepeated, fixed-width leaf needs no ``SizeStatistics`` at
    all, so requiring either everywhere would silently disable exact sizing for
    the commonest schemas while still paying the decode cost.

    Args:
        row_group: The ``RowGroupMetaData`` to size.
        leaf_profiles: The file's per-leaf profiles from
            :func:`build_leaf_profiles`, or ``None`` when no size statistics
            were recovered for the file or its schema is not modeled (in which
            case the answer is ``None`` regardless).
        leaf_indices: Leaf-column indices the read task will decode, or ``None``
            for all of them.
        size_stats: This row group's per-leaf ``SizeStatistics``, as returned by
            :func:`~.parquet_size_statistics.read_size_statistics`, or ``None``.

    Returns:
        The summed decoded size in bytes, or ``None`` if it cannot be determined.
    """
    if size_stats is None or leaf_profiles is None:
        return None
    indices = range(row_group.num_columns) if leaf_indices is None else leaf_indices
    num_rows = row_group.num_rows
    counted = set()
    total = 0
    for leaf_idx in indices:
        if leaf_idx >= len(size_stats) or leaf_idx >= len(leaf_profiles):
            return None
        profile = leaf_profiles[leaf_idx]
        levels = _levels(
            profile, row_group.column(leaf_idx), size_stats[leaf_idx], num_rows
        )
        if levels is None:
            return None
        # Any leaf below an array sees the same counts for it, so whichever leaf
        # reaches a shared ancestor first sizes it.
        for node in profile.nodes:
            if node.node_id in counted:
                continue
            counted.add(node.node_id)
            node_size = _node_size(node, levels)
            if node_size is None:
                return None
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

    ``None`` means the footer could not yield an exact answer, so this reproduces
    exactly what shipped before: uncompressed bytes scaled by the fixed encoding
    ratio. Shared by the bin packer and the reader's batch sizing so the two
    cannot drift.
    """
    if decoded_size is not None:
        return decoded_size
    return uncompressed_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
