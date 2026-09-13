"""Estimate the Arrow decoded size of a Parquet row group from its footer.

``total_uncompressed_size`` is Parquet's *encoded* size: decompressed, but still
dictionary/RLE/bit-packed. It is a poor proxy for the Arrow block a read task
materializes -- measured decoded/uncompressed ratios span 0.87x for plain
``int64`` to 133x for a nullable dictionary-encoded string column. Sizing read
tasks on it (whether raw or scaled by a fixed ratio) therefore either floods the
scheduler with tiny tasks or overfills blocks to the point of OOM.

Decoded size decomposes into three quantities per leaf. Two come from the footer
exactly: the value count, from ``ColumnChunkMetaData.num_values``, and the
per-value width, a pure function of the leaf's Parquet type. The third -- total
character bytes of ``BYTE_ARRAY`` leaves -- comes from ``SizeStatistics``, which
:mod:`.parquet_size_statistics` recovers from the footer bytes.

Every function here returns ``None`` rather than guessing when the inputs cannot
support an exact answer, which is the caller's signal to keep its existing
uncompressed-size behavior.
"""

from __future__ import annotations

import json
import math
from typing import Iterable, List, NamedTuple, Optional, Sequence

from pyarrow.parquet import (
    ColumnChunkMetaData,
    ColumnSchema,
    ParquetSchema,
    RowGroupMetaData,
)

from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
    LeafSizeStats,
)

# The V2 copy, which is what the reader's own fallback sizing uses
# (parquet_file_reader.py); importing the same definition keeps this module's
# fallback and the reader's from ever diverging.
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)

# Arrow's offsets buffer for string/binary/list uses int32 offsets and holds one
# more entry than there are values, so the trailing end offset is included.
_OFFSET_WIDTH = 4

_BOOLEAN = "BOOLEAN"
_BYTE_ARRAY = "BYTE_ARRAY"
_FIXED_LEN_BYTE_ARRAY = "FIXED_LEN_BYTE_ARRAY"

# Widths of the Arrow type each Parquet physical type decodes to. INT96 is 12
# bytes on disk but decodes to a 64-bit nanosecond timestamp.
_PHYSICAL_WIDTHS = {
    "INT32": 4,
    "INT64": 8,
    "INT96": 8,
    "FLOAT": 4,
    "DOUBLE": 8,
}

# Logical INT annotations narrow their physical type's Arrow width: an INT(8)
# rides on INT32 storage but decodes to a 1-byte Arrow integer.
_INT_LOGICAL_WIDTHS = {8: 1, 16: 2, 32: 4, 64: 8}
_FLOAT16_WIDTH = 2
_UUID_WIDTH = 16
# Arrow uses decimal128 up to precision 38 and decimal256 beyond it.
_DECIMAL128_WIDTH = 16
_DECIMAL256_WIDTH = 32
_MAX_DECIMAL128_PRECISION = 38


def _logical_type_name(column: ColumnSchema) -> Optional[str]:
    logical_type = column.logical_type
    if logical_type is None:
        return None
    name = logical_type.type
    # PyArrow reports "NONE" (and historically "UNKNOWN") for a leaf carrying no
    # logical annotation, which must not be mistaken for a real one.
    return None if name in ("NONE", "UNKNOWN") else name


def _int_logical_bit_width(column: ColumnSchema) -> Optional[int]:
    """Bit width of a leaf's logical ``INT`` annotation, if it declares one.

    ``ColumnSchema.precision`` is DECIMAL-only (it reports ``-1`` here) and
    ``ParquetLogicalType`` exposes only ``type`` and ``to_json``, so the width has
    to come out of the JSON. Reached only for ``INT32`` leaves carrying an ``INT``
    annotation, so the parse is not on the common path.
    """
    try:
        return json.loads(column.logical_type.to_json()).get("bitWidth")
    except (ValueError, AttributeError):
        return None


def _decimal_width(column: ColumnSchema) -> int:
    precision = column.precision
    if precision and precision > _MAX_DECIMAL128_PRECISION:
        return _DECIMAL256_WIDTH
    return _DECIMAL128_WIDTH


def parquet_leaf_fixed_width(column: ColumnSchema) -> Optional[int]:
    """Bytes per value in the Arrow data buffer for a fixed-width leaf.

    Returns ``None`` for ``BOOLEAN`` (bit-packed, so not a whole number of bytes
    per value) and ``BYTE_ARRAY`` (variable width), both of which callers size
    separately. Derived from the Parquet type rather than by building an Arrow
    schema, so no schema-tree walk is needed.

    Args:
        column: A ``pyarrow.parquet.ColumnSchema`` leaf.

    Returns:
        The per-value width in bytes, or ``None`` if the leaf is not fixed-width
        or its type is unrecognized.
    """
    physical_type = column.physical_type
    logical_name = _logical_type_name(column)

    if logical_name == "DECIMAL":
        return _decimal_width(column)

    if physical_type == _FIXED_LEN_BYTE_ARRAY:
        if logical_name == "FLOAT16":
            return _FLOAT16_WIDTH
        if logical_name == "UUID":
            return _UUID_WIDTH
        # Otherwise it decodes to fixed_size_binary of the declared length.
        length = column.length
        return length if length and length > 0 else None

    if physical_type == "INT32" and logical_name == "INT":
        bit_width = _int_logical_bit_width(column)
        return _INT_LOGICAL_WIDTHS.get(bit_width, 4) if bit_width else 4

    return _PHYSICAL_WIDTHS.get(physical_type)


class LeafProfile(NamedTuple):
    """The per-leaf schema facts the estimator consumes, all constant per file.

    Hoisted out of the per-row-group loop because every accessor behind them --
    ``ParquetSchema.column``, ``ColumnSchema.logical_type``, and the JSON
    round-trip for INT bit widths -- builds a fresh object per call, and the
    estimator runs once per row group.
    """

    physical_type: str
    # Bytes per value in the Arrow data buffer; ``None`` for ``BOOLEAN``
    # (bit-packed) and ``BYTE_ARRAY`` (variable width), which are sized
    # separately, and for unrecognized types, which force the fallback.
    fixed_width: Optional[int]
    max_definition_level: int
    max_repetition_level: int


def build_leaf_profiles(parquet_schema: ParquetSchema) -> List[LeafProfile]:
    """One :class:`LeafProfile` per leaf, in schema (== row group) column order.

    Called once per file; the result feeds every row group's
    :func:`estimate_row_group_decoded_size` call.
    """
    profiles: List[LeafProfile] = []
    for leaf_idx in range(len(parquet_schema)):
        column = parquet_schema.column(leaf_idx)
        profiles.append(
            LeafProfile(
                physical_type=column.physical_type,
                fixed_width=parquet_leaf_fixed_width(column),
                max_definition_level=column.max_definition_level,
                max_repetition_level=column.max_repetition_level,
            )
        )
    return profiles


def _has_nulls(
    profile: LeafProfile, chunk: ColumnChunkMetaData, stats: LeafSizeStats
) -> bool:
    """Whether this leaf chunk actually contains a null.

    Arrow allocates a validity bitmap only when a null is present, so keying off
    ``max_definition_level > 0`` (i.e. "nullable") instead double-counts: a
    nullable but null-free ``bool`` column then estimates 2.00x its real
    ``nbytes``.
    """
    if profile.max_definition_level == 0:
        return False  # required leaf: a null is impossible
    histogram = stats.definition_level_histogram
    if histogram:
        # The last bucket counts values at the max definition level, i.e. the
        # non-null ones; everything below it is a null at some level.
        return chunk.num_values > histogram[-1]
    # The spec permits omitting the histogram when max_definition_level <= 1, so
    # fall back to the null count PyArrow does expose.
    #
    # Cold for PyArrow-written files, which emit the histogram even at
    # max_definition_level 1 -- worth knowing because it is not cheap: building a
    # ``Statistics`` per leaf measured at roughly two thirds of this module's
    # total estimating cost. A writer that omits histograms would pay that on
    # every nullable leaf, which is where to look first if sizing turns up slow.
    statistics = chunk.statistics
    if statistics is not None and statistics.has_null_count:
        return statistics.null_count > 0
    # Unknown: assume a bitmap exists. Overestimating is the safe direction,
    # since undersizing a bin is what overfills a read task.
    return True


def _list_offsets_size(profile: LeafProfile, stats: LeafSizeStats) -> int:
    """Bytes of Arrow list-offset buffers for a repeated leaf.

    A leaf nested under ``R`` repeated groups decodes to ``R`` nested list
    arrays, each with its own offsets buffer. The repetition level histogram
    gives the count at each level, and the number of lists at level ``k`` is the
    running sum up to ``k`` -- so level 0 alone is the number of top-level lists.
    """
    max_repetition_level = profile.max_repetition_level
    if max_repetition_level == 0:
        return 0
    histogram = stats.repetition_level_histogram
    if not histogram:
        # Permitted omission only when max_repetition_level is 0, which is
        # already handled above; without the histogram there is no list count to
        # work from, so contribute nothing rather than invent one.
        return 0
    total = 0
    running = 0
    for level in range(min(max_repetition_level, len(histogram))):
        running += histogram[level]
        total += _OFFSET_WIDTH * (running + 1)
    return total


def _leaf_decoded_size(
    profile: LeafProfile, chunk: ColumnChunkMetaData, stats: LeafSizeStats
) -> Optional[int]:
    """Decoded Arrow bytes for one leaf column chunk, or ``None`` if unknown."""
    num_values = chunk.num_values
    physical_type = profile.physical_type

    if physical_type == _BYTE_ARRAY:
        if stats.unencoded_byte_array_data_bytes is None:
            # The one hard requirement: a BYTE_ARRAY leaf's character bytes exist
            # nowhere else in the footer, so without them there is no exact
            # answer and the caller must fall back.
            return None
        # The spec defines the field as exclusive of each value's length prefix,
        # so Arrow's offsets buffer is added separately rather than assumed
        # included.
        total = stats.unencoded_byte_array_data_bytes + _OFFSET_WIDTH * (num_values + 1)
    elif physical_type == _BOOLEAN:
        total = math.ceil(num_values / 8)
    else:
        width = profile.fixed_width
        if width is None:
            return None
        total = num_values * width

    if _has_nulls(profile, chunk, stats):
        total += math.ceil(num_values / 8)
    total += _list_offsets_size(profile, stats)
    return total


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

    Note that ``None`` is returned only when a *``BYTE_ARRAY``* leaf lacks
    ``unencoded_byte_array_data_bytes``. Fixed-width leaves legitimately omit
    that sub-field -- the spec restricts it to ``BYTE_ARRAY`` -- and requiring it
    everywhere would silently disable exact sizing for every flat numeric schema
    while still paying the decode cost.

    Args:
        row_group: The ``RowGroupMetaData`` to size.
        leaf_profiles: The file's per-leaf profiles from
            :func:`build_leaf_profiles`, or ``None`` when no size statistics
            were recovered for the file (in which case the answer is ``None``
            regardless).
        leaf_indices: Leaf-column indices the read task will decode, or ``None``
            for all of them.
        size_stats: This row group's per-leaf ``SizeStatistics``, as returned by
            :func:`~.parquet_size_statistics.read_size_statistics`, or ``None``.

    Returns:
        The summed decoded size in bytes, or ``None`` if it cannot be determined
        exactly.
    """
    if size_stats is None or leaf_profiles is None:
        return None
    indices = range(row_group.num_columns) if leaf_indices is None else leaf_indices
    total = 0
    for leaf_idx in indices:
        if leaf_idx >= len(size_stats) or leaf_idx >= len(leaf_profiles):
            return None
        stats = size_stats[leaf_idx]
        if stats is None:
            return None
        leaf_size = _leaf_decoded_size(
            leaf_profiles[leaf_idx], row_group.column(leaf_idx), stats
        )
        if leaf_size is None:
            return None
        total += leaf_size
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
