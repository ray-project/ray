"""Extract exact per-column min/max/null counts from a Parquet footer.

Only scalar, non-repeated leaves whose bounds Parquet stores exactly are
answered. Everything else returns ``state=UNKNOWN`` and the caller reads the
file instead: a wrong min/max is silent, a fallback is merely slower.
"""

from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.interfaces.supports_column_statistics import (
    ColumnStatistics,
    ColumnStatisticsState,
)

# Bounds for these physical types are exact in Parquet. Floats may carry NaN
# (which bounds exclude) and BYTE_ARRAY bounds may be truncated by the writer,
# so both fall back to a read.
_EXACT_PHYSICAL_TYPES = frozenset({"INT32", "INT64", "BOOLEAN"})


def is_exact_statistics_type(column_type: pa.DataType) -> bool:
    """Whether a column of ``column_type`` can have exact footer bounds.

    The Arrow allowlist is the logical counterpart of
    ``_EXACT_PHYSICAL_TYPES``: only scalar types Parquet stores as INT32,
    INT64, or BOOLEAN. Keeping it next to the physical list is what stops the
    two from drifting, and it is why the planner can decline floats and
    strings without the reader decoding them first.

    A planning-time hint only: a file whose footer carries no statistics is
    still ``UNKNOWN`` per file.
    """
    return (
        pa.types.is_integer(column_type)
        or pa.types.is_boolean(column_type)
        or pa.types.is_date(column_type)
        or pa.types.is_time(column_type)
        or pa.types.is_timestamp(column_type)
    )


def _resolve_leaf(schema: pq.ParquetSchema, column: str) -> Optional[int]:
    """Return the sole physical leaf for a scalar column, or ``None``.

    A nested column is ambiguous here -- its statistics are per leaf, not per
    top-level value -- and a repeated leaf has no single row value either.
    """
    matches = [idx for idx in range(len(schema)) if schema.column(idx).path == column]
    if len(matches) == 1 and schema.column(matches[0]).max_repetition_level == 0:
        return matches[0]
    return None


def column_statistics(metadata: pq.FileMetaData, column: str) -> ColumnStatistics:
    """Return exact statistics for ``column``, or an unknown marker."""
    num_rows = metadata.num_rows
    leaf_idx = _resolve_leaf(metadata.schema, column)

    if leaf_idx is None:
        # Missing, nested, or repeated: let the caller read this file. A
        # missing column is null-filled by the reader, so this is conservative
        # rather than wrong.
        return ColumnStatistics.unknown(num_rows)

    leaf = metadata.schema.column(leaf_idx)
    if leaf.physical_type not in _EXACT_PHYSICAL_TYPES:
        return ColumnStatistics.unknown(num_rows)

    total_rows = 0
    null_count = 0
    min_value = None
    max_value = None
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        total_rows += row_group.num_rows
        if row_group.num_rows == 0:
            continue

        row_group_stats = row_group.column(leaf_idx).statistics
        if row_group_stats is None or not row_group_stats.has_null_count:
            return ColumnStatistics.unknown(num_rows)
        if not 0 <= row_group_stats.null_count <= row_group.num_rows:
            return ColumnStatistics.unknown(num_rows)

        null_count += row_group_stats.null_count
        if row_group_stats.null_count == row_group.num_rows:
            # All-null row groups contribute no bounds. Some writers still
            # emit placeholder bounds, which must not be folded.
            continue
        if not row_group_stats.has_min_max:
            # Non-null values exist but no bounds; the footer cannot settle it.
            return ColumnStatistics.unknown(num_rows)

        if min_value is None:
            min_value = row_group_stats.min
        else:
            min_value = min(min_value, row_group_stats.min)
        if max_value is None:
            max_value = row_group_stats.max
        else:
            max_value = max(max_value, row_group_stats.max)

    # The footer's row count is the sum of its row groups. If they disagree,
    # the null counts do not cover the file and no result can be trusted.
    if total_rows != num_rows:
        return ColumnStatistics.unknown(num_rows)

    if num_rows == 0:
        return ColumnStatistics(
            num_rows=0,
            null_count=0,
            min=None,
            max=None,
            state=ColumnStatisticsState.EMPTY,
        )

    if min_value is None:
        if null_count != num_rows:
            return ColumnStatistics.unknown(num_rows)
        state = ColumnStatisticsState.ALL_NULL
    else:
        if max_value is None or null_count >= num_rows:
            return ColumnStatistics.unknown(num_rows)
        state = ColumnStatisticsState.BOUNDED

    return ColumnStatistics(
        num_rows=num_rows,
        null_count=null_count,
        min=min_value,
        max=max_value,
        state=state,
    )


__all__ = ["column_statistics"]
