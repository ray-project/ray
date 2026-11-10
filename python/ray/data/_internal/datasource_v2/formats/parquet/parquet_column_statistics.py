"""Extract exact per-column min/max/null counts from a Parquet footer.

Only scalar, non-repeated leaves whose bounds Parquet stores exactly are
answered. Everything else returns ``exact=False`` and the caller reads the
file instead: a wrong min/max is silent, a fallback is merely slower.
"""

from typing import Optional

import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.interfaces.supports_column_statistics import (
    ColumnStatistics,
)

# Bounds for these physical types are exact in Parquet. Floats may carry NaN
# (which bounds exclude) and BYTE_ARRAY bounds may be truncated by the writer,
# so both fall back to a read.
_EXACT_PHYSICAL_TYPES = frozenset({"INT32", "INT64", "BOOLEAN"})


def _resolve_leaf(schema: pq.ParquetSchema, column: str) -> Optional[int]:
    """Return the sole physical leaf for a scalar column, or ``None``.

    A nested column is ambiguous here -- its statistics are per leaf, not per
    top-level value -- and a repeated leaf has no single row value either.
    """
    exact = [idx for idx in range(len(schema)) if schema.column(idx).path == column]
    if len(exact) == 1 and schema.column(exact[0]).max_repetition_level == 0:
        return exact[0]
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

    null_count = 0
    min_value = None
    max_value = None
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        if row_group.num_rows == 0:
            continue

        statistics = row_group.column(leaf_idx).statistics
        if statistics is None or not statistics.has_null_count:
            return ColumnStatistics.unknown(num_rows)
        if statistics.null_count < 0 or statistics.null_count > row_group.num_rows:
            return ColumnStatistics.unknown(num_rows)

        null_count += statistics.null_count
        if statistics.has_min_max:
            if min_value is None or statistics.min < min_value:
                min_value = statistics.min
            if max_value is None or statistics.max > max_value:
                max_value = statistics.max
        elif statistics.null_count != row_group.num_rows:
            # Non-null values exist but no bounds; the footer cannot settle it.
            return ColumnStatistics.unknown(num_rows)

    if num_rows == 0:
        return ColumnStatistics(
            num_rows=0,
            null_count=0,
            min=None,
            max=None,
            exact=True,
        )

    if min_value is None and null_count < num_rows:
        return ColumnStatistics.unknown(num_rows)

    return ColumnStatistics(
        num_rows=num_rows,
        null_count=null_count,
        min=min_value,
        max=max_value,
        exact=True,
    )


__all__ = ["column_statistics"]
