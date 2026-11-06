"""Column statistics from Parquet footers for the Ray Data CBO."""

import logging
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    import pyarrow.parquet

logger = logging.getLogger(__name__)


def extract_column_stats_from_parquet_metadata(
    parquet_metadata: "pyarrow.parquet.FileMetaData",
) -> Dict[str, Dict[str, Any]]:
    """Aggregates the column statistics of a Parquet file across row groups.

    Only top-level primitive columns are returned. A statistic is reported only
    if every row group provides it (all-null row groups need no min/max), so
    min/max are valid bounds and null counts are exact for the whole file. The
    distinct count, if any, is the maximum over row groups (a lower bound of the
    file's distinct count).

    Returns:
        A mapping from column name to a dict with the keys ``min``, ``max``,
        ``null_count`` and ``distinct_count``. Values may be ``None``.
    """
    try:
        top_level_names = set(parquet_metadata.schema.to_arrow_schema().names)
    except Exception as e:
        logger.debug(f"Failed to read Parquet schema: {e}")
        return {}

    column_stats = {}
    for col_idx in range(parquet_metadata.num_columns):
        try:
            path = parquet_metadata.schema.column(col_idx).path
            # Leaves of nested columns have paths like ``a.b`` or ``a.list.item``.
            if path not in top_level_names:
                continue
            column_stats[path] = _aggregate_row_groups(parquet_metadata, col_idx)
        except Exception as e:
            logger.debug(f"Failed to extract Parquet statistics of {col_idx=}: {e}")
    return column_stats


def _aggregate_row_groups(
    parquet_metadata: "pyarrow.parquet.FileMetaData", col_idx: int
) -> Dict[str, Any]:
    min_value = max_value = distinct_count = None
    null_count = 0
    has_min_max = has_null_count = True

    for rg_idx in range(parquet_metadata.num_row_groups):
        row_group = parquet_metadata.row_group(rg_idx)
        column = row_group.column(col_idx)
        stats = column.statistics if column.is_stats_set else None
        if stats is None:
            has_min_max = has_null_count = False
            continue

        rg_has_null_count = getattr(stats, "has_null_count", True)
        # An all-null row group has no min/max but doesn't widen the range.
        all_null = rg_has_null_count and stats.null_count == row_group.num_rows
        if all_null and not stats.has_min_max:
            pass
        elif has_min_max and stats.has_min_max:
            try:
                if min_value is None:
                    min_value, max_value = stats.min, stats.max
                else:
                    min_value = min(min_value, stats.min)
                    max_value = max(max_value, stats.max)
            except TypeError:
                has_min_max = False
        else:
            has_min_max = False

        if has_null_count and rg_has_null_count:
            null_count += stats.null_count or 0
        else:
            has_null_count = False

        if getattr(stats, "has_distinct_count", False) and stats.distinct_count:
            distinct_count = max(distinct_count or 0, stats.distinct_count)

    return {
        "min": min_value if has_min_max else None,
        "max": max_value if has_min_max else None,
        "null_count": null_count if has_null_count else None,
        "distinct_count": distinct_count,
    }
