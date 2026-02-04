"""UPSERT logic for Delta Lake datasink.

This module handles UPSERT operations which delete matching rows and then append
new data. Note that this is NOT fully atomic (two separate transactions).

Delta Lake: https://delta.io/
deltalake Python library: https://delta-io.github.io/delta-rs/python/
"""

import datetime
import functools
import logging
import math
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.datasource.delta.utils import normalize_commit_properties

logger = logging.getLogger(__name__)

_MAX_IN_VALUES = 10_000
_MAX_COMPOUND_KEYS = 1_000


def quote_identifier(name: str) -> str:
    """Quote column name with backticks for SQL predicates.

    Args:
        name: Column name to quote.

    Returns:
        Quoted identifier string.
    """
    return f"`{name.replace('`', '``')}`"


def format_sql_value(value: Any) -> str:
    """Format a Python value for SQL predicate.

    Args:
        value: Python value to format.

    Returns:
        SQL-formatted value string.

    Raises:
        ValueError: If value cannot be formatted (e.g., NaN, Inf).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise ValueError(f"Cannot format {value} for SQL predicate")
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return f"'{value.isoformat()}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def build_delete_predicate(
    keys_table: pa.Table, upsert_cols: List[str]
) -> Optional[str]:
    """Build SQL delete predicate: (`col1` = 'val1' AND `col2` = 'val2') OR ...

    Args:
        keys_table: Table with key columns to match on.
        upsert_cols: List of column names to use for matching.

    Returns:
        SQL predicate string, or None if no keys.
    """
    if len(keys_table) == 0:
        return None

    if len(upsert_cols) == 1:
        col = upsert_cols[0]
        vals = keys_table.column(col).unique().to_pylist()
        if not vals:
            return None
        if len(vals) > _MAX_IN_VALUES:
            raise ValueError(
                f"Upsert has {len(vals)} unique key values; limit is {_MAX_IN_VALUES}. Batch your upserts."
            )
        return f"{quote_identifier(col)} IN ({', '.join(format_sql_value(v) for v in vals)})"

    unique = keys_table.group_by(upsert_cols).aggregate([])
    if len(unique) > _MAX_COMPOUND_KEYS:
        raise ValueError(
            f"Upsert has {len(unique)} unique compound keys; limit is {_MAX_COMPOUND_KEYS}. Batch your upserts."
        )

    ors = []
    for i in range(len(unique)):
        ands = [
            f"{quote_identifier(c)} = {format_sql_value(unique.column(c)[i].as_py())}"
            for c in upsert_cols
        ]
        ors.append(f"({' AND '.join(ands)})")
    return " OR ".join(ors)


def commit_upsert(
    table,
    file_actions,
    upsert_keys: Optional[pa.Table],
    upsert_cols: List[str],
    partition_cols: List[str],
    write_kwargs: dict,
) -> None:
    """Commit upsert: delete matching rows, then append. NOT fully atomic.

    Args:
        table: DeltaTable to commit to.
        file_actions: List of AddAction objects for new files.
        upsert_keys: Table with upsert key columns to match on.
        upsert_cols: List of column names to use for matching.
        partition_cols: List of partition column names.
        write_kwargs: Additional write options.

    Raises:
        RuntimeError: If the append transaction fails after delete succeeds.
    """
    logger.warning(
        "UPSERT uses two transactions (delete then append) and is NOT atomic. "
        "If append fails after delete succeeds, deleted rows will not be restored."
    )

    if upsert_keys is not None and len(upsert_keys) > 0:
        masks = []
        for c in upsert_cols:
            arr = upsert_keys[c]
            m = pc.is_valid(arr)
            if pa.types.is_floating(arr.type):
                m = pc.and_(m, pc.invert(pc.is_nan(arr)))
            masks.append(m)
        keys = upsert_keys.filter(functools.reduce(pc.and_, masks))
        pred = (
            build_delete_predicate(keys, upsert_cols) if len(keys) > 0 else None
        )
        if pred:
            table.delete(pred)

    props = normalize_commit_properties(write_kwargs.get("commit_properties"))
    try:
        table.create_write_transaction(
            file_actions,
            mode="append",
            schema=table.schema(),
            partition_by=partition_cols or None,
            commit_properties=props,
            post_commithook_properties=write_kwargs.get(
                "post_commithook_properties"
            ),
        )
    except Exception as e:
        raise RuntimeError(
            "UPSERT append failed after delete succeeded. Use Delta time travel to recover."
        ) from e
