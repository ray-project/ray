"""UPSERT logic for Delta Lake datasink.

This module handles UPSERT operations which delete matching rows and then append
new data. Note that this is NOT fully atomic (two separate transactions).
"""

import datetime
import functools
import logging
import math
from typing import TYPE_CHECKING, Any, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.datasource.delta.utils import normalize_commit_properties

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


def commit_upsert(
    table: "DeltaTable",
    file_actions: List["AddAction"],
    upsert_keys: Optional[pa.Table],
    upsert_cols: List[str],
    partition_cols: Optional[List[str]],
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

    Warning:
        UPSERT uses two transactions (delete then append) and is not atomic.
        For atomic upsert, use deltalake's native merge() API.
    """
    logger.warning(
        "UPSERT uses two transactions (delete then append) and is not atomic. "
        "For atomic upsert, use deltalake's native merge() API."
    )

    # Build delete predicate from upsert keys
    if upsert_keys is not None and len(upsert_keys) > 0:
        # Filter out rows with NULL or NaN values in join columns
        # (NULL != NULL and NaN != NaN in SQL semantics, causing silent duplicates)
        masks = []
        for col in upsert_cols:
            col_arr = upsert_keys[col]
            # Filter NULL values
            valid_mask = pc.is_valid(col_arr)
            # Also filter NaN for float columns
            if pa.types.is_floating(col_arr.type):
                not_nan_mask = pc.invert(pc.is_nan(col_arr))
                valid_mask = pc.and_(valid_mask, not_nan_mask)
            masks.append(valid_mask)
        mask = functools.reduce(pc.and_, masks)
        keys_table = upsert_keys.filter(mask)

        if len(keys_table) > 0:
            # Build IN predicate for each key column
            delete_predicate = build_delete_predicate(keys_table, upsert_cols)
            if delete_predicate:
                table.delete(delete_predicate)

    # Append new data files
    commit_properties = normalize_commit_properties(
        write_kwargs.get("commit_properties")
    )

    # Get schema for transaction
    # Use delta schema directly for transaction
    delta_schema = table.schema()

    table.create_write_transaction(
        file_actions,
        mode="append",
        schema=delta_schema,
        partition_by=partition_cols or None,
        commit_properties=commit_properties,
        post_commithook_properties=write_kwargs.get("post_commithook_properties"),
    )


def quote_identifier(name: str) -> str:
    """Quote column name with backticks for SQL predicates.

    Args:
        name: Column name to quote.

    Returns:
        Quoted identifier string.
    """
    return f"`{name.replace('`', '``')}`"


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

    # For efficiency with large key sets, use IN clause for single column
    if len(upsert_cols) == 1:
        col = upsert_cols[0]
        quoted_col = quote_identifier(col)
        values = keys_table.column(col).unique().to_pylist()
        if not values:
            return None

        # Limit single-column IN clause to prevent excessively large predicates
        max_in_values = 10000
        if len(values) > max_in_values:
            raise ValueError(
                f"Upsert has {len(values)} unique key values, exceeding limit of "
                f"{max_in_values}. Batch your upserts into smaller chunks."
            )

        # Format values for SQL (filter out NaN which can't be compared)
        formatted_vals = ", ".join(format_sql_value(v) for v in values)
        return f"{quoted_col} IN ({formatted_vals})"

    # For compound keys, deduplicate first then build OR of ANDed conditions
    # Use group_by to get unique compound key combinations
    unique_keys = keys_table.group_by(upsert_cols).aggregate([])

    max_keys = 1000
    if len(unique_keys) > max_keys:
        raise ValueError(
            f"Upsert has {len(unique_keys)} unique compound keys, exceeding "
            f"limit of {max_keys}. Batch your upserts into smaller chunks."
        )

    conditions = []
    for i in range(len(unique_keys)):
        row_conditions = []
        for col in upsert_cols:
            quoted_col = quote_identifier(col)
            val = unique_keys.column(col)[i].as_py()
            row_conditions.append(f"{quoted_col} = {format_sql_value(val)}")
        conditions.append(f"({' AND '.join(row_conditions)})")

    return " OR ".join(conditions)


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
