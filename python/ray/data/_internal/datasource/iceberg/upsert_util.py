"""
Utility functions for Iceberg merge and upsert operations in Ray Data.

Provides MERGE INTO and UPSERT operations on Iceberg tables using PyIceberg.
See: https://py.iceberg.apache.org/reference/pyiceberg/table/
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from packaging import version

from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression

    import ray.data

logger = logging.getLogger(__name__)


def _check_pyiceberg_version_for_upsert() -> bool:
    """Check if PyIceberg version supports native upsert operations."""
    try:
        import pyiceberg

        return version.parse(pyiceberg.__version__) >= version.parse("0.9.0")
    except ImportError:
        return False


def _build_key_filter(join_columns: List[str], keys_df) -> "BooleanExpression":
    """Build PyIceberg filter expression for matching keys."""
    from pyiceberg.expressions import And, EqualTo, In, Or

    if len(join_columns) == 1:
        col_name = join_columns[0]
        unique_keys = keys_df[col_name].unique().tolist()
        return EqualTo(col_name, unique_keys[0]) if len(unique_keys) == 1 else In(col_name, unique_keys)

    unique_combinations = keys_df.drop_duplicates().head(1000)
    if len(unique_combinations) < len(keys_df):
        logger.warning(f"Limiting {len(keys_df)} key combinations to 1000 for filter expression")

    filters = [And(*[EqualTo(k, row[k]) for k in join_columns]) for _, row in unique_combinations.iterrows()]
    return filters[0] if len(filters) == 1 else Or(*filters)


def _add_priority_column(batch, priority_col: str, priority: int):
    """Add priority column to batch."""
    import pyarrow as pa
    if isinstance(batch, pa.Table):
        return batch.append_column(priority_col, pa.array([priority] * len(batch), type=pa.int8()))
    batch[priority_col] = priority
    return batch


def _deduplicate_batch(batch, join_columns: List[str], priority_col: str):
    """Deduplicate batch keeping row with highest priority."""
    import pyarrow as pa
    if not isinstance(batch, pa.Table):
        batch = pa.Table.from_pandas(batch)
    if len(batch) == 0 or priority_col not in batch.schema.names:
        return batch.drop([priority_col]) if priority_col in batch.schema.names else batch

    df = batch.to_pandas(zero_copy_only=False, self_destruct=True)
    df.sort_values(priority_col, ascending=False, inplace=True, kind="mergesort")
    df.drop_duplicates(subset=join_columns, keep="first", inplace=True)
    df.drop(columns=[priority_col], inplace=True)
    return pa.Table.from_pandas(df, schema=batch.drop([priority_col]).schema)


def upsert_to_iceberg(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    join_columns: List[str],
    catalog_kwargs: Optional[Dict[str, Any]] = None,
    snapshot_properties: Optional[Dict[str, str]] = None,
    update_filter: Optional["BooleanExpression"] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """
    Perform an upsert (merge) operation on an Iceberg table.

    Updates existing rows and inserts new rows based on join_columns.
    Uses streaming execution to scale to any dataset size.

    Args:
        dataset: Ray Dataset containing source data to merge
        table_identifier: Fully qualified table identifier (e.g., "db.table")
        join_columns: Column names to use for matching rows
        catalog_kwargs: Arguments for PyIceberg catalog.load_catalog()
        snapshot_properties: Custom properties to attach to snapshot
        update_filter: Optional filter to limit which rows can be updated
        ray_remote_args: kwargs passed to ray.remote
        concurrency: Maximum number of concurrent Ray tasks
    """
    _check_import(None, module="pyiceberg", package="pyiceberg")

    if not join_columns:
        raise ValueError(
            "join_columns must be specified for upsert operations. "
            "Provide at least one column to use as a key."
        )

    catalog_kwargs = catalog_kwargs or {}
    snapshot_properties = {
        **(snapshot_properties or {}),
        "ray.operation": "upsert",
        "ray.join_columns": ",".join(join_columns),
    }

    if _check_pyiceberg_version_for_upsert():
        _upsert_native(
            dataset, table_identifier, join_columns, catalog_kwargs,
            snapshot_properties, update_filter, ray_remote_args, concurrency
        )
    else:
        logger.warning(
            "PyIceberg version < 0.9.0 detected. Using fallback implementation. "
            "Consider upgrading for better performance."
        )
        _upsert_fallback(
            dataset, table_identifier, join_columns, catalog_kwargs,
            snapshot_properties, update_filter, ray_remote_args, concurrency
        )


def _upsert_native(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    join_columns: List[str],
    catalog_kwargs: Dict[str, Any],
    snapshot_properties: Dict[str, str],
    update_filter: Optional["BooleanExpression"],
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> None:
    """Perform efficient merge using partial overwrite."""
    from pyiceberg.expressions import And, In, Not

    import ray

    catalog_name = catalog_kwargs.pop("name", "default")
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Validate schemas
    source_columns, table_columns = set(dataset.schema().names), {f.name for f in table.schema().fields}
    missing_in_source = set(join_columns) - source_columns
    missing_in_target = set(join_columns) - table_columns
    if missing_in_source:
        raise ValueError(f"Join columns {missing_in_source} not in source. Available: {sorted(source_columns)}")
    if missing_in_target:
        raise ValueError(f"Join columns {missing_in_target} not in table. Available: {sorted(table_columns)}")

    # Check partition alignment
    partition_fields = table.spec().fields
    if partition_fields:
        partition_cols = {table.schema().find_column_name(f.source_id) for f in partition_fields}
        if not set(join_columns).issubset(partition_cols):
            logger.warning(f"Merge keys {join_columns} don't match partition columns {sorted(partition_cols)}")

    # Build filter
    source_keys_table = dataset.select_columns(join_columns).to_arrow()
    if len(join_columns) == 1:
        read_filter = In(join_columns[0], list(set(source_keys_table[join_columns[0]].to_pylist())))
    else:
        read_filter = _build_key_filter(join_columns, source_keys_table.to_pandas())

    read_filter_with_update = And(read_filter, update_filter) if update_filter else read_filter

    # Read existing rows matching keys
    # If update_filter is provided, we need to read ALL rows matching keys (not just filtered ones)
    # to ensure rows that don't match the filter are preserved
    existing_rows = ray.data.read_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        row_filter=read_filter,  # Read all rows matching keys
    )

    # When update_filter is provided, we need to:
    # 1. Only merge rows that match the update_filter
    # 2. Preserve rows that don't match the filter
    # So we read existing rows matching the filter for merging, and separately preserve others
    if update_filter:
        # Read rows matching both keys AND update_filter for merging
        existing_rows_to_merge = ray.data.read_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs={"name": catalog_name, **catalog_kwargs},
            row_filter=read_filter_with_update,
        )
    else:
        existing_rows_to_merge = existing_rows

    # Merge new data with existing rows that match the filter
    merged = dataset.map_batches(
        lambda b: _add_priority_column(b, "_merge_priority", 1), batch_format="pyarrow"
    ).union(existing_rows_to_merge.map_batches(
        lambda b: _add_priority_column(b, "_merge_priority", 0), batch_format="pyarrow"
    )).map_batches(
        lambda b: _deduplicate_batch(b, join_columns, "_merge_priority"), batch_format="pyarrow"
    )

    # If update_filter is provided, we need to also include rows that don't match the filter
    if update_filter:
        # Read rows matching keys but NOT matching update_filter
        from pyiceberg.expressions import Not
        existing_rows_to_preserve = ray.data.read_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs={"name": catalog_name, **catalog_kwargs},
            row_filter=And(read_filter, Not(update_filter)),
        )
        merged = merged.union(existing_rows_to_preserve)

    # Write with partial overwrite - only overwrite rows matching both keys AND update_filter
    from ray.data._internal.datasource.iceberg import IcebergDatasink

    merged.write_datasink(
        IcebergDatasink(
            table_identifier=table_identifier,
            catalog_kwargs={"name": catalog_name, **catalog_kwargs},
            snapshot_properties=snapshot_properties,
            mode="overwrite",
            overwrite_filter=read_filter_with_update,  # Only overwrite rows matching keys AND update_filter
        ),
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )


def _upsert_fallback(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    join_columns: List[str],
    catalog_kwargs: Dict[str, Any],
    snapshot_properties: Dict[str, str],
    update_filter: Optional["BooleanExpression"],
    ray_remote_args: Optional[Dict[str, Any]],
    concurrency: Optional[int],
) -> None:
    """Fallback upsert for PyIceberg < 0.9.0."""
    from pyiceberg.expressions import And

    import ray

    catalog_name = catalog_kwargs.pop("name", "default")
    keys_df = dataset.select_columns(join_columns).to_pandas()
    if len(keys_df) == 0:
        logger.warning("Source dataset is empty, nothing to upsert")
        return

    read_filter = And(_build_key_filter(join_columns, keys_df), update_filter) if update_filter else _build_key_filter(join_columns, keys_df)
    existing_data = ray.data.read_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        row_filter=read_filter,
    )

    merged = dataset.map_batches(
        lambda b: _add_priority_column(b, "_source_priority", 1), batch_format="pyarrow"
    ).union(existing_data.map_batches(
        lambda b: _add_priority_column(b, "_source_priority", 0), batch_format="pyarrow"
    )).map_batches(
        lambda b: _deduplicate_batch(b, join_columns, "_source_priority"), batch_format="pyarrow"
    )

    merged.write_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="overwrite",
        overwrite_filter=read_filter,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )
