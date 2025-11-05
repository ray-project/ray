"""
Utility functions for Iceberg merge and upsert operations in Ray Data.

This module provides convenience functions for performing MERGE INTO and UPSERT operations
on Iceberg tables using PyIceberg's native upsert capabilities (PyIceberg 0.9.0+).

For older versions of PyIceberg, it falls back to a read-merge-write pattern.

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

    # Multi-column keys: build OR of AND expressions
    unique_combinations = keys_df.drop_duplicates().head(1000)
    if len(unique_combinations) < len(keys_df):
        logger.warning(
            f"Upsert has {len(keys_df)} unique key combinations. "
            "Limiting to first 1000 for filter expression."
        )

    filters = []
    for _, row in unique_combinations.iterrows():
        key_filters = [EqualTo(key, row[key]) for key in join_columns]
        filters.append(key_filters[0] if len(key_filters) == 1 else And(*key_filters))

    return filters[0] if len(filters) == 1 else Or(*filters)


def _deduplicate_rows(join_columns: List[str], priority_col: str = "_merge_priority"):
    """Create deduplication function that keeps row with highest priority."""

    def deduplicate(batch):
        import pyarrow as pa

        if not isinstance(batch, pa.Table):
            batch = pa.Table.from_pandas(batch)

        if len(batch) == 0:
            return batch.drop([priority_col]) if priority_col in batch.schema.names else batch

        df = batch.to_pandas(zero_copy_only=False, self_destruct=True)
        df.sort_values(priority_col, ascending=False, inplace=True, kind="mergesort")
        df.drop_duplicates(subset=join_columns, keep="first", inplace=True)
        df.drop(columns=[priority_col], inplace=True)

        return pa.Table.from_pandas(df, schema=batch.drop([priority_col]).schema)

    return deduplicate


def _add_priority_column(priority_col: str = "_merge_priority"):
    """Create function to add priority column to batches."""

    def add_priority(batch, priority: int):
        import pyarrow as pa

        if isinstance(batch, pa.Table):
            return batch.append_column(
                priority_col, pa.array([priority] * len(batch), type=pa.int8())
            )
        batch[priority_col] = priority
        return batch

    return add_priority


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

    Examples:
        >>> new_data.write_iceberg(  # doctest: +SKIP
        ...     table_identifier="db.customers",
        ...     mode="merge",
        ...     merge_keys=["customer_id"],
        ...     catalog_kwargs={"type": "glue"}
        ... )
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
    from pyiceberg.expressions import And, In

    import ray

    catalog_name = catalog_kwargs.pop("name", "default")
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Validate schemas
    dataset_schema = dataset.schema()
    source_columns = set(dataset_schema.names)
    table_columns = {field.name for field in table.schema().fields}

    missing_in_source = set(join_columns) - source_columns
    missing_in_target = set(join_columns) - table_columns
    if missing_in_source:
        raise ValueError(
            f"Join columns {missing_in_source} not found in source dataset. "
            f"Available: {sorted(source_columns)}"
        )
    if missing_in_target:
        raise ValueError(
            f"Join columns {missing_in_target} not found in target table. "
            f"Available: {sorted(table_columns)}"
        )

    # Check partition alignment
    partition_fields = table.spec().fields
    if partition_fields:
        partition_column_names = {
            table.schema().find_column_name(field.source_id)
            for field in partition_fields
        }
        if not set(join_columns).issubset(partition_column_names):
            logger.warning(
                f"Merge keys {join_columns} don't match partition columns "
                f"{sorted(partition_column_names)}. This may cause more data files "
                "to be rewritten than necessary."
            )

    # Collect keys and build filter
    source_keys_table = dataset.select_columns(join_columns).to_arrow()
    if len(join_columns) == 1:
        col_name = join_columns[0]
        source_keys = set(source_keys_table[col_name].to_pylist())
        read_filter = In(col_name, list(source_keys))
    else:
        # For multi-column keys, convert to pandas for filter building
        keys_df = source_keys_table.to_pandas()
        read_filter = _build_key_filter(join_columns, keys_df)

    read_filter_with_update = And(read_filter, update_filter) if update_filter else read_filter

    # Read existing matching rows
    existing_rows = ray.data.read_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        row_filter=read_filter_with_update,
    )

    # Merge and deduplicate
    add_priority = _add_priority_column()
    new_data_marked = dataset.map_batches(
        lambda batch: add_priority(batch, 1), batch_format="pyarrow"
    )
    existing_data_marked = existing_rows.map_batches(
        lambda batch: add_priority(batch, 0), batch_format="pyarrow"
    )
    merged_data = new_data_marked.union(existing_data_marked)
    deduped_data = merged_data.map_batches(_deduplicate_rows(join_columns), batch_format="pyarrow")

    # Write with partial overwrite
    from ray.data._internal.datasource.iceberg import IcebergDatasink

    datasink = IcebergDatasink(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="overwrite",
        overwrite_filter=read_filter,
    )
    deduped_data.write_datasink(datasink, ray_remote_args=ray_remote_args, concurrency=concurrency)


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

    # Collect keys and build filter
    keys_df = dataset.select_columns(join_columns).to_pandas()
    if len(keys_df) == 0:
        logger.warning("Source dataset is empty, nothing to upsert")
        return

    read_filter = _build_key_filter(join_columns, keys_df)
    if update_filter:
        read_filter = And(read_filter, update_filter)

    # Read existing matching rows
    existing_data = ray.data.read_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        row_filter=read_filter,
    )

    # Merge and deduplicate (same logic as native)
    add_priority = _add_priority_column("_source_priority")
    new_data_marked = dataset.map_batches(
        lambda batch: add_priority(batch, 1)
    )
    existing_data_marked = existing_data.map_batches(
        lambda batch: add_priority(batch, 0)
    )
    merged = new_data_marked.union(existing_data_marked)
    deduped = merged.map_batches(_deduplicate_rows(join_columns, "_source_priority"), batch_format="pyarrow")

    # Write with partial overwrite
    deduped.write_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="overwrite",
        overwrite_filter=read_filter,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )
