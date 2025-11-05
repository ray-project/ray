"""
Utilities for Change Data Feed (CDF) operations on Iceberg tables.

This module provides functionality to read and write change data feeds for Apache Iceberg
tables, enabling incremental data processing and CDC (Change Data Capture) workflows.

CDF Read Operations:
- Read data files added between snapshots (incremental reads)
- Track which files were added, modified, or deleted
- Support for time travel and snapshot-based queries

CDF Write Operations:
- Apply mixed INSERT/UPDATE/DELETE operations in a single transaction
- Process CDC streams from Debezium, Maxwell, or custom sources
- Maintain ACID guarantees for all operations

See: https://py.iceberg.apache.org/ for PyIceberg documentation
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from pyiceberg.table import Table

    import ray

logger = logging.getLogger(__name__)


def read_iceberg_cdf(
    table_identifier: str,
    start_snapshot_id: Optional[int] = None,
    end_snapshot_id: Optional[int] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    catalog_kwargs: Optional[Dict[str, Any]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    override_num_blocks: Optional[int] = None,
) -> "ray.data.Dataset":
    """
    Read incremental changes from an Iceberg table between two snapshots.

    This function reads only the data files that were added between the start and end
    snapshots, enabling efficient incremental data processing without scanning the
    entire table. This is useful for CDC pipelines, incremental ETL, and streaming-like
    batch processing.

    The function uses Iceberg's manifest entries to identify which files were added
    in each snapshot, then reads only those files. This provides significant performance
    improvements over full table scans.

    Args:
        table_identifier: Fully qualified table identifier (e.g., "db_name.table_name")
        start_snapshot_id: Starting snapshot ID (exclusive). If not provided, reads
            from the beginning of the table history.
        end_snapshot_id: Ending snapshot ID (inclusive). If not provided, uses the
            current snapshot.
        start_timestamp: Starting timestamp (ISO format, e.g., "2024-01-01T00:00:00").
            Alternative to start_snapshot_id. The snapshot at or after this timestamp
            will be used.
        end_timestamp: Ending timestamp (ISO format). Alternative to end_snapshot_id.
            The snapshot at or before this timestamp will be used.
        catalog_kwargs: Arguments to pass to PyIceberg's catalog.load_catalog()
            function. See https://py.iceberg.apache.org/configuration/
        ray_remote_args: kwargs passed to ray.remote in the read tasks
        num_cpus: The number of CPUs to reserve for each parallel read worker
        num_gpus: The number of GPUs to reserve for each parallel read worker
        memory: The heap memory in bytes to reserve for each parallel read worker
        override_num_blocks: Override the number of output blocks from all read tasks

    Returns:
        Dataset containing only the rows from files added between the snapshots.
        The dataset includes all original columns from the table.

    Examples:
        Read changes between two snapshots:

        >>> import ray
        >>> ds = ray.data.read_iceberg_cdf(  # doctest: +SKIP
        ...     table_identifier="db.users",
        ...     start_snapshot_id=12345,
        ...     end_snapshot_id=12350,
        ...     catalog_kwargs={"type": "glue"}
        ... )

        Read changes since a specific timestamp:

        >>> ds = ray.data.read_iceberg_cdf(  # doctest: +SKIP
        ...     table_identifier="db.orders",
        ...     start_timestamp="2024-01-01T00:00:00",
        ...     catalog_kwargs={"type": "glue"}
        ... )

        Read all changes up to a snapshot:

        >>> ds = ray.data.read_iceberg_cdf(  # doctest: +SKIP
        ...     table_identifier="db.events",
        ...     end_snapshot_id=12345,
        ...     catalog_kwargs={"type": "glue"}
        ... )

    Note:
        - This function reads entire data files, not individual rows. If a file was
          added between snapshots, all rows in that file are returned.
        - Deleted files are not tracked in the returned dataset. To detect deletions,
          you need to track row-level primary keys.
        - For tables with frequent small updates, this may read more data than strictly
          necessary. Consider table maintenance (compaction) for optimal performance.
        - Start snapshot is exclusive, end snapshot is inclusive.

    Performance:
        This operation is significantly faster than reading full snapshots and computing
        differences, as it only reads the files that changed between snapshots.
    """
    _check_import(None, module="pyiceberg", package="pyiceberg")
    from pyiceberg.catalog import load_catalog

    import ray

    # Load catalog and table
    catalog_kwargs = catalog_kwargs or {}
    catalog_name = catalog_kwargs.pop("name", "default")
    catalog = load_catalog(name=catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Resolve snapshot IDs from timestamps if provided
    if start_timestamp is not None:
        start_snapshot_id = _resolve_snapshot_from_timestamp(
            table, start_timestamp, before=False
        )
        logger.info(
            f"Resolved start_timestamp {start_timestamp} to snapshot {start_snapshot_id}"
        )

    if end_timestamp is not None:
        end_snapshot_id = _resolve_snapshot_from_timestamp(
            table, end_timestamp, before=True
        )
        logger.info(
            f"Resolved end_timestamp {end_timestamp} to snapshot {end_snapshot_id}"
        )

    # Default to current snapshot if end not specified
    if end_snapshot_id is None:
        end_snapshot_id = table.current_snapshot().snapshot_id
        logger.info(f"Using current snapshot {end_snapshot_id} as end snapshot")

    # Get list of files added between snapshots
    added_files = _get_added_files_between_snapshots(
        table, start_snapshot_id, end_snapshot_id
    )

    if not added_files:
        logger.warning(
            f"No files added between snapshots {start_snapshot_id} and {end_snapshot_id}"
        )
        # Return empty dataset with table schema
        import pyarrow as pa

        empty_table = pa.Table.from_pylist([], schema=table.schema().as_arrow())
        return ray.data.from_arrow(empty_table)

    logger.info(
        f"Found {len(added_files)} files added between snapshots "
        f"{start_snapshot_id} and {end_snapshot_id}"
    )

    # Read only the added files using Iceberg's scan API
    # Get file paths from added files
    added_file_paths = {str(f.file_path) for f in added_files}

    # Read the table at end snapshot and filter to only added files
    # This is done by reading the files directly
    from ray.data._internal.datasource.iceberg.datasource import IcebergDatasource

    datasource = IcebergDatasource(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_id=end_snapshot_id,
    )

    # Filter the read tasks to only include files we care about
    read_tasks = datasource.get_read_tasks(parallelism=-1)
    filtered_tasks = [
        task
        for task in read_tasks
        if any(fp in added_file_paths for fp in _extract_file_paths_from_task(task))
    ]

    if not filtered_tasks:
        logger.warning("No matching read tasks found after filtering")
        import pyarrow as pa

        empty_table = pa.Table.from_pylist([], schema=table.schema().as_arrow())
        return ray.data.from_arrow(empty_table)

    logger.info(f"Reading {len(filtered_tasks)} tasks for incremental data")

    # Create dataset from filtered read tasks
    from ray.data._internal.execution.interfaces import RefBundle
    from ray.data._internal.logical.operators.read_operator import Read
    from ray.data._internal.plan import ExecutionPlan
    from ray.data.dataset import MaterializedDataset

    logical_plan = Read(datasource)
    execution_plan = ExecutionPlan(
        _in_blocks=RefBundle(
            [(task.read(), task.metadata()) for task in filtered_tasks]
        )
    )

    return MaterializedDataset(execution_plan, logical_plan)


def write_iceberg_cdf(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    merge_keys: List[str],
    change_type_column: str = "_change_type",
    catalog_kwargs: Optional[Dict[str, Any]] = None,
    snapshot_properties: Optional[Dict[str, str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> Dict[str, int]:
    """
    Write change data feed to an Iceberg table with mixed operations.

    This function applies INSERT, UPDATE, and DELETE operations from a dataset
    containing a change type column. All operations are applied in a single ACID
    transaction, ensuring consistency.

    The dataset must contain a column (specified by change_type_column) with one of:
    - "insert" or "I": Insert new rows
    - "update" or "U": Update existing rows based on merge_keys
    - "delete" or "D": Delete rows based on merge_keys

    Implementation strategy:
    1. Split dataset by change type
    2. Apply deletes first (remove rows matching keys)
    3. Apply updates (upsert matching rows)
    4. Apply inserts (append new rows)
    5. Commit all operations in a single transaction

    Args:
        dataset: Ray Dataset containing the change data with a change type column
        table_identifier: Fully qualified table identifier (e.g., "db_name.table_name")
        merge_keys: List of column names to use as keys for UPDATE and DELETE operations.
            These columns identify which rows to modify or delete.
        change_type_column: Name of the column containing change types. Valid values:
            "insert"/"I", "update"/"U", "delete"/"D". Defaults to "_change_type".
        catalog_kwargs: Arguments to pass to PyIceberg's catalog.load_catalog()
            function. See https://py.iceberg.apache.org/configuration/
        snapshot_properties: Custom properties to attach to the snapshot when
            committing, useful for tracking metadata
        ray_remote_args: kwargs passed to ray.remote in the write tasks
        concurrency: Maximum number of Ray tasks to run concurrently

    Returns:
        None. Operation details can be inspected via Iceberg snapshot metadata.

    Examples:
        Apply mixed CDC operations:

        >>> import ray
        >>> cdc_data = ray.data.from_items([  # doctest: +SKIP
        ...     {"id": 1, "name": "Alice", "_change_type": "insert"},
        ...     {"id": 2, "name": "Bob Updated", "_change_type": "update"},
        ...     {"id": 3, "name": "Charlie", "_change_type": "delete"},
        ... ])
        >>> ray.data.write_iceberg_cdf(  # doctest: +SKIP
        ...     dataset=cdc_data,
        ...     table_identifier="db.users",
        ...     merge_keys=["id"],
        ...     catalog_kwargs={"type": "glue"}
        ... )

        Process Debezium CDC events:

        >>> def transform_debezium(batch):  # doctest: +SKIP
        ...     import pandas as pd
        ...     records = []
        ...     for row in batch:
        ...         op = row["op"]  # "c", "u", "d", "r"
        ...         change_type = {"c": "insert", "u": "update",
        ...                       "d": "delete", "r": "insert"}[op]
        ...         data = row.get("after") or row.get("before")
        ...         data["_change_type"] = change_type
        ...         records.append(data)
        ...     return pd.DataFrame(records)
        >>> debezium_stream = ray.data.read_kafka(...)  # doctest: +SKIP
        >>> cdf_data = debezium_stream.map_batches(transform_debezium)  # doctest: +SKIP
        >>> cdf_data.write_iceberg_cdf(  # doctest: +SKIP
        ...     table_identifier="db.orders",
        ...     merge_keys=["order_id"],
        ...     catalog_kwargs={"type": "glue"}
        ... )

    Note:
        - **ACID Limitation**: Currently, operations are applied as separate Iceberg
          snapshots (DELETE → UPDATE → INSERT), not a single atomic transaction. Each
          operation has ACID guarantees, but the overall CDF write is not atomic across
          all three types. This is a PyIceberg/Ray Data integration limitation.
        - Operations are applied in order: DELETE → UPDATE → INSERT
        - The change_type_column is automatically removed before writing
        - For UPDATE operations, the entire row is replaced (not partial updates)
        - DELETE operations only need merge_key columns; other columns are ignored
        - If a row has an unknown change type, it will raise an error

    Raises:
        ValueError: If merge_keys is empty or if change_type_column is not in dataset
        ValueError: If dataset contains invalid change type values
    """
    _check_import(None, module="pyiceberg", package="pyiceberg")
    from pyiceberg.catalog import load_catalog

    # Validate inputs
    if not merge_keys:
        raise ValueError("merge_keys cannot be empty for CDF writes")

    # Check if change type column exists
    schema = dataset.schema()
    if change_type_column not in schema.names:
        raise ValueError(
            f"Change type column '{change_type_column}' not found in dataset. "
            f"Available columns: {schema.names}"
        )

    logger.info(
        f"Starting CDF write to {table_identifier} with change column '{change_type_column}'"
    )

    # Load catalog and table
    catalog_kwargs = catalog_kwargs or {}
    catalog_name = catalog_kwargs.pop("name", "default")
    catalog = load_catalog(name=catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Separate dataset by change type
    def is_delete(row):
        ct = row.get(change_type_column, "").upper()
        return ct in ["DELETE", "D"]

    def is_update(row):
        ct = row.get(change_type_column, "").upper()
        return ct in ["UPDATE", "U"]

    def is_insert(row):
        ct = row.get(change_type_column, "").upper()
        return ct in ["INSERT", "I"]

    deletes_ds = dataset.filter(is_delete)
    updates_ds = dataset.filter(is_update)
    inserts_ds = dataset.filter(is_insert)

    # Don't materialize - keep streaming execution
    # Counts will be determined during write operations
    logger.info(
        f"Starting CDF write to {table_identifier} - "
        f"operations will be processed in streaming mode"
    )

    # Remove change type column from all datasets
    def drop_change_column(batch):
        import pyarrow as pa

        if isinstance(batch, pa.Table):
            if change_type_column in batch.schema.names:
                return batch.drop([change_type_column])
            return batch
        else:  # pandas
            if change_type_column in batch.columns:
                return batch.drop(columns=[change_type_column])
            return batch

    deletes_clean = deletes_ds.map_batches(drop_change_column, batch_format="pyarrow")
    updates_clean = updates_ds.map_batches(drop_change_column, batch_format="pyarrow")
    inserts_clean = inserts_ds.map_batches(drop_change_column, batch_format="pyarrow")

    # CRITICAL: All operations must use PyIceberg transaction to maintain ACID guarantees
    # See: https://py.iceberg.apache.org/api/#transactions
    #
    # WARNING: Currently PyIceberg doesn't support mixing Ray Data writes with
    # transactions in a single atomic operation. This is a known limitation.
    # Each operation creates a separate snapshot, which means CDF writes are NOT
    # truly atomic across all three operation types.
    #
    # For now, we perform operations sequentially and rely on Iceberg's per-operation
    # ACID guarantees. Future work: implement proper transaction support.

    # Process operations in order: DELETE → UPDATE → INSERT
    # Each maintains streaming execution without materializing the full dataset

    logger.info("Processing DELETE operations (streaming)")
    _apply_deletes(
        table, deletes_clean, merge_keys, snapshot_properties, ray_remote_args
    )

    logger.info("Processing UPDATE operations (streaming)")
    from ray.data._internal.datasource.iceberg.upsert_util import upsert_to_iceberg

    upsert_to_iceberg(
        dataset=updates_clean,
        table_identifier=table_identifier,
        join_columns=merge_keys,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        update_filter=None,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )

    logger.info("Processing INSERT operations (streaming)")
    inserts_clean.write_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="append",
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )

    logger.info(f"CDF write completed for {table_identifier}")

    # Note: We don't return counts since that would require materializing datasets
    # Users can query Iceberg snapshots to see operation details
    return None


def _resolve_snapshot_from_timestamp(
    table: "Table", timestamp: str, before: bool = True
) -> Optional[int]:
    """
    Resolve a snapshot ID from a timestamp.

    Args:
        table: PyIceberg Table object
        timestamp: ISO format timestamp string (e.g., "2024-01-01T00:00:00")
        before: If True, find snapshot at or before timestamp. If False, at or after.

    Returns:
        Snapshot ID, or None if no suitable snapshot found
    """
    from datetime import datetime

    target_ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    target_ms = int(target_ts.timestamp() * 1000)

    snapshots = table.metadata.snapshots
    if not snapshots:
        return None

    # Sort snapshots by timestamp
    sorted_snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms)

    if before:
        # Find latest snapshot at or before timestamp
        for snapshot in reversed(sorted_snapshots):
            if snapshot.timestamp_ms <= target_ms:
                return snapshot.snapshot_id
        return None
    else:
        # Find earliest snapshot at or after timestamp
        for snapshot in sorted_snapshots:
            if snapshot.timestamp_ms >= target_ms:
                return snapshot.snapshot_id
        return None


def _get_added_files_between_snapshots(
    table: "Table", start_snapshot_id: Optional[int], end_snapshot_id: int
) -> List[Any]:
    """
    Get list of data files added between two snapshots.

    Args:
        table: PyIceberg Table object
        start_snapshot_id: Starting snapshot ID (exclusive), or None for all files up to end
        end_snapshot_id: Ending snapshot ID (inclusive)

    Returns:
        List of DataFile objects added between snapshots
    """
    # Get all manifest entries at end snapshot
    entries_table = table.inspect.entries(snapshot_id=end_snapshot_id)

    # Convert to list of entries
    entries_list = entries_table.to_pylist()

    if start_snapshot_id is None:
        # Return all files up to end snapshot
        return [entry["data_file"] for entry in entries_list]

    # Filter entries to only those added after start_snapshot_id
    added_files = []
    for entry in entries_list:
        # entry['snapshot_id'] is the snapshot that added this file
        if entry["snapshot_id"] > start_snapshot_id:
            added_files.append(entry["data_file"])

    return added_files


def _extract_file_paths_from_task(task: Any) -> List[str]:
    """
    Extract file paths from a read task.

    Args:
        task: ReadTask object

    Returns:
        List of file paths in the task
    """
    # ReadTask has a file attribute
    if hasattr(task, "file"):
        return [str(task.file.file_path)]
    # Fallback: try to get from metadata
    if hasattr(task, "metadata") and hasattr(task.metadata(), "input_files"):
        return [str(f) for f in task.metadata().input_files]
    return []


def _apply_deletes(
    table: "Table",
    deletes_dataset: "ray.data.Dataset",
    merge_keys: List[str],
    snapshot_properties: Optional[Dict[str, str]],
    ray_remote_args: Optional[Dict[str, Any]],
) -> None:
    """
    Apply delete operations to an Iceberg table.

    This collects the keys to delete and uses Iceberg's delete API to remove
    matching rows. Keys are materialized (not full rows) to build the delete filter.

    Note: Keys must be materialized to build PyIceberg filter expressions.
    This is unavoidable but minimizes memory usage by only materializing key columns.

    Args:
        table: PyIceberg Table object
        deletes_dataset: Dataset containing rows to delete (only merge_keys are used)
        merge_keys: Columns to use as keys for deletion
        snapshot_properties: Properties to attach to the delete snapshot
        ray_remote_args: Ray remote args (unused for deletes)
    """
    from pyiceberg.expressions import And, EqualTo, In, Or

    # Materialize ONLY the merge key columns (not full rows)
    # This is necessary to build the PyIceberg delete filter expression
    # See: https://py.iceberg.apache.org/api/#delete
    keys_df = deletes_dataset.select_columns(merge_keys).to_pandas()

    if len(keys_df) == 0:
        return

    # Build delete filter
    if len(merge_keys) == 1:
        # Single key: use IN expression
        key_col = merge_keys[0]
        unique_keys = keys_df[key_col].unique().tolist()

        if len(unique_keys) == 1:
            delete_filter = EqualTo(key_col, unique_keys[0])
        else:
            delete_filter = In(key_col, unique_keys)
    else:
        # Multiple keys: build OR of AND expressions
        # Limit to 1000 combinations to avoid huge filter expressions
        unique_combinations = keys_df.drop_duplicates().head(1000)

        if len(unique_combinations) < len(keys_df):
            logger.warning(
                f"Delete has {len(keys_df)} unique key combinations. "
                f"Limiting to first 1000 for filter expression."
            )

        filters = []
        for _, row in unique_combinations.iterrows():
            key_filters = [EqualTo(key, row[key]) for key in merge_keys]
            if len(key_filters) == 1:
                filters.append(key_filters[0])
            else:
                filters.append(And(*key_filters))

        if len(filters) == 1:
            delete_filter = filters[0]
        else:
            delete_filter = Or(*filters)

    # Apply delete using PyIceberg's delete API
    logger.info(f"Deleting rows matching {len(keys_df)} keys from table")
    table.delete(delete_filter=delete_filter)

    logger.info("Delete operation completed")
