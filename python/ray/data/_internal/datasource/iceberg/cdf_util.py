"""
Utilities for Change Data Feed (CDF) operations on Iceberg tables.

Provides incremental reads and CDC write operations for Apache Iceberg tables.
See: https://py.iceberg.apache.org/ for PyIceberg documentation
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from pyiceberg.table import Table

    import ray

logger = logging.getLogger(__name__)


def _validate_table_identifier(table_identifier: str) -> None:
    """Validate that table_identifier is in 'database.table' format."""
    if not table_identifier or not isinstance(table_identifier, str):
        raise ValueError(f"table_identifier must be a non-empty string. Got: {table_identifier}")
    parts = table_identifier.split(".")
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise ValueError(f"table_identifier must be in format 'database.table'. Got: {table_identifier}")


def _validate_merge_keys(merge_keys: List[str]) -> None:
    """Validate that merge_keys is a valid list of unique, non-empty column names."""
    if not merge_keys:
        raise ValueError("merge_keys cannot be empty for CDF writes")
    if any(key is None or not isinstance(key, str) or not key.strip() for key in merge_keys):
        raise ValueError(f"merge_keys must be non-empty strings. Got: {merge_keys}")
    if len(merge_keys) != len(set(merge_keys)):
        duplicates = [k for k in set(merge_keys) if merge_keys.count(k) > 1]
        raise ValueError(f"merge_keys contains duplicates: {duplicates}")


def _read_iceberg_cdf(
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
    """Read incremental changes from an Iceberg table between two snapshots.

    Internal function used by read_iceberg() for CDF reads.

    Args:
        table_identifier: Fully qualified table identifier (e.g., "db_name.table_name")
        start_snapshot_id: Starting snapshot ID (exclusive). If None, reads from beginning.
        end_snapshot_id: Ending snapshot ID (inclusive). If None, uses current snapshot.
        start_timestamp: ISO format timestamp (e.g., "2024-01-01T00:00:00").
            Alternative to start_snapshot_id.
        end_timestamp: ISO format timestamp. Alternative to end_snapshot_id.
        catalog_kwargs: Arguments for PyIceberg catalog.load_catalog()
        ray_remote_args: kwargs passed to ray.remote
        num_cpus: CPUs per worker
        num_gpus: GPUs per worker
        memory: Heap memory per worker in bytes
        override_num_blocks: Override output block count

    Returns:
        Dataset containing rows from files added between snapshots.
    """
    _check_import(None, module="pyiceberg", package="pyiceberg")
    from pyiceberg.catalog import load_catalog

    import ray

    _validate_table_identifier(table_identifier)

    # Validate parameters
    if (start_snapshot_id is not None and start_snapshot_id < 0) or (end_snapshot_id is not None and end_snapshot_id < 0):
        raise ValueError("Snapshot IDs must be non-negative")
    if (start_timestamp and start_snapshot_id) or (end_timestamp and end_snapshot_id):
        raise ValueError("Cannot specify both timestamp and snapshot_id")
    if start_timestamp and end_timestamp:
        from datetime import datetime
        try:
            start_ts = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
            end_ts = datetime.fromisoformat(end_timestamp.replace("Z", "+00:00"))
            if start_ts > end_ts:
                raise ValueError(f"start_timestamp ({start_timestamp}) must be <= end_timestamp ({end_timestamp})")
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format. Expected ISO format. Error: {e}")

    # Load catalog and table
    catalog_kwargs = catalog_kwargs or {}
    catalog_name = catalog_kwargs.pop("name", "default")
    catalog = load_catalog(name=catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Resolve snapshot IDs from timestamps
    if start_timestamp:
        start_snapshot_id = _resolve_snapshot_from_timestamp(table, start_timestamp, before=False)
        if start_snapshot_id is None:
            raise ValueError(f"No snapshot found at or after start_timestamp {start_timestamp}")
    if end_timestamp:
        end_snapshot_id = _resolve_snapshot_from_timestamp(table, end_timestamp, before=True)
        if end_snapshot_id is None:
            raise ValueError(f"No snapshot found at or before end_timestamp {end_timestamp}")

    # Default to current snapshot if end not specified
    if end_snapshot_id is None:
        current_snapshot = table.current_snapshot()
        if current_snapshot is None:
            raise ValueError(f"Table {table_identifier} has no snapshots")
        end_snapshot_id = current_snapshot.snapshot_id

    # Validate snapshot order
    if start_snapshot_id is not None and end_snapshot_id < start_snapshot_id:
        raise ValueError(f"end_snapshot_id ({end_snapshot_id}) must be >= start_snapshot_id ({start_snapshot_id})")
    if start_snapshot_id == end_snapshot_id:
        logger.warning(f"start_snapshot_id equals end_snapshot_id ({end_snapshot_id}). Will return empty dataset.")

    # Get files added between snapshots
    added_files = _get_added_files_between_snapshots(table, start_snapshot_id, end_snapshot_id)
    added_file_paths = {str(f.file_path) for f in added_files}

    from ray.data._internal.datasource.iceberg.datasource import IcebergDatasource

    datasource = IcebergDatasource(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_id=end_snapshot_id,
    )
    filtered_tasks = [
        task for task in datasource.get_read_tasks(parallelism=-1)
        if any(fp in added_file_paths for fp in _extract_file_paths_from_task(task))
    ]

    if not filtered_tasks:
        import pyarrow as pa
        return ray.data.from_arrow(pa.Table.from_pylist([], schema=table.schema().as_arrow()))

    # Create dataset from filtered tasks
    from ray.data._internal.execution.interfaces import RefBundle
    from ray.data._internal.logical.operators.read_operator import Read
    from ray.data._internal.plan import ExecutionPlan
    from ray.data.dataset import MaterializedDataset

    return MaterializedDataset(
        ExecutionPlan(_in_blocks=RefBundle([(task.read(), task.metadata()) for task in filtered_tasks])),
        Read(datasource),
    )


def write_iceberg_cdf(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    merge_keys: List[str],
    change_type_column: str = "_change_type",
    catalog_kwargs: Optional[Dict[str, Any]] = None,
    snapshot_properties: Optional[Dict[str, str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """
    Write change data feed to an Iceberg table with mixed INSERT/UPDATE/DELETE operations.

    Applies operations from a dataset with a change_type_column:
    - "insert"/"I": Insert new rows
    - "update"/"U": Update existing rows based on merge_keys
    - "delete"/"D": Delete rows based on merge_keys

    Operations are applied in order: DELETE → UPDATE → INSERT.
    Note: Currently creates separate snapshots (not atomic across all three types).

    Args:
        dataset: Ray Dataset with change type column
        table_identifier: Fully qualified table identifier (e.g., "db_name.table_name")
        merge_keys: Column names for UPDATE/DELETE operations
        change_type_column: Column name for change types. Defaults to "_change_type".
        catalog_kwargs: Arguments for PyIceberg catalog.load_catalog()
        snapshot_properties: Custom properties for snapshot
        ray_remote_args: kwargs passed to ray.remote
        concurrency: Maximum concurrent Ray tasks

    Returns:
        None. Check Iceberg snapshots for operation details.
    """
    _check_import(None, module="pyiceberg", package="pyiceberg")
    from pyiceberg.catalog import load_catalog

    _validate_merge_keys(merge_keys)
    _validate_table_identifier(table_identifier)

    schema = dataset.schema()
    if change_type_column not in schema.names:
        raise ValueError(
            f"Change type column '{change_type_column}' not found. Available: {schema.names}"
        )

    # Load catalog and table
    catalog_kwargs = catalog_kwargs or {}
    catalog_name = catalog_kwargs.pop("name", "default")
    catalog = load_catalog(name=catalog_name, **catalog_kwargs)
    table = catalog.load_table(table_identifier)

    # Validate schemas
    table_columns = {field.name for field in table.schema().fields}
    missing_in_table = set(merge_keys) - table_columns
    missing_in_dataset = set(merge_keys) - set(schema.names)
    if missing_in_table:
        raise ValueError(f"merge_keys {missing_in_table} not in table. Available: {sorted(table_columns)}")
    if missing_in_dataset:
        raise ValueError(f"merge_keys {missing_in_dataset} not in dataset. Available: {sorted(schema.names)}")

    # Validate change_type_column values
    try:
        first_batch = dataset.take(limit=100)
        if first_batch:
            valid_values = {"INSERT", "I", "UPDATE", "U", "DELETE", "D"}
            invalid = [
                str(row.get(change_type_column, "")).strip().upper()
                for row in first_batch
                if str(row.get(change_type_column, "")).strip().upper() not in valid_values
            ]
            if invalid:
                examples = list(set(invalid))[:10]
                raise ValueError(
                    f"Invalid change_type_column values: {examples}. "
                    f"Valid: 'insert'/'I', 'update'/'U', 'delete'/'D'"
                )
    except ValueError as e:
        if "Invalid change_type_column" in str(e):
            raise
        logger.warning(f"Could not validate change_type_column values: {e}")

    # Separate by change type and remove change_type_column
    def split_by_change_type(row):
        ct = str(row.get(change_type_column, "")).strip().upper()
        return ct in ["DELETE", "D"], ct in ["UPDATE", "U"], ct in ["INSERT", "I"]

    def drop_change_column(batch):
        import pyarrow as pa
        if isinstance(batch, pa.Table):
            return batch.drop([change_type_column]) if change_type_column in batch.schema.names else batch
        return batch.drop(columns=[change_type_column]) if change_type_column in batch.columns else batch

    deletes_clean = dataset.filter(lambda r: split_by_change_type(r)[0]).map_batches(drop_change_column, batch_format="pyarrow")
    updates_clean = dataset.filter(lambda r: split_by_change_type(r)[1]).map_batches(drop_change_column, batch_format="pyarrow")
    inserts_clean = dataset.filter(lambda r: split_by_change_type(r)[2]).map_batches(drop_change_column, batch_format="pyarrow")

    # Process in order: DELETE → UPDATE → INSERT
    # Note: Creates separate snapshots (not atomic across all three types)
    _apply_deletes(table, deletes_clean, merge_keys, snapshot_properties, ray_remote_args)

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

    inserts_clean.write_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs},
        snapshot_properties=snapshot_properties,
        mode="append",
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )


def _resolve_snapshot_from_timestamp(table: "Table", timestamp: str, before: bool = True) -> Optional[int]:
    """Resolve snapshot ID from ISO format timestamp."""
    from datetime import datetime

    if not timestamp or not isinstance(timestamp, str):
        raise ValueError(f"timestamp must be non-empty ISO format string. Got: {timestamp}")

    try:
        target_ms = int(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp() * 1000)
    except (ValueError, AttributeError) as e:
        raise ValueError(f"Invalid timestamp format '{timestamp}'. Expected ISO format. Error: {e}")

    snapshots = table.metadata.snapshots
    if not snapshots:
        return None

    sorted_snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms)
    iterator = reversed(sorted_snapshots) if before else sorted_snapshots
    for snapshot in iterator:
        if (before and snapshot.timestamp_ms <= target_ms) or (not before and snapshot.timestamp_ms >= target_ms):
            return snapshot.snapshot_id
    return None


def _get_added_files_between_snapshots(
    table: "Table", start_snapshot_id: Optional[int], end_snapshot_id: int
) -> List[Any]:
    """Get list of data files added between two snapshots."""
    entries_list = table.inspect.entries(snapshot_id=end_snapshot_id).to_pylist()
    if start_snapshot_id is None:
        return [entry["data_file"] for entry in entries_list]
    return [entry["data_file"] for entry in entries_list if entry["snapshot_id"] > start_snapshot_id]


def _extract_file_paths_from_task(task: Any) -> List[str]:
    """Extract file paths from a read task."""
    if hasattr(task, "file"):
        return [str(task.file.file_path)]
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
    """Apply delete operations using PyIceberg delete API."""
    from ray.data._internal.datasource.iceberg.upsert_util import _build_key_filter

    keys_df = deletes_dataset.select_columns(merge_keys).to_pandas()
    if len(keys_df) == 0:
        return

    delete_filter = _build_key_filter(merge_keys, keys_df)
    table.delete(delete_filter=delete_filter)
