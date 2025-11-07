"""
Utilities for Change Data Feed (CDF) operations on Iceberg tables.

Provides incremental reads and CDC write operations for Apache Iceberg tables.

This module uses PyIceberg to interact with Iceberg tables:
- https://py.iceberg.apache.org/ - PyIceberg documentation
- https://iceberg.apache.org/docs/latest/ - Apache Iceberg specification

For CDF reads, uses PyIceberg's table.inspect.entries() API to identify files
added between snapshots. See:
https://py.iceberg.apache.org/api_reference/table/#pyiceberg.table.Table.inspect
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


def _validate_cdf_read_params(
    start_snapshot_id: Optional[int],
    end_snapshot_id: Optional[int],
    start_timestamp: Optional[str],
    end_timestamp: Optional[str],
) -> None:
    """Validate parameters for CDF read operations."""
    if start_snapshot_id is not None:
        if not isinstance(start_snapshot_id, int):
            raise ValueError(f"start_snapshot_id must be an integer, got {type(start_snapshot_id).__name__}")
        if start_snapshot_id < 0:
            raise ValueError("start_snapshot_id must be non-negative")
    if end_snapshot_id is not None:
        if not isinstance(end_snapshot_id, int):
            raise ValueError(f"end_snapshot_id must be an integer, got {type(end_snapshot_id).__name__}")
        if end_snapshot_id < 0:
            raise ValueError("end_snapshot_id must be non-negative")
    if (start_timestamp and start_snapshot_id) or (end_timestamp and end_snapshot_id):
        raise ValueError("Cannot specify both timestamp and snapshot_id")
    if start_timestamp and end_timestamp:
        from datetime import datetime
        start_ts = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
        end_ts = datetime.fromisoformat(end_timestamp.replace("Z", "+00:00"))
        if start_ts > end_ts:
            raise ValueError(f"start_timestamp ({start_timestamp}) must be <= end_timestamp ({end_timestamp})")


def _load_iceberg_table(
    table_identifier: str, catalog_kwargs: Optional[Dict[str, Any]]
) -> tuple[Any, Any, str, Dict[str, Any]]:
    """Load Iceberg catalog and table.

    Uses PyIceberg's catalog.load_catalog() and catalog.load_table() APIs.
    See: https://py.iceberg.apache.org/api_reference/catalog/

    Args:
        table_identifier: Fully qualified table identifier
        catalog_kwargs: Arguments for PyIceberg catalog.load_catalog()

    Returns:
        Tuple of (catalog, table, catalog_name, catalog_kwargs_copy)
    """
    from pyiceberg.catalog import load_catalog

    catalog_kwargs = catalog_kwargs or {}
    catalog_kwargs_copy = catalog_kwargs.copy()
    catalog_name = catalog_kwargs_copy.pop("name", "default")
    catalog = load_catalog(name=catalog_name, **catalog_kwargs_copy)
    table = catalog.load_table(table_identifier)
    return catalog, table, catalog_name, catalog_kwargs_copy


def _resolve_snapshot_ids(
    table: "Table",
    start_snapshot_id: Optional[int],
    end_snapshot_id: Optional[int],
    start_timestamp: Optional[str],
    end_timestamp: Optional[str],
    table_identifier: str,
) -> tuple[Optional[int], int]:
    """Resolve and validate snapshot IDs for CDF read.

    Args:
        table: PyIceberg Table object
        start_snapshot_id: Starting snapshot ID (exclusive)
        end_snapshot_id: Ending snapshot ID (inclusive)
        start_timestamp: ISO format timestamp for start
        end_timestamp: ISO format timestamp for end
        table_identifier: Table identifier for error messages

    Returns:
        Tuple of (start_snapshot_id, end_snapshot_id)
    """
    if start_timestamp:
        start_snapshot_id = _resolve_snapshot_from_timestamp(table, start_timestamp, before=False)
        if start_snapshot_id is None:
            raise ValueError(f"No snapshot found at or after start_timestamp {start_timestamp}")
    if end_timestamp:
        end_snapshot_id = _resolve_snapshot_from_timestamp(table, end_timestamp, before=True)
        if end_snapshot_id is None:
            raise ValueError(f"No snapshot found at or before end_timestamp {end_timestamp}")

    if end_snapshot_id is None:
        current_snapshot = table.current_snapshot()
        if current_snapshot is None:
            raise ValueError(f"Table {table_identifier} has no snapshots")
        end_snapshot_id = current_snapshot.snapshot_id

    if start_snapshot_id is not None and end_snapshot_id < start_snapshot_id:
        raise ValueError(f"end_snapshot_id ({end_snapshot_id}) must be >= start_snapshot_id ({start_snapshot_id})")
    if start_snapshot_id == end_snapshot_id:
        logger.warning(f"start_snapshot_id equals end_snapshot_id ({end_snapshot_id}). Will return empty dataset.")

    return start_snapshot_id, end_snapshot_id


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
    import ray

    _validate_table_identifier(table_identifier)
    _validate_cdf_read_params(start_snapshot_id, end_snapshot_id, start_timestamp, end_timestamp)

    catalog, table, catalog_name, catalog_kwargs_copy = _load_iceberg_table(table_identifier, catalog_kwargs)

    start_snapshot_id, end_snapshot_id = _resolve_snapshot_ids(
        table, start_snapshot_id, end_snapshot_id, start_timestamp, end_timestamp, table_identifier
    )

    # Get files added between snapshots using PyIceberg's inspect API
    # See: https://py.iceberg.apache.org/api_reference/table/#pyiceberg.table.Table.inspect
    added_files = _get_added_files_between_snapshots(table, start_snapshot_id, end_snapshot_id)
    added_file_paths = {str(f.file_path) for f in added_files}

    from ray.data._internal.datasource.iceberg.datasource import IcebergDatasource

    # Create datasource for end snapshot to get all read tasks
    datasource = IcebergDatasource(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs_copy},
        snapshot_id=end_snapshot_id,
    )
    filtered_tasks = [
        task for task in datasource.get_read_tasks(parallelism=-1)
        if any(fp in added_file_paths for fp in _extract_file_paths_from_task(task))
    ]

    if not filtered_tasks:
        import pyarrow as pa
        return ray.data.from_arrow(pa.Table.from_pylist([], schema=table.schema().as_arrow()))

    # Create filtered datasource wrapper that returns only matching tasks
    from ray.data.read_api import read_datasource

    class FilteredIcebergDatasource(IcebergDatasource):
        """Wrapper that filters tasks to only include files added between snapshots."""

        def __init__(self, base_datasource, filtered_tasks):
            # Copy all attributes from base datasource to maintain compatibility
            self._scan_kwargs = base_datasource._scan_kwargs
            self._catalog_kwargs = base_datasource._catalog_kwargs
            self._catalog_name = base_datasource._catalog_name
            self.table_identifier = base_datasource.table_identifier
            self._row_filter = base_datasource._row_filter
            self._selected_fields = base_datasource._selected_fields
            self._plan_files = None
            self._table = base_datasource._table
            self._filtered_tasks = filtered_tasks

        def get_read_tasks(self, parallelism: int, per_task_row_limit: Optional[int] = None):
            # Return only the filtered tasks
            # Note: parallelism parameter is ignored since tasks are pre-filtered
            # per_task_row_limit is already set on the tasks when they were created
            return self._filtered_tasks

    filtered_datasource = FilteredIcebergDatasource(datasource, filtered_tasks)
    return read_datasource(
        datasource=filtered_datasource,
        parallelism=-1,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        override_num_blocks=override_num_blocks,
        ray_remote_args=ray_remote_args,
    )


def _write_iceberg_cdf(
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

    _validate_merge_keys(merge_keys)
    _validate_table_identifier(table_identifier)

    schema = dataset.schema()
    if change_type_column not in schema.names:
        raise ValueError(
            f"Change type column '{change_type_column}' not found. Available: {schema.names}"
        )

    catalog, table, catalog_name, catalog_kwargs_copy = _load_iceberg_table(table_identifier, catalog_kwargs)

    # Validate schemas
    table_columns = {field.name for field in table.schema().fields}
    missing_in_table = set(merge_keys) - table_columns
    missing_in_dataset = set(merge_keys) - set(schema.names)
    if missing_in_table:
        raise ValueError(f"merge_keys {missing_in_table} not in table. Available: {sorted(table_columns)}")
    if missing_in_dataset:
        raise ValueError(f"merge_keys {missing_in_dataset} not in dataset. Available: {sorted(schema.names)}")

    # Validate change_type_column values
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

    from ray.data._internal.datasource.iceberg.upsert_util import _upsert_to_iceberg

    _upsert_to_iceberg(
        dataset=updates_clean,
        table_identifier=table_identifier,
        join_columns=merge_keys,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs_copy},
        snapshot_properties=snapshot_properties,
        update_filter=None,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )

    inserts_clean.write_iceberg(
        table_identifier=table_identifier,
        catalog_kwargs={"name": catalog_name, **catalog_kwargs_copy},
        snapshot_properties=snapshot_properties,
        mode="append",
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )


def _resolve_snapshot_from_timestamp(table: "Table", timestamp: str, before: bool = True) -> Optional[int]:
    """Resolve snapshot ID from ISO format timestamp.

    Handles various ISO timestamp formats including:
    - With/without timezone (Z or +00:00)
    - With/without microseconds/nanoseconds
    - Date-only format (assumes midnight UTC)
    """
    from datetime import datetime

    if not timestamp or not isinstance(timestamp, str):
        raise ValueError(f"timestamp must be non-empty ISO format string. Got: {timestamp}")

    try:
        # Normalize timezone indicator
        normalized_timestamp = timestamp.replace("Z", "+00:00")

        # Handle date-only format (e.g., "2024-01-01")
        if "T" not in normalized_timestamp and " " not in normalized_timestamp:
            normalized_timestamp = normalized_timestamp + "T00:00:00+00:00"

        # Handle nanoseconds - truncate to microseconds since datetime.fromisoformat
        # only supports up to microseconds
        if "." in normalized_timestamp:
            # Find the fractional seconds part
            dot_idx = normalized_timestamp.index(".")
            # Find where fractional seconds end (either +, -, or end of string)
            tz_start = len(normalized_timestamp)
            for tz_char in ["+", "-"]:
                tz_idx = normalized_timestamp.find(tz_char, dot_idx + 1)
                if tz_idx != -1 and tz_idx < tz_start:
                    tz_start = tz_idx

            fractional_part = normalized_timestamp[dot_idx + 1:tz_start]
            if len(fractional_part) > 6:
                # Truncate nanoseconds to microseconds
                fractional_part = fractional_part[:6]
                normalized_timestamp = (
                    normalized_timestamp[:dot_idx + 1] + fractional_part + normalized_timestamp[tz_start:]
                )

        target_dt = datetime.fromisoformat(normalized_timestamp)
        target_ms = int(target_dt.timestamp() * 1000)
    except (ValueError, AttributeError) as e:
        raise ValueError(f"Invalid timestamp format '{timestamp}'. Expected ISO format (e.g., '2024-01-01T00:00:00' or '2024-01-01T00:00:00Z'). Error: {e}")

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
    """Get list of data files added between two snapshots.

    Uses PyIceberg's table.inspect.entries() API to get file entries for a snapshot.
    Filters entries where snapshot_id > start_snapshot_id (exclusive start).

    See: https://py.iceberg.apache.org/api_reference/table/#pyiceberg.table.Table.inspect

    Args:
        table: PyIceberg Table object
        start_snapshot_id: Starting snapshot ID (exclusive). If None, returns all files.
        end_snapshot_id: Ending snapshot ID (inclusive)

    Returns:
        List of DataFile objects added between snapshots
    """
    entries_list = table.inspect.entries(snapshot_id=end_snapshot_id).to_pylist()
    if start_snapshot_id is None:
        return [entry["data_file"] for entry in entries_list]
    return [entry["data_file"] for entry in entries_list if entry["snapshot_id"] > start_snapshot_id]


def _extract_file_paths_from_task(task: Any) -> List[str]:
    """Extract file paths from a read task."""
    if hasattr(task, "file"):
        return [str(task.file.file_path)]
    # task.metadata is a property, not a callable
    if hasattr(task, "metadata"):
        metadata = task.metadata
        if hasattr(metadata, "input_files") and metadata.input_files:
            return [str(f) for f in metadata.input_files]
    return []


def _apply_deletes(
    table: "Table",
    deletes_dataset: "ray.data.Dataset",
    merge_keys: List[str],
    snapshot_properties: Optional[Dict[str, str]],
    ray_remote_args: Optional[Dict[str, Any]],
) -> None:
    """Apply delete operations using PyIceberg delete API.

    Uses PyIceberg's table.delete() method with a filter expression.
    See: https://py.iceberg.apache.org/api_reference/table/#pyiceberg.table.Table.delete

    Args:
        table: PyIceberg Table object
        deletes_dataset: Dataset containing rows to delete
        merge_keys: Column names to use for matching rows
        snapshot_properties: Unused (kept for API compatibility)
        ray_remote_args: Unused (kept for API compatibility)
    """
    from ray.data._internal.datasource.iceberg.upsert_util import _build_key_filter

    keys_df = deletes_dataset.select_columns(merge_keys).to_pandas()
    if len(keys_df) == 0:
        return

    delete_filter = _build_key_filter(merge_keys, keys_df)
    table.delete(delete_filter=delete_filter)
