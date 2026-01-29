"""Transaction commit logic for Delta Lake datasink.

This module handles committing file actions to Delta Lake transaction log.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.utils import (
    convert_schema_to_delta,
    get_file_info_with_retry,
    infer_partition_type,
    join_delta_path,
    normalize_commit_properties,
    to_pyarrow_schema,
    validate_file_path,
    validate_schema_type_compatibility,
)

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


def validate_file_actions(
    file_actions: List["AddAction"],
    table_uri: str,
    filesystem: pa.fs.FileSystem,
) -> None:
    """Validate file actions before committing.

    Args:
        file_actions: List of AddAction objects to validate.
        table_uri: Base URI for Delta table.
        filesystem: PyArrow filesystem for checking files.
    """
    for action in file_actions:
        validate_file_path(action.path)
        full_path = join_delta_path(table_uri, action.path)
        file_info = get_file_info_with_retry(filesystem, full_path)
        if file_info.type == pa_fs.FileType.NotFound:
            raise ValueError(f"File does not exist: {full_path}")
        if file_info.size == 0:
            raise ValueError(f"File is empty: {full_path}")


def create_table_with_files(
    table_uri: str,
    file_actions: List["AddAction"],
    schema: Optional[pa.Schema],
    mode: str,
    partition_cols: Optional[List[str]],
    storage_options: Dict[str, str],
    write_kwargs: Dict[str, Any],
    filesystem: pa.fs.FileSystem,
) -> None:
    """Create new Delta table and commit files atomically.

    Args:
        table_uri: URI for Delta table.
        file_actions: List of AddAction objects for files to commit.
        schema: PyArrow schema for table (or None to infer from files).
        mode: Write mode (e.g., "append", "overwrite").
        partition_cols: List of partition column names.
        storage_options: Storage authentication options.
        write_kwargs: Additional write options.
        filesystem: PyArrow filesystem for reading files.
    """
    from deltalake.transaction import create_table_with_add_actions

    inferred_schema = (
        schema
        if not file_actions
        else infer_schema(file_actions, table_uri, partition_cols, filesystem, schema)
    )
    delta_schema = convert_schema_to_delta(inferred_schema)

    commit_properties = normalize_commit_properties(
        write_kwargs.get("commit_properties")
    )

    create_table_with_add_actions(
        table_uri=table_uri,
        schema=delta_schema,
        add_actions=file_actions,
        mode=mode,
        partition_by=partition_cols or None,
        name=write_kwargs.get("name"),
        description=write_kwargs.get("description"),
        configuration=write_kwargs.get("configuration"),
        storage_options=storage_options,
        commit_properties=commit_properties,
        post_commithook_properties=write_kwargs.get("post_commithook_properties"),
    )


def commit_to_existing_table(
    existing_table: "DeltaTable",
    file_actions: List["AddAction"],
    mode: str,
    partition_cols: Optional[List[str]],
    schema: Optional[pa.Schema],
    write_kwargs: Dict[str, Any],
    table_uri: str,
    filesystem: pa.fs.FileSystem,
) -> None:
    """Commit files to existing Delta table using write transaction.

    Validates schema compatibility, then creates a write transaction to commit
    file metadata atomically to the Delta transaction log.

    Delta Lake transactions: https://delta.io/specification/#transaction-log-entries
    deltalake write transaction: https://delta-io.github.io/delta-rs/python/api/deltalake.table.html#deltalake.table.DeltaTable.create_write_transaction

    Args:
        existing_table: DeltaTable to commit to.
        file_actions: List of AddAction objects for files to commit.
        mode: Write mode ("append" or "overwrite").
        partition_cols: List of partition column names.
        schema: PyArrow schema (or None to infer from files).
        write_kwargs: Additional write options.
        table_uri: Base URI for Delta table.
        filesystem: PyArrow filesystem for reading files.
    """
    # Validate schema compatibility (allows new columns via schema evolution)
    existing_schema = to_pyarrow_schema(existing_table.schema())
    if file_actions:
        inferred_schema = infer_schema(
            file_actions, table_uri, partition_cols, filesystem, schema
        )
    elif schema:
        inferred_schema = schema
    else:
        # No file actions and no schema - skip validation
        inferred_schema = None

    if inferred_schema:
        validate_schema_type_compatibility(existing_schema, inferred_schema)
        # New columns are allowed (Delta Lake adds them automatically)
        # Missing columns are OK (partial writes)

    # Note: Partition validation is already done in on_write_start for early detection
    # No need to validate again here

    commit_properties = normalize_commit_properties(
        write_kwargs.get("commit_properties")
    )

    # For OVERWRITE mode, try using mode="overwrite" directly first
    # If that doesn't work (e.g., deltalake version doesn't support it),
    # fall back to delete + append pattern
    if mode == "overwrite":
        try:
            # Try atomic overwrite mode first
            existing_table.create_write_transaction(
                actions=file_actions,
                mode="overwrite",
                schema=existing_table.schema(),
                partition_by=partition_cols or None,
                commit_properties=commit_properties,
                post_commithook_properties=write_kwargs.get(
                    "post_commithook_properties"
                ),
            )
        except (ValueError, TypeError, AttributeError):
            # Fallback: delete then append (not fully atomic, but works with all versions)
            # WARNING: If append fails after delete, data loss can occur
            # Users should use time travel to recover if needed
            existing_table.delete("1=1")
            try:
                existing_table.create_write_transaction(
                    actions=file_actions,
                    mode="append",
                    schema=existing_table.schema(),
                    partition_by=partition_cols or None,
                    commit_properties=commit_properties,
                    post_commithook_properties=write_kwargs.get(
                        "post_commithook_properties"
                    ),
                )
            except Exception:
                # If append fails after delete, data is lost
                # Log warning and re-raise so caller can handle cleanup
                logger.warning(
                    "OVERWRITE mode failed: delete succeeded but append failed. "
                    "Table is now empty. Use Delta Lake time travel to recover data: "
                    f"read_delta('{table_uri}', version=<previous_version>)"
                )
                raise
    else:
        # For APPEND mode, just append files
        existing_table.create_write_transaction(
            actions=file_actions,
            mode="append",
            schema=existing_table.schema(),
            partition_by=partition_cols or None,
            commit_properties=commit_properties,
            post_commithook_properties=write_kwargs.get("post_commithook_properties"),
        )


def validate_partition_columns_match_existing(
    existing_table: "DeltaTable", partition_cols: Optional[List[str]]
) -> None:
    """Validate partition columns align with the existing table metadata.

    Args:
        existing_table: DeltaTable to check.
        partition_cols: List of partition column names (or None).

    Raises:
        ValueError: If partition columns don't match existing table.
    """
    existing_partitions = existing_table.metadata().partition_columns
    if partition_cols:
        if existing_partitions and existing_partitions != partition_cols:
            raise ValueError(
                f"Partition columns mismatch. Existing: {existing_partitions}, "
                f"requested: {partition_cols}"
            )
        if not existing_partitions:
            raise ValueError(
                f"Partition columns provided {partition_cols} but table is not partitioned."
            )
    elif existing_partitions:
        raise ValueError(
            f"Table is partitioned by {existing_partitions} but no partition columns "
            "were provided."
        )


def infer_schema(
    add_actions: List["AddAction"],
    table_uri: str,
    partition_cols: Optional[List[str]],
    filesystem: pa.fs.FileSystem,
    provided_schema: Optional[pa.Schema] = None,
) -> pa.Schema:
    """Infer schema from first Parquet file and partition columns.

    Args:
        add_actions: List of AddAction objects with file paths.
        table_uri: Base URI for Delta table.
        partition_cols: List of partition column names.
        filesystem: PyArrow filesystem for reading files.
        provided_schema: Optional schema to use instead of inferring.

    Returns:
        PyArrow schema with partition columns added.

    Raises:
        ValueError: If schema cannot be inferred.
    """
    if provided_schema:
        return provided_schema

    if not add_actions:
        raise ValueError("Cannot infer schema from empty file list")

    # Find first action with a valid path
    first_action = None
    for action in add_actions:
        if action and action.path:
            first_action = action
            break

    if not first_action:
        raise ValueError("No valid file actions found for schema inference")

    first_file = join_delta_path(table_uri, first_action.path)
    try:
        with filesystem.open_input_file(first_file) as file_obj:
            parquet_file = pq.ParquetFile(file_obj)
            schema = parquet_file.schema_arrow
    except Exception as e:
        raise ValueError(f"Failed to read schema from {first_file}: {e}") from e

    if len(schema) == 0:
        raise ValueError(f"Cannot infer schema from file with no columns: {first_file}")

    # Add partition columns to schema if not present
    if partition_cols:
        for col in partition_cols:
            if col not in schema.names:
                # Infer type from first partition value
                col_type = pa.string()  # Default to string
                for action in add_actions:
                    if action and hasattr(action, "partition_values"):
                        partition_vals = action.partition_values or {}
                        if col in partition_vals:
                            val = partition_vals[col]
                            if val is not None:
                                col_type = infer_partition_type(val)
                                break
                schema = schema.append(pa.field(col, col_type))

    return schema
