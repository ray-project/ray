"""Transaction commit logic for Delta Lake datasink.

This module handles committing file actions to Delta Lake transaction log.

Delta Lake specification: https://delta.io/specification/
deltalake Python library: https://delta-io.github.io/delta-rs/python/
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

from ray.data._internal.datasource.delta.utils import (
    convert_schema_to_delta,
    get_file_info_with_retry,
    infer_partition_type,
    normalize_commit_properties,
    to_pyarrow_schema,
    validate_file_path,
    validate_schema_type_compatibility,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CommitInputs:
    """Inputs for commit operations."""

    table_uri: str
    mode: str
    partition_cols: List[str]
    storage_options: Dict[str, str]
    write_kwargs: Dict[str, Any]


def validate_file_actions(
    file_actions: List["AddAction"], filesystem: pa_fs.FileSystem
) -> None:
    """Validate file actions before committing.

    Args:
        file_actions: List of AddAction objects to validate.
        filesystem: PyArrow filesystem (SubTreeFileSystem rooted at table path).

    Raises:
        ValueError: If files don't exist or are empty.
    """
    for action in file_actions:
        validate_file_path(action.path)
        # Check size from AddAction first (avoids storage read if size is 0)
        if action.size == 0:
            raise ValueError(f"File is empty: {action.path}")
        # Verify file exists (trust size from writer, but confirm existence)
        info = get_file_info_with_retry(filesystem, action.path)
        if info.type == pa_fs.FileType.NotFound:
            raise ValueError(
                f"File does not exist: {action.path} (relative to table root)"
            )


def infer_schema_from_files(
    add_actions: List["AddAction"],
    partition_cols: List[str],
    filesystem: pa_fs.FileSystem,
    provided_schema: Optional[pa.Schema],
) -> pa.Schema:
    """Infer schema from first Parquet file and partition columns.

    Args:
        add_actions: List of AddAction objects with file paths.
        partition_cols: List of partition column names.
        filesystem: PyArrow filesystem for reading files.
        provided_schema: Optional schema to use instead of inferring.

    Returns:
        PyArrow schema with partition columns added.

    Raises:
        ValueError: If schema cannot be inferred.
    """
    if provided_schema:
        # Ensure partition columns are included in provided schema
        schema = provided_schema
        for col in partition_cols:
            if col not in schema.names:
                # Infer type from partition values in file actions if available
                col_type = pa.string()  # Default to string if no values found
                for a in add_actions:
                    pv = getattr(a, "partition_values", None) or {}
                    if col in pv and pv[col] is not None:
                        col_type = infer_partition_type(pv[col])
                        break
                schema = schema.append(pa.field(col, col_type))
        return schema
    if not add_actions:
        raise ValueError("Cannot infer schema from empty file list")

    first = next((a for a in add_actions if a and a.path), None)
    if not first:
        raise ValueError("No valid file actions found for schema inference")

    try:
        with filesystem.open_input_file(first.path) as f:
            schema = pq.ParquetFile(f).schema_arrow
    except Exception as e:
        raise ValueError(f"Failed to read schema from {first.path}: {e}") from e

    if len(schema) == 0:
        raise ValueError(f"Cannot infer schema from file with no columns: {first.path}")

    # Add partition columns to schema if missing.
    # Infer partition column types from partition values in file actions.
    for col in partition_cols:
        if col in schema.names:
            continue
        # Infer type from partition values in file actions
        col_type = pa.string()  # Default to string if no values found
        for a in add_actions:
            pv = getattr(a, "partition_values", None) or {}
            if col in pv and pv[col] is not None:
                col_type = infer_partition_type(pv[col])
                break
        schema = schema.append(pa.field(col, col_type))

    return schema


def create_table_with_files(
    inputs: CommitInputs,
    file_actions: List["AddAction"],
    schema: Optional[pa.Schema],
    filesystem: pa_fs.FileSystem,
) -> None:
    """Create new Delta table and commit files atomically.

    Args:
        inputs: Commit inputs configuration.
        file_actions: List of AddAction objects for files to commit.
        schema: PyArrow schema for table (or None to infer from files).
        filesystem: PyArrow filesystem for reading files.

    Raises:
        ValueError: If no file_actions and no schema provided.
    """
    from deltalake.transaction import create_table_with_add_actions

    if not file_actions:
        if schema is None or len(schema) == 0:
            raise ValueError(
                "Cannot create a new Delta table with no files and no schema. "
                "Provide `schema=` when writing an empty dataset to a new path."
            )
        inferred = schema
    else:
        inferred = infer_schema_from_files(
            file_actions, inputs.partition_cols, filesystem, schema
        )
    delta_schema = convert_schema_to_delta(inferred)

    create_table_with_add_actions(
        table_uri=inputs.table_uri,
        schema=delta_schema,
        add_actions=file_actions,
        mode=inputs.mode,
        partition_by=inputs.partition_cols or None,
        name=inputs.write_kwargs.get("name"),
        description=inputs.write_kwargs.get("description"),
        configuration=inputs.write_kwargs.get("configuration"),
        storage_options=inputs.storage_options,
        commit_properties=normalize_commit_properties(
            inputs.write_kwargs.get("commit_properties")
        ),
        post_commithook_properties=inputs.write_kwargs.get(
            "post_commithook_properties"
        ),
    )


def _build_partition_delete_predicate(
    file_actions: List["AddAction"],
    partition_cols: List[str],
    schema: Optional[pa.Schema] = None,
) -> Optional[str]:
    """Build SQL delete predicate for partitions being overwritten.

    Args:
        file_actions: List of AddAction objects with partition_values.
        partition_cols: List of partition column names.
        schema: Optional PyArrow schema to determine column types. Required to
            distinguish float NaN from string "NaN" values.

    Returns:
        SQL predicate string matching partitions to delete, or None if no partitions.
    """
    if not partition_cols or not file_actions:
        return None

    # Import here to avoid circular dependency
    from ray.data._internal.datasource.delta.upsert import (
        format_sql_value,
        quote_identifier,
    )

    # Build mapping of partition column names to their types
    partition_col_types = {}
    if schema:
        for col in partition_cols:
            if col in schema.names:
                partition_col_types[col] = schema.field(col).type

    # Extract unique partition value combinations from file actions
    partition_combinations = set()
    for action in file_actions:
        pv = getattr(action, "partition_values", None) or {}
        if pv:
            # Build tuple of partition values in order
            # Note: partition_values are stored as strings (from build_partition_path)
            # but format_sql_value handles string conversion correctly
            # Use get() with None default - missing keys indicate bug but we handle gracefully
            combo = tuple(pv.get(col) for col in partition_cols)
            # Add all combinations, including all-NULL partitions
            # All-NULL partitions are valid and must be handled with IS NULL predicates
            # Filtering them out would cause fallback to delete all (incorrect behavior)
            partition_combinations.add(combo)

    if not partition_combinations:
        return None

    # Build OR predicate: (col1 = 'val1' AND col2 = 'val2') OR ...
    # Note: partition_values are strings, format_sql_value will quote them appropriately
    # CRITICAL: Must include NULL checks for NULL partition values to avoid over-deletion
    ors = []
    for combo in partition_combinations:
        ands = []
        for i, col in enumerate(partition_cols):
            val = combo[i]
            if val is not None:
                # Handle "NaN" string: distinguish float NaN from string "NaN"
                if val == "NaN":
                    col_type = partition_col_types.get(col)
                    if col_type and pa.types.is_floating(col_type):
                        # Float NaN: SQL NaN comparison: NaN != NaN is true (IEEE 754)
                        # Use col != col to correctly match NaN values in float columns
                        ands.append(f"{quote_identifier(col)} != {quote_identifier(col)}")
                    else:
                        # String "NaN": treat as regular string value
                        ands.append(f"{quote_identifier(col)} = {format_sql_value(val)}")
                else:
                    # format_sql_value handles string conversion and quoting
                    ands.append(f"{quote_identifier(col)} = {format_sql_value(val)}")
            else:
                # NULL partition values must be explicitly checked with IS NULL
                # Skipping them would cause over-deletion (matching all non-NULL values)
                ands.append(f"{quote_identifier(col)} IS NULL")
        if ands:
            ors.append(f"({' AND '.join(ands)})")

    return " OR ".join(ors) if ors else None


def commit_to_existing_table(
    inputs: CommitInputs,
    table: "DeltaTable",
    file_actions: List["AddAction"],
    schema: Optional[pa.Schema],
    filesystem: pa_fs.FileSystem,
) -> None:
    """Commit files to existing Delta table using write transaction.

    Args:
        inputs: Commit inputs configuration.
        table: DeltaTable to commit to.
        file_actions: List of AddAction objects for files to commit.
        schema: PyArrow schema (or None to infer from files).
        filesystem: PyArrow filesystem for reading files.
    """
    # Get existing schema once (used for both partition delete predicate and validation)
    existing_schema = to_pyarrow_schema(table.schema())

    # Validate schema compatibility BEFORE deleting data in overwrite mode.
    # This prevents data loss if validation fails.
    if file_actions:
        incoming = infer_schema_from_files(
            file_actions, inputs.partition_cols, filesystem, schema
        )
    else:
        incoming = schema

    if incoming is not None and len(incoming) > 0:
        validate_schema_type_compatibility(existing_schema, incoming)

    # For OVERWRITE mode, delete existing data after validation passes.
    if inputs.mode == "overwrite":
        partition_overwrite_mode = inputs.write_kwargs.get(
            "partition_overwrite_mode", "static"
        )
        if partition_overwrite_mode not in ("static", "dynamic"):
            raise ValueError(
                f"Invalid partition_overwrite_mode '{partition_overwrite_mode}'. "
                "Must be 'static' or 'dynamic'."
            )
        if partition_overwrite_mode == "dynamic" and inputs.partition_cols:
            # Dynamic partition overwrite: only delete partitions being written
            # Use existing schema to determine partition column types for NaN handling
            pred = _build_partition_delete_predicate(
                file_actions, inputs.partition_cols, existing_schema
            )
            if pred:
                table.delete(pred)
            else:
                # No partitions in file actions (shouldn't happen for partitioned table,
                # but fallback to delete all for safety)
                table.delete()
        else:
            # Static partition overwrite: delete all data
            table.delete()

    table.create_write_transaction(
        actions=file_actions,
        mode="append",  # Always append after delete for OVERWRITE
        schema=table.schema(),
        partition_by=inputs.partition_cols or None,
        commit_properties=normalize_commit_properties(
            inputs.write_kwargs.get("commit_properties")
        ),
        post_commithook_properties=inputs.write_kwargs.get(
            "post_commithook_properties"
        ),
    )


def validate_partition_columns_match_existing(
    existing_table: "DeltaTable", partition_cols: List[str]
) -> None:
    """Validate partition columns align with the existing table metadata.

    Args:
        existing_table: DeltaTable to check.
        partition_cols: List of partition column names.

    Raises:
        ValueError: If partition columns don't match existing table.
    """
    existing = existing_table.metadata().partition_columns
    if partition_cols:
        if existing and existing != partition_cols:
            raise ValueError(
                f"Partition columns mismatch. Existing: {existing}, requested: {partition_cols}"
            )
        if not existing:
            raise ValueError(
                f"Partition columns provided {partition_cols} but table is not partitioned."
            )
    elif existing:
        raise ValueError(
            f"Table is partitioned by {existing} but no partition columns were provided."
        )
