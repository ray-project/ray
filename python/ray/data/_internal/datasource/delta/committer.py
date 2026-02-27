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
    filesystem: pa_fs.FileSystem,
    provided_schema: Optional[pa.Schema],
) -> pa.Schema:
    """Infer schema from first Parquet file.

    Args:
        add_actions: List of AddAction objects with file paths.
        filesystem: PyArrow filesystem for reading files.
        provided_schema: Optional schema to use instead of inferring.

    Returns:
        PyArrow schema.

    Raises:
        ValueError: If schema cannot be inferred.
    """
    if provided_schema:
        return provided_schema
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
        raise ValueError(
            f"Cannot infer schema from file with no columns: {first.path}"
        )

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
        inferred = infer_schema_from_files(file_actions, filesystem, schema)
    delta_schema = convert_schema_to_delta(inferred)

    create_table_with_add_actions(
        table_uri=inputs.table_uri,
        schema=delta_schema,
        add_actions=file_actions,
        mode="append",
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
    existing_schema = to_pyarrow_schema(table.schema())

    # Validate schema compatibility before commit
    if file_actions:
        incoming = infer_schema_from_files(file_actions, filesystem, schema)
    else:
        incoming = schema

    if incoming is not None and len(incoming) > 0:
        validate_schema_type_compatibility(existing_schema, incoming)

    table.create_write_transaction(
        actions=file_actions,
        mode="append",
        schema=table.schema(),
        commit_properties=normalize_commit_properties(
            inputs.write_kwargs.get("commit_properties")
        ),
        post_commithook_properties=inputs.write_kwargs.get(
            "post_commithook_properties"
        ),
    )
