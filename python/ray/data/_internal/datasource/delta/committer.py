"""Transaction commit logic for Delta Lake datasink (MVP, APPEND only).

This MVP build implements only the APPEND code paths -- ``create_table_with_files``
for first-time writes and ``commit_to_existing_table`` for appends. PR 4 adds
the OVERWRITE branch and PR 5 adds dynamic-partition overwrite.

Delta Lake spec: https://delta.io/specification/
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
    resolve_under_table_root,
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
    local_filesystem_root: Optional[str] = None


def validate_file_actions(
    file_actions: List["AddAction"],
    filesystem: pa_fs.FileSystem,
    local_filesystem_root: Optional[str] = None,
) -> None:
    """Validate file actions before committing."""
    for action in file_actions:
        validate_file_path(action.path)
        if action.size == 0:
            raise ValueError(f"File is empty: {action.path}")
        phys = resolve_under_table_root(local_filesystem_root, action.path)
        info = get_file_info_with_retry(filesystem, phys)
        if info.type == pa_fs.FileType.NotFound:
            raise ValueError(
                f"File does not exist: {action.path} (relative to table root)"
            )


def infer_schema_from_files(
    add_actions: List["AddAction"],
    partition_cols: List[str],
    filesystem: pa_fs.FileSystem,
    provided_schema: Optional[pa.Schema],
    local_filesystem_root: Optional[str] = None,
) -> pa.Schema:
    """Infer schema from the first Parquet file plus any partition cols."""
    if provided_schema:
        schema = provided_schema
        for col in partition_cols:
            if col not in schema.names:
                col_type = pa.string()
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
        phys = resolve_under_table_root(local_filesystem_root, first.path)
        with filesystem.open_input_file(phys) as f:
            schema = pq.ParquetFile(f).schema_arrow
    except Exception as e:
        raise ValueError(f"Failed to read schema from {first.path}: {e}") from e

    if len(schema) == 0:
        raise ValueError(f"Cannot infer schema from file with no columns: {first.path}")

    # Append partition columns to schema if missing.
    for col in partition_cols:
        if col in schema.names:
            continue
        col_type = pa.string()
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
    """Create a new Delta table and commit ``file_actions`` atomically."""
    from deltalake.transaction import create_table_with_add_actions

    if not file_actions:
        if schema is None or len(schema) == 0:
            raise ValueError(
                "Cannot create a new Delta table with no files and no schema."
            )
        inferred = schema
    else:
        inferred = infer_schema_from_files(
            file_actions,
            inputs.partition_cols,
            filesystem,
            schema,
            inputs.local_filesystem_root,
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
    )


def commit_to_existing_table(
    inputs: CommitInputs,
    table: "DeltaTable",
    file_actions: List["AddAction"],
    schema: Optional[pa.Schema],
    filesystem: pa_fs.FileSystem,
) -> None:
    """Commit ``file_actions`` to an existing Delta table.

    Supports APPEND and OVERWRITE (static -- deletes all data before
    writing). PR 5 adds dynamic partition overwrite.
    """
    existing_schema = to_pyarrow_schema(table.schema())
    if file_actions:
        incoming = infer_schema_from_files(
            file_actions,
            inputs.partition_cols,
            filesystem,
            schema,
            inputs.local_filesystem_root,
        )
    else:
        incoming = schema
    if incoming is not None and len(incoming) > 0:
        validate_schema_type_compatibility(existing_schema, incoming)

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
            pred = _build_partition_delete_predicate(
                file_actions, inputs.partition_cols, existing_schema
            )
            if pred:
                table.delete(pred)
            else:
                table.delete()
        else:
            table.delete()

    table.create_write_transaction(
        actions=file_actions,
        mode="append",
        schema=table.schema(),
        partition_by=inputs.partition_cols or None,
    )


def _build_partition_delete_predicate(
    file_actions: List["AddAction"],
    partition_cols: List[str],
    schema: Optional[pa.Schema] = None,
) -> Optional[str]:
    """Build a SQL delete predicate for partitions being overwritten."""
    if not partition_cols or not file_actions:
        return None

    def quote_id(name: str) -> str:
        return f"`{name.replace('`', '``')}`"

    def fmt_val(val: Any) -> str:
        if val is None:
            return "NULL"
        return "'" + str(val).replace("'", "''") + "'"

    partition_col_types: Dict[str, pa.DataType] = {}
    if schema:
        for col in partition_cols:
            if col in schema.names:
                partition_col_types[col] = schema.field(col).type

    partition_combinations = set()
    for action in file_actions:
        pv = getattr(action, "partition_values", None) or {}
        if pv:
            combo = tuple(pv.get(col) for col in partition_cols)
            partition_combinations.add(combo)
    if not partition_combinations:
        return None

    ors = []
    for combo in partition_combinations:
        ands = []
        for i, col in enumerate(partition_cols):
            val = combo[i]
            if val is None:
                ands.append(f"{quote_id(col)} IS NULL")
            elif (
                val == "NaN"
                and partition_col_types.get(col) is not None
                and pa.types.is_floating(partition_col_types[col])
            ):
                ands.append(f"{quote_id(col)} != {quote_id(col)}")
            else:
                ands.append(f"{quote_id(col)} = {fmt_val(val)}")
        if ands:
            ors.append("(" + " AND ".join(ands) + ")")
    return " OR ".join(ors) if ors else None


def validate_partition_columns_match_existing(
    existing_table: "DeltaTable", partition_cols: List[str]
) -> None:
    """Ensure ``partition_cols`` is compatible with the existing table."""
    existing = existing_table.metadata().partition_columns
    if partition_cols:
        if existing and existing != partition_cols:
            raise ValueError(
                f"Partition columns mismatch. Existing: {existing}, "
                f"requested: {partition_cols}"
            )
        if not existing:
            raise ValueError(
                f"Partition columns provided {partition_cols} but table is "
                "not partitioned."
            )
    elif existing:
        raise ValueError(
            f"Table is partitioned by {existing} but no partition columns "
            "were provided."
        )
