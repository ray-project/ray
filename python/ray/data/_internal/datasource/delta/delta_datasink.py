"""Datasink for writing Ray Data to Delta Lake tables."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

from ray.data._internal.arrow_ops.transform_pyarrow import (
    concat as concat_arrow_tables,
    unify_schemas,
)
from ray.data._internal.datasource.delta.delta_committer import (
    commit_to_existing_table,
    create_table_with_files,
    validate_file_actions,
    validate_partition_columns_match_existing,
)
from ray.data._internal.datasource.delta.delta_upsert import commit_upsert
from ray.data._internal.datasource.delta.delta_writer import DeltaFileWriter
from ray.data._internal.datasource.delta.utils import (
    UPSERT_JOIN_COLS,
    DeltaWriteResult,
    get_file_info_with_retry,
    get_storage_options,
    join_delta_path,
    to_pyarrow_schema,
    try_get_deltatable,
    types_compatible,
    validate_partition_column_names,
    validate_partition_columns_in_table,
    validate_schema_type_compatibility,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import (
    RetryingPyFileSystem,
    _check_import,
    _is_local_scheme,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


class DeltaDatasink(Datasink[DeltaWriteResult]):
    """Datasink for writing to Delta Lake tables.

    Uses two-phase commit: write Parquet files, then commit to transaction log.
    Supports APPEND, OVERWRITE, UPSERT, ERROR, and IGNORE modes.

    deltalake: https://delta-io.github.io/delta-rs/python/
    """

    def __init__(
        self,
        path: str,
        *,
        mode: SaveMode = SaveMode.APPEND,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
        **write_kwargs,
    ):
        """Initialize DeltaDatasink.

        Args:
            path: Path to Delta table (local or cloud storage).
            mode: Write mode - APPEND, OVERWRITE, UPSERT, ERROR, or IGNORE.
            partition_cols: Columns to partition by (Hive-style).
            filesystem: Optional PyArrow filesystem.
            schema: Optional explicit schema for the table.
            upsert_kwargs: Options for UPSERT mode:
                - join_cols: List of column names to match rows on (required for UPSERT)
            **write_kwargs: Additional options passed to Delta writer.
        """
        _check_import(self, module="deltalake", package="deltalake")

        # Validate and normalize mode
        self.mode = self._validate_mode(mode)
        self.partition_cols = validate_partition_column_names(partition_cols or [])
        self.schema = schema
        self.write_kwargs = write_kwargs
        self._upsert_kwargs = (upsert_kwargs or {}).copy()

        # Validate upsert_kwargs
        if self._upsert_kwargs and self.mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified with SaveMode.UPSERT, got {self.mode}"
            )

        # Internal state
        self._skip_write = False
        self._written_files: Set[str] = set()
        self._table_existed_at_start: bool = False
        self._write_uuid: Optional[str] = None

        # Store unresolved path (original URI) for deltalake APIs which need the scheme
        self.table_uri = path

        # Set up filesystem with retry support
        data_context = DataContext.get_current()
        resolved_paths, self.filesystem = _resolve_paths_and_filesystem(
            path, filesystem
        )
        self.filesystem = RetryingPyFileSystem.wrap(
            self.filesystem, retryable_errors=data_context.retried_io_errors
        )
        if len(resolved_paths) != 1:
            raise ValueError(
                f"Expected exactly one path for Delta table, got {len(resolved_paths)} paths"
            )
        self.path = resolved_paths[0]

        # Validate path is not empty
        if not self.path or not self.path.strip():
            raise ValueError("Delta table path cannot be empty")

        # Get storage options with auto-detection for cloud storage
        self.storage_options = get_storage_options(
            self.table_uri, write_kwargs.get("storage_options")
        )

    def __getstate__(self) -> dict:
        """Exclude non-serializable state during pickling."""
        state = self.__dict__.copy()
        state.pop("filesystem", None)  # Re-created on deserialization
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state and re-create filesystem on unpickling."""
        self.__dict__.update(state)
        # Re-create filesystem from path
        data_context = DataContext.get_current()
        _, self.filesystem = _resolve_paths_and_filesystem(self.table_uri, None)
        self.filesystem = RetryingPyFileSystem.wrap(
            self.filesystem, retryable_errors=data_context.retried_io_errors
        )

    def _validate_mode(self, mode: Any) -> SaveMode:
        """Validate and normalize write mode to SaveMode enum."""
        if isinstance(mode, SaveMode):
            # All SaveMode enum instances are valid by definition
            return mode

        # Handle string mode for backwards compatibility
        if isinstance(mode, str):
            mode_lower = mode.lower()
            mode_map = {
                "append": SaveMode.APPEND,
                "overwrite": SaveMode.OVERWRITE,
                "upsert": SaveMode.UPSERT,
                "error": SaveMode.ERROR,
                "ignore": SaveMode.IGNORE,
            }
            if mode_lower not in mode_map:
                raise ValueError(
                    f"Invalid mode '{mode}'. Supported: {list(mode_map.keys())}"
                )
            return mode_map[mode_lower]

        raise ValueError(f"Invalid mode type: {type(mode).__name__}")

    @property
    def supports_distributed_writes(self) -> bool:
        """If False, only launch write tasks on the driver's node.

        Local filesystem writes require all tasks to run on the same node
        to ensure files are accessible for the commit phase.
        """
        # Check for local:// scheme (Ray's custom scheme)
        if _is_local_scheme(self.table_uri):
            return False

        # Check for regular local filesystem paths (no scheme or file:// scheme)
        uri_lower = self.table_uri.lower()
        if uri_lower.startswith("file://") or (
            not uri_lower.startswith(
                ("s3://", "s3a://", "gs://", "gcs://", "abfss://", "abfs://", "hdfs://")
            )
            and "://" not in self.table_uri
        ):
            return False

        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return None

    def get_name(self) -> str:
        return "Delta"

    def __repr__(self) -> str:
        """String representation for debugging."""
        partition_info = (
            f", partition_cols={self.partition_cols}" if self.partition_cols else ""
        )
        return f"DeltaDatasink(path={self.table_uri}, mode={self.mode.value}{partition_info})"

    def _get_upsert_cols(self) -> List[str]:
        """Get join columns for upsert operations."""
        return self._upsert_kwargs.get(UPSERT_JOIN_COLS, [])

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        """Initialize table for writing and validate constraints.

        Validates write mode constraints by checking if table exists.
        Stores table state at start time to detect race conditions later.
        For UPSERT mode, validates that join_cols are specified.

        Args:
            schema: PyArrow schema from the first input bundle. Used for
                schema validation and empty table creation.
        """
        # Store schema from first input bundle
        if schema is not None and self.schema is None:
            self.schema = schema

        # Check if table exists at start time
        existing_table = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing_table is not None

        if existing_table:
            validate_partition_columns_match_existing(
                existing_table, self.partition_cols
            )

            # Early schema evolution preparation (like Iceberg)
            # Validate schema compatibility and prepare for evolution before writing files
            # Delta Lake automatically evolves schema during commit, but we validate early
            if self.schema is not None:
                existing_schema = to_pyarrow_schema(existing_table.schema())
                validate_schema_type_compatibility(existing_schema, self.schema)
                # New columns are allowed and will be added automatically during commit
                # This early check prevents errors during write phase

        # Validate mode-specific constraints
        if self.mode == SaveMode.ERROR and existing_table:
            raise ValueError(
                f"Delta table already exists at {self.table_uri}. "
                "Use mode=SaveMode.APPEND or SaveMode.OVERWRITE."
            )

        if self.mode == SaveMode.IGNORE and existing_table:
            self._skip_write = True
        else:
            self._skip_write = False

        # Validate UPSERT mode requirements
        if self.mode == SaveMode.UPSERT:
            if not existing_table:
                raise ValueError(
                    "UPSERT mode requires an existing Delta table. "
                    "Create the table first with APPEND mode."
                )
            upsert_cols = self._get_upsert_cols()
            if not upsert_cols:
                raise ValueError(
                    "UPSERT mode requires join_cols in upsert_kwargs. "
                    "Example: upsert_kwargs={'join_cols': ['id']}"
                )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> DeltaWriteResult:
        """Phase 1: Write Parquet files and return metadata for commit.

        This is the first phase of the two-phase commit protocol. Files are written
        to storage and metadata is collected, but nothing is committed to the Delta
        transaction log until on_write_complete() is called.

        For UPSERT mode, also collects key columns to identify rows to delete.

        Args:
            blocks: Iterable of data blocks to write.
            ctx: Task context with metadata like write_uuid.

        Returns:
            DeltaWriteResult containing add_actions, upsert_keys (if applicable),
            and schemas from written blocks.
        """
        if self._skip_write:
            return DeltaWriteResult()

        # Capture write_uuid from TaskContext (set by plan_write_op)
        ctx_kwargs = getattr(ctx, "kwargs", None) or {}
        self._write_uuid = ctx_kwargs.get(WRITE_UUID_KWARG_NAME)

        all_actions = []
        upsert_keys_tables = []
        block_schemas = []
        block_idx = 0
        use_upsert = self.mode == SaveMode.UPSERT

        try:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)
                table = block_accessor.to_arrow()

                # Skip empty blocks (simplified pattern like Iceberg)
                if table.num_rows == 0:
                    continue

                self._validate_table_schema(table)
                validate_partition_columns_in_table(self.partition_cols, table)

                # Collect schema for reconciliation
                block_schemas.append(table.schema)

                # Extract upsert keys for copy-on-write pattern
                if use_upsert:
                    upsert_cols = self._get_upsert_cols()
                    if upsert_cols:
                        # Validate upsert columns exist in table
                        missing_upsert_cols = [
                            c for c in upsert_cols if c not in table.column_names
                        ]
                        if missing_upsert_cols:
                            raise ValueError(
                                f"UPSERT join columns not found in data: {missing_upsert_cols}. "
                                f"Available columns: {table.column_names}"
                            )
                        upsert_keys_tables.append(table.select(upsert_cols))

                # Write data and collect actions
                actions = self._write_table_data(table, ctx.task_idx, block_idx)
                all_actions.extend([a for a in actions if a is not None])
                block_idx += 1

            # Combine upsert keys from all blocks
            upsert_keys = (
                concat_arrow_tables(upsert_keys_tables) if upsert_keys_tables else None
            )

            return DeltaWriteResult(
                add_actions=all_actions,
                upsert_keys=upsert_keys,
                schemas=block_schemas,
            )
        except Exception:
            self._cleanup_written_files()
            raise

    def _validate_table_schema(self, table: pa.Table) -> None:
        """Validate table schema matches expected schema if provided."""
        if not self.schema:
            return

        table_cols = set(table.column_names)
        schema_cols = set(self.schema.names)
        missing = schema_cols - table_cols
        if missing:
            raise ValueError(
                f"Missing columns: {sorted(missing)}. Table has: {sorted(table_cols)}"
            )

        for field in self.schema:
            if field.name in table_cols and field.name not in self.partition_cols:
                table_col = table[field.name]
                table_col_type = table_col.type

                # Handle NULL-only columns: if column is all NULL and field is nullable,
                # allow it (type will be inferred correctly during write)
                if field.nullable and pa.types.is_null(table_col_type):
                    # Check if all values are NULL using pc.all() for ChunkedArray
                    is_null_mask = pa.compute.is_null(table_col)
                    if pc.all(is_null_mask).as_py():
                        # All NULL values in nullable field - type will be inferred from schema
                        continue

                if not types_compatible(field.type, table_col_type):
                    raise ValueError(
                        f"Type mismatch for '{field.name}': expected {field.type}, "
                        f"got {table_col_type}"
                    )

    def _get_file_writer(self) -> DeltaFileWriter:
        """Get file writer instance for this datasink."""
        return DeltaFileWriter(
            table_uri=self.table_uri,
            filesystem=self.filesystem,
            partition_cols=self.partition_cols,
            write_uuid=self._write_uuid,
            write_kwargs=self.write_kwargs,
            written_files=self._written_files,
        )

    def _write_table_data(
        self, table: pa.Table, task_idx: int, block_idx: int = 0
    ) -> List["AddAction"]:
        """Write table data as partitioned or non-partitioned Parquet files."""
        writer = self._get_file_writer()
        return writer.write_table_data(table, task_idx, block_idx)

    def on_write_complete(self, write_result: WriteResult[DeltaWriteResult]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        all_file_actions, upsert_keys, all_schemas = self._collect_write_results(
            write_result
        )
        existing_table = try_get_deltatable(self.table_uri, self.storage_options)

        # Validate race conditions
        if (
            not self._table_existed_at_start
            and existing_table is not None
            and self.mode == SaveMode.ERROR
        ):
            self._cleanup_written_files(all_file_actions)
            raise ValueError(
                f"Race condition: Delta table was created at {self.table_uri} "
                "after write started. Use SaveMode.APPEND or OVERWRITE."
            )

        # IGNORE mode race condition: if table was created concurrently, clean up files
        if (
            not self._table_existed_at_start
            and existing_table is not None
            and self.mode == SaveMode.IGNORE
        ):
            self._cleanup_written_files(all_file_actions)
            return

        if (
            self._table_existed_at_start
            and existing_table is None
            and self.mode in (SaveMode.APPEND, SaveMode.UPSERT)
        ):
            self._cleanup_written_files(all_file_actions)
            raise ValueError(
                f"Delta table was deleted at {self.table_uri} after write started. "
                "Use SaveMode.OVERWRITE to create a new table."
            )

        # Handle empty writes (no files written)
        if not all_file_actions:
            if self._table_existed_at_start and existing_table is not None:
                if self.mode == SaveMode.OVERWRITE:
                    # Clear existing table with empty commit
                    commit_to_existing_table(
                        existing_table,
                        [],
                        self.mode.value,
                        self.partition_cols,
                        self.schema,
                        self.write_kwargs,
                        self.table_uri,
                        self.filesystem,
                    )
                    return
                if self.mode == SaveMode.IGNORE:
                    # Do nothing if table exists
                    return
            # Create empty table if schema provided and table doesn't exist
            # Skip if table existed at start (IGNORE mode semantics)
            if self.schema and not existing_table and not self._table_existed_at_start:
                create_table_with_files(
                    self.table_uri,
                    [],
                    self.schema,
                    self.mode.value,
                    self.partition_cols,
                    self.storage_options,
                    self.write_kwargs,
                    self.filesystem,
                )
            return

        # Commit phase: validate and commit files atomically
        # Wrap in try/except to ensure cleanup on failure (files written by workers)
        try:
            # Reconcile schemas from all workers with type promotion
            # Handles cases where different workers infer different types (e.g., int32 vs int64)
            # This is inside the try block so files are cleaned up if reconciliation fails
            if all_schemas:
                if existing_table:
                    table_schema = to_pyarrow_schema(existing_table.schema())
                    reconciled_schema = unify_schemas(
                        [table_schema] + all_schemas, promote_types=True
                    )
                else:
                    reconciled_schema = unify_schemas(all_schemas, promote_types=True)

                if reconciled_schema:
                    self.schema = reconciled_schema
            validate_file_actions(all_file_actions, self.table_uri, self.filesystem)

            if self._table_existed_at_start:
                # Table existed at start - handle based on mode
                if self.mode == SaveMode.IGNORE:
                    self._cleanup_written_files(all_file_actions)
                    return
                if existing_table is None and self.mode == SaveMode.OVERWRITE:
                    # Table was deleted, create new one
                    create_table_with_files(
                        self.table_uri,
                        all_file_actions,
                        self.schema,
                        self.mode.value,
                        self.partition_cols,
                        self.storage_options,
                        self.write_kwargs,
                        self.filesystem,
                    )
                elif existing_table is not None:
                    # Commit to existing table (APPEND, OVERWRITE, or UPSERT)
                    self._commit_by_mode(existing_table, all_file_actions, upsert_keys)
                else:
                    raise ValueError(
                        f"Delta table was deleted at {self.table_uri} after write started."
                    )
            else:
                # Table didn't exist at start - create new table
                create_table_with_files(
                    self.table_uri,
                    all_file_actions,
                    self.schema,
                    self.mode.value,
                    self.partition_cols,
                    self.storage_options,
                    self.write_kwargs,
                    self.filesystem,
                )
        except Exception:
            # Clean up orphaned files on commit failure
            self._cleanup_written_files(all_file_actions)
            raise

        self._written_files.clear()

    def _commit_by_mode(
        self,
        table: "DeltaTable",
        file_actions: List["AddAction"],
        upsert_keys: Optional[pa.Table],
    ) -> None:
        """Commit files to existing table based on write mode."""
        if self.mode == SaveMode.UPSERT:
            commit_upsert(
                table,
                file_actions,
                upsert_keys,
                self._get_upsert_cols(),
                self.partition_cols,
                self.write_kwargs,
            )
        else:
            commit_to_existing_table(
                table,
                file_actions,
                self.mode.value,
                self.partition_cols,
                self.schema,
                self.write_kwargs,
                self.table_uri,
                self.filesystem,
            )

    def _collect_write_results(
        self, write_result: WriteResult[DeltaWriteResult]
    ) -> tuple[List["AddAction"], Optional[pa.Table], List[pa.Schema]]:
        """Collect all results from distributed write tasks.

        Args:
            write_result: WriteResult containing DeltaWriteResult from each task.

        Returns:
            Tuple of (all_file_actions, combined_upsert_keys, all_schemas).
        """
        if not write_result.write_returns:
            return [], None, []

        all_actions = []
        upsert_keys_tables = []
        all_schemas = []

        for result in write_result.write_returns:
            if result is None:
                continue
            if not isinstance(result, DeltaWriteResult):
                raise ValueError(
                    f"Invalid write return: expected DeltaWriteResult, "
                    f"got {type(result).__name__}"
                )
            all_actions.extend(result.add_actions)
            if result.upsert_keys is not None:
                upsert_keys_tables.append(result.upsert_keys)
            # Collect schemas from all workers for reconciliation
            if result.schemas:
                all_schemas.extend(result.schemas)

        # Validate no duplicate file paths (shouldn't happen but check for safety)
        seen_paths = set()
        for action in all_actions:
            if action.path in seen_paths:
                raise ValueError(f"Duplicate file paths detected: {action.path}")
            seen_paths.add(action.path)

        # Combine upsert keys from all workers
        upsert_keys = (
            concat_arrow_tables(upsert_keys_tables) if upsert_keys_tables else None
        )

        return all_actions, upsert_keys, all_schemas

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure - attempt cleanup of orphaned files.

        Note: In distributed execution, cleanup may be incomplete as file paths
        are tracked in worker processes. Orphaned files may remain and require
        manual cleanup.
        """
        write_id = (
            self._write_uuid[:8]
            if self._write_uuid and len(self._write_uuid) >= 8
            else self._write_uuid or "unknown"
        )

        logger.error(
            f"Delta write failed for {self.table_uri} (write_id={write_id}): {error}. "
            "Attempting cleanup of uncommitted files (may be incomplete in distributed execution)."
        )
        # Attempt cleanup, but note that _written_files may be empty on driver
        # in distributed execution since each worker has its own copy
        cleaned_count = len(self._written_files)
        self._cleanup_written_files()
        if cleaned_count == 0:
            logger.warning(
                f"No files cleaned up (driver has no record of written files). "
                f"Orphaned files matching 'part-{write_id}-*' may exist at "
                f"{self.table_uri} and require manual cleanup or VACUUM."
            )

    def _cleanup_written_files(
        self, file_actions: Optional[List["AddAction"]] = None
    ) -> None:
        """Clean up all written files that weren't committed.

        Args:
            file_actions: Optional list of AddAction objects with file paths.
                If provided, uses these paths instead of _written_files set.
        """
        files_to_cleanup = set()
        if file_actions:
            # Use paths from file actions (used when commit fails)
            for action in file_actions:
                if action and action.path:
                    full_path = join_delta_path(self.table_uri, action.path)
                    files_to_cleanup.add(full_path)
        else:
            # Use tracked written files (used when write fails)
            files_to_cleanup = self._written_files.copy()

        for file_path in files_to_cleanup:
            try:
                file_info = get_file_info_with_retry(self.filesystem, file_path)
                if file_info.type != pa_fs.FileType.NotFound:
                    self.filesystem.delete_file(file_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {file_path}: {e}")
        self._written_files.clear()
