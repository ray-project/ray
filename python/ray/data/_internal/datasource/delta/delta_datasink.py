"""Datasink for writing Ray Data to Delta Lake tables."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

from ray.data._internal.arrow_ops.transform_pyarrow import concat, unify_schemas
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
    create_app_transaction_id,
    get_file_info_with_retry,
    get_storage_options,
    normalize_commit_properties,
    to_pyarrow_schema,
    try_get_deltatable,
    types_compatible,
    validate_partition_column_names,
    validate_partition_columns_in_table,
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

    Delta Lake: https://delta.io/
    deltalake Python library: https://delta-io.github.io/delta-rs/python/
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
        schema_mode: str = "error",
        **write_kwargs,
    ):
        """Initialize DeltaDatasink.

        Args:
            path: Path to Delta table (local or cloud storage).
            mode: Write mode - APPEND, OVERWRITE, UPSERT, ERROR, or IGNORE.
            partition_cols: Columns to partition by (Hive-style).
            filesystem: Optional PyArrow filesystem.
                PyArrow filesystems: https://arrow.apache.org/docs/python/api/filesystems.html
                Note: For distributed writes, filesystem must be reconstructible from
                storage_options. Consider passing storage_options instead.
            schema: Optional explicit schema for the table.
            upsert_kwargs: Options for UPSERT mode:
                - join_cols: List of column names to match rows on (required for UPSERT)
            schema_mode: How to handle schema changes when writing to existing table:
                - "error": Reject new columns (default, safest)
                - "merge": Add new columns using DeltaTable.alter.add_columns()
                - "overwrite": Replace table schema (not recommended)
            **write_kwargs: Additional options passed to Delta writer.
        """
        _check_import(self, module="deltalake", package="deltalake")

        # Validate and normalize mode
        self.mode = self._validate_mode(mode)
        self.partition_cols = validate_partition_column_names(partition_cols or [])
        self.schema = schema
        self.write_kwargs = write_kwargs
        self._upsert_kwargs = (upsert_kwargs or {}).copy()
        self.schema_mode = self._validate_schema_mode(schema_mode)

        # Validate upsert_kwargs
        if self._upsert_kwargs and self.mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified with SaveMode.UPSERT, got {self.mode}"
            )

        # Internal state
        self._skip_write = False
        self._table_existed_at_start: bool = False
        self._new_columns_to_add: List[tuple] = []  # List of (name, type, nullable) tuples for schema evolution
        self._write_uuid: Optional[str] = None  # Store write_uuid for app_transactions
        self._new_columns_to_add: List[tuple] = []  # List of (name, type, nullable) tuples

        # Store unresolved path (original URI) for deltalake APIs which need the scheme
        self.table_uri = path

        # Set up filesystem with retry support
        # Use SubTreeFileSystem rooted at table path for consistent relative paths
        data_context = DataContext.get_current()
        resolved_paths, raw_filesystem = _resolve_paths_and_filesystem(path, filesystem)
        if len(resolved_paths) != 1:
            raise ValueError(
                f"Expected exactly one path for Delta table, got {len(resolved_paths)} paths"
            )
        self.table_root = resolved_paths[0]

        # Validate path is not empty
        if not self.table_root or not self.table_root.strip():
            raise ValueError("Delta table path cannot be empty")

        # Create SubTreeFileSystem rooted at table path
        # This ensures all filesystem operations use relative paths from table root
        # PyArrow SubTreeFileSystem: https://arrow.apache.org/docs/python/api/filesystems.html#pyarrow.fs.SubTreeFileSystem
        self.filesystem = pa_fs.SubTreeFileSystem(self.table_root, raw_filesystem)
        self.filesystem = RetryingPyFileSystem.wrap(
            self.filesystem, retryable_errors=data_context.retried_io_errors
        )

        # Store original path for deltalake APIs (which need full URI)
        self.path = self.table_root

        # Get storage options with auto-detection for cloud storage
        # These are serializable and used to reconstruct filesystem on workers
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
        # Re-create filesystem from storage_options (serializable)
        # This ensures workers use the same credentials as the driver
        data_context = DataContext.get_current()
        _, raw_filesystem = _resolve_paths_and_filesystem(self.table_uri, None)
        # Recreate SubTreeFileSystem rooted at table path
        self.filesystem = pa_fs.SubTreeFileSystem(self.table_root, raw_filesystem)
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

    def _validate_schema_mode(self, schema_mode: Any) -> str:
        """Validate schema_mode parameter."""
        if not isinstance(schema_mode, str):
            raise ValueError(f"schema_mode must be a string, got {type(schema_mode).__name__}")

        schema_mode_lower = schema_mode.lower()
        valid_modes = ["error", "merge", "overwrite"]
        if schema_mode_lower not in valid_modes:
            raise ValueError(
                f"Invalid schema_mode '{schema_mode}'. Supported: {valid_modes}"
            )
        return schema_mode_lower

    @property
    def supports_distributed_writes(self) -> bool:
        """If False, only launch write tasks on the driver's node.

        Local filesystem writes require all tasks to run on the same node
        to ensure files are accessible for the commit phase.
        """
        # Check for local:// scheme (Ray's custom scheme)
        if _is_local_scheme(self.table_uri):
            return False

        # Check for regular local filesystem paths
        uri_lower = self.table_uri.lower()
        if uri_lower.startswith("file://"):
            return False

        # Check for paths without scheme (local filesystem)
        if "://" not in self.table_uri:
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

    def _validate_and_evolve_schema(
        self,
        existing_table: "DeltaTable",
        existing_schema: pa.Schema,
        incoming_schema: pa.Schema,
    ) -> None:
        """Validate schema compatibility and handle schema evolution.

        Args:
            existing_table: Existing DeltaTable.
            existing_schema: Schema from existing table.
            incoming_schema: Schema from incoming data.

        Raises:
            ValueError: If schema_mode="error" and new columns are detected,
                or if type mismatches are found in existing columns.
        """
        existing_cols = {f.name: f.type for f in existing_schema}
        incoming_cols = {f.name: f.type for f in incoming_schema}

        # Check for type mismatches in existing columns
        mismatches = [
            c
            for c in existing_cols
            if c in incoming_cols
            and not types_compatible(existing_cols[c], incoming_cols[c])
        ]
        if mismatches:
            raise ValueError(
                f"Schema mismatch: type mismatches for existing columns: {mismatches}"
            )

        # Detect new columns
        new_columns = [c for c in incoming_cols if c not in existing_cols]

        if new_columns:
            if self.schema_mode == "error":
                raise ValueError(
                    f"New columns detected: {new_columns}. "
                    "Schema evolution is disabled by default. "
                    "To allow new columns, set schema_mode='merge'."
                )
            elif self.schema_mode == "merge":
                # Schema evolution will be handled in commit phase using alter.add_columns
                # Store field definitions (name, type, nullable) for new columns
                self._new_columns_to_add = [
                    (f.name, f.type, f.nullable)
                    for f in incoming_schema
                    if f.name in new_columns
                ]
                logger.info(
                    f"Schema evolution enabled: {len(new_columns)} new columns will be added: {new_columns}"
                )
            elif self.schema_mode == "overwrite":
                # Schema overwrite - no action needed here, handled in commit
                logger.warning(
                    "schema_mode='overwrite' will replace the table schema. "
                    "This may cause data loss if columns are removed."
                )

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
            # Infer partition_cols from existing table if not provided
            if not self.partition_cols:
                existing_partitions = existing_table.metadata().partition_columns
                if existing_partitions:
                    self.partition_cols = existing_partitions
            else:
                validate_partition_columns_match_existing(
                    existing_table, self.partition_cols
                )

            # Validate schema compatibility and handle schema evolution
            if self.schema is not None:
                existing_schema = to_pyarrow_schema(existing_table.schema())
                self._validate_and_evolve_schema(existing_table, existing_schema, self.schema)

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
            # Warn about non-atomicity
            logger.warning(
                "UPSERT mode uses two separate transactions (delete then append) "
                "and is NOT fully atomic. If the second transaction fails, "
                "deleted rows will not be restored. For atomic upserts, consider "
                "using Delta Lake's native merge() API directly."
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
            schemas from written blocks, and written_files paths.
        """
        if self._skip_write:
            return DeltaWriteResult()

        # Capture write_uuid from TaskContext (set by plan_write_op)
        # Store in instance state for app_transactions idempotence checking
        ctx_kwargs = getattr(ctx, "kwargs", None) or {}
        write_uuid = ctx_kwargs.get(WRITE_UUID_KWARG_NAME)
        self._write_uuid = write_uuid

        all_actions = []
        upsert_keys_tables = []
        block_schemas = []
        written_files = []  # Track files written by this worker
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
                actions, files = self._write_table_data(
                    table, ctx.task_idx, block_idx, write_uuid
                )
                all_actions.extend([a for a in actions if a is not None])
                written_files.extend(files)
                block_idx += 1

            # Combine upsert keys from all blocks
            upsert_keys = concat(upsert_keys_tables) if upsert_keys_tables else None

            return DeltaWriteResult(
                add_actions=all_actions,
                upsert_keys=upsert_keys,
                schemas=block_schemas,
                written_files=written_files,
                write_uuid=write_uuid,  # Pass write_uuid to driver for app_transactions
            )
        except Exception as e:
            # Store written files in exception for cleanup in on_write_failed()
            # This ensures cleanup works even if write() fails before returning
            e._delta_written_files = written_files.copy()
            # Cleanup files written by this worker on failure
            self._cleanup_files(written_files)
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

    def _write_table_data(
        self,
        table: pa.Table,
        task_idx: int,
        block_idx: int = 0,
        write_uuid: Optional[str] = None,
    ) -> tuple[List["AddAction"], List[str]]:
        """Write table data as partitioned or non-partitioned Parquet files.

        Args:
            table: PyArrow table to write.
            task_idx: Task index for filename generation.
            block_idx: Block index for filename generation.
            write_uuid: Unique identifier for this write operation.

        Returns:
            Tuple of (add_actions, written_files) where written_files are relative paths
            from table root (filesystem is SubTreeFileSystem).
        """
        # Recreate filesystem on worker using storage_options (serializable)
        # This ensures workers use the same credentials as the driver
        data_context = DataContext.get_current()
        _, raw_filesystem = _resolve_paths_and_filesystem(self.table_uri, None)
        # Recreate SubTreeFileSystem rooted at table path
        filesystem = pa_fs.SubTreeFileSystem(self.table_root, raw_filesystem)
        filesystem = RetryingPyFileSystem.wrap(
            filesystem, retryable_errors=data_context.retried_io_errors
        )

        # Track written files locally (not in instance state)
        written_files = set()

        writer = DeltaFileWriter(
            table_uri=self.table_uri,
            filesystem=filesystem,
            partition_cols=self.partition_cols,
            write_uuid=write_uuid,
            write_kwargs=self.write_kwargs,
            written_files=written_files,
        )
        actions = writer.write_table_data(table, task_idx, block_idx)
        return actions, list(written_files)

    def on_write_complete(self, write_result: WriteResult[DeltaWriteResult]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        (
            all_file_actions,
            upsert_keys,
            all_schemas,
            all_written_files,
        ) = self._collect_write_results(write_result)
        existing_table = try_get_deltatable(self.table_uri, self.storage_options)
        table_existed_before_validation = existing_table is not None

        # Validate race conditions and handle early returns
        # May update existing_table and _table_existed_at_start if table was created concurrently
        existing_table = self._validate_race_conditions(existing_table, all_written_files)

        # IGNORE mode: if table was created concurrently, files were cleaned up and we should return
        # Check if IGNORE mode handled the race condition (existing_table is None but table existed before validation)
        if (
            self.mode == SaveMode.IGNORE
            and existing_table is None
            and not self._table_existed_at_start
            and table_existed_before_validation
        ):
            # IGNORE mode handled concurrent table creation - files cleaned up, silently succeed
            return

        # Handle empty writes (no files written)
        if not all_file_actions:
            self._handle_empty_write(existing_table)
            return

        # Commit phase: validate and commit files atomically
        # Add app_transactions for idempotence (allows checking if commit succeeded)
        # Get write_uuid from WriteResult (set by workers, passed to driver)
        write_uuid = None
        if write_result.write_returns:
            # Get write_uuid from first result (all tasks share same write_uuid)
            first_result = write_result.write_returns[0]
            if isinstance(first_result, DeltaWriteResult):
                write_uuid = first_result.write_uuid

        if write_uuid:
            app_transaction = create_app_transaction_id(write_uuid)
            existing_props = normalize_commit_properties(
                self.write_kwargs.get("commit_properties")
            )
            self.write_kwargs["commit_properties"] = {
                **existing_props,
                **app_transaction,
            }

        try:
            self._reconcile_schemas(all_schemas, existing_table)
            validate_file_actions(all_file_actions, self.table_uri, self.filesystem)
            self._commit_files(existing_table, all_file_actions, upsert_keys, all_written_files)
        except Exception as commit_error:
            # On commit exception, be conservative: don't delete files
            # We can't reliably check if commit succeeded with current delta-rs API
            # (transaction_version checking not available)
            # Deleting files after a successful commit would corrupt the table
            logger.warning(
                f"Delta commit raised exception for {self.table_uri}: {commit_error}. "
                f"Not cleaning up {len(all_written_files)} files to avoid deleting "
                "potentially committed files. Use Delta Lake VACUUM to clean up if needed."
            )
            # Store error info for debugging
            commit_error._delta_written_files = all_written_files
            raise

    def _validate_race_conditions(
        self, existing_table: Optional["DeltaTable"], all_written_files: List[str]
    ) -> Optional["DeltaTable"]:
        """Validate race conditions and raise errors or cleanup as needed.

        Args:
            existing_table: DeltaTable that exists now (may be None).
            all_written_files: List of file paths written by workers (for cleanup).

        Returns:
            Updated existing_table if race condition handling changed it, None otherwise.
        """
        # ERROR mode: table created after write started
        if (
            not self._table_existed_at_start
            and existing_table is not None
            and self.mode == SaveMode.ERROR
        ):
            self._cleanup_files(all_written_files)
            raise ValueError(
                f"Race condition: Delta table was created at {self.table_uri} "
                "after write started. Use SaveMode.APPEND or OVERWRITE."
            )

        # IGNORE mode: table created concurrently
        if (
            not self._table_existed_at_start
            and existing_table is not None
            and self.mode == SaveMode.IGNORE
        ):
            self._cleanup_files(all_written_files)
            return None

        # Table deleted after write started (for APPEND/UPSERT)
        if (
            self._table_existed_at_start
            and existing_table is None
            and self.mode in (SaveMode.APPEND, SaveMode.UPSERT)
        ):
            self._cleanup_files(all_written_files)
            raise ValueError(
                f"Delta table was deleted at {self.table_uri} after write started. "
                "Use SaveMode.OVERWRITE to create a new table."
            )

        # APPEND/OVERWRITE mode: table created concurrently - return it so we can append/overwrite
        # This is not an error, just a race condition we need to handle
        if (
            not self._table_existed_at_start
            and existing_table is not None
            and self.mode in (SaveMode.APPEND, SaveMode.OVERWRITE)
        ):
            # Validate partition columns match the concurrently-created table
            # (on_write_start skipped this validation since table didn't exist then)
            validate_partition_columns_match_existing(existing_table, self.partition_cols)
            # Update state to reflect that table now exists
            self._table_existed_at_start = True
            return existing_table

        return existing_table

    def _handle_empty_write(self, existing_table: Optional["DeltaTable"]) -> None:
        """Handle empty writes (no files written)."""
        if self._table_existed_at_start:
            if existing_table is not None:
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
            elif self.mode == SaveMode.OVERWRITE:
                # Table was deleted during write - create new empty table (consistent with non-empty writes)
                if self.schema:
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

        # Create empty table if schema provided and table doesn't exist
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

    def _reconcile_schemas(
        self, all_schemas: List[pa.Schema], existing_table: Optional["DeltaTable"]
    ) -> None:
        """Reconcile schemas from all workers with type promotion."""
        if not all_schemas:
            return

        if existing_table:
            table_schema = to_pyarrow_schema(existing_table.schema())
            reconciled_schema = unify_schemas(
                [table_schema] + all_schemas, promote_types=True
            )
        else:
            reconciled_schema = unify_schemas(all_schemas, promote_types=True)

        if reconciled_schema:
            self.schema = reconciled_schema

    def _commit_files(
        self,
        existing_table: Optional["DeltaTable"],
        all_file_actions: List["AddAction"],
        upsert_keys: Optional[pa.Table],
        all_written_files: List[str],
    ) -> None:
        """Commit files to table based on whether it existed at start."""
        if self._table_existed_at_start:
            self._commit_to_existing_table_state(
                existing_table, all_file_actions, upsert_keys, all_written_files
            )
        else:
            self._commit_to_new_table(existing_table, all_file_actions, all_written_files)

    def _commit_to_existing_table_state(
        self,
        existing_table: Optional["DeltaTable"],
        all_file_actions: List["AddAction"],
        upsert_keys: Optional[pa.Table],
        all_written_files: List[str],
    ) -> None:
        """Commit files when table existed at start."""
        if self.mode == SaveMode.IGNORE:
            self._cleanup_files(all_written_files)
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
            # Handle schema evolution before committing files
            if self._new_columns_to_add and self.schema_mode == "merge":
                self._evolve_schema(existing_table, self._new_columns_to_add)
            # Commit to existing table (APPEND, OVERWRITE, or UPSERT)
            self._commit_by_mode(existing_table, all_file_actions, upsert_keys)
        else:
            raise ValueError(
                f"Delta table was deleted at {self.table_uri} after write started."
            )

    def _commit_to_new_table(
        self,
        existing_table: Optional["DeltaTable"],
        all_file_actions: List["AddAction"],
        all_written_files: List[str],
    ) -> None:
        """Commit files when table didn't exist at start.

        Args:
            existing_table: Table that exists now (may be None or created concurrently).
            all_file_actions: File actions to commit.
            all_written_files: Files written for cleanup on failure.
        """
        # Check if table was created concurrently (race condition)
        if existing_table is not None:
            # Table was created concurrently - handle based on mode
            if self.mode == SaveMode.APPEND:
                # Append to concurrently-created table
                self._commit_by_mode(existing_table, all_file_actions, None)
                return
            elif self.mode == SaveMode.OVERWRITE:
                # Overwrite the concurrently-created table
                self._commit_by_mode(existing_table, all_file_actions, None)
                return
            elif self.mode == SaveMode.IGNORE:
                # Table was created concurrently, clean up files
                self._cleanup_files(all_written_files)
                return
            # ERROR mode already handled in _validate_race_conditions

        # No table exists - create new one
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

    def _evolve_schema(
        self, existing_table: "DeltaTable", new_fields: List[tuple]
    ) -> None:
        """Add new columns to existing table using DeltaTable.alter.add_columns.

        This is a separate transaction from the file commit, so not perfectly atomic,
        but it's the correct way to evolve schema in delta-rs.

        Args:
            existing_table: DeltaTable to evolve.
            new_fields: List of (name, type, nullable) tuples for new columns.
        """

        # Convert PyArrow types to Delta Lake types
        delta_fields = []
        for name, pa_type, nullable in new_fields:
            # Convert PyArrow type to Delta Lake DataType string
            from ray.data._internal.datasource.delta.utils import (
                convert_schema_to_delta,
            )

            # Create a temporary schema with just this field to convert it
            temp_schema = pa.schema([pa.field(name, pa_type, nullable)])
            delta_schema = convert_schema_to_delta(temp_schema)
            # Extract the field from the converted schema
            delta_field = delta_schema.fields[0]
            delta_fields.append(delta_field)

        if delta_fields:
            # Use alter.add_columns to evolve schema
            # DeltaTable.alter API: https://delta-io.github.io/delta-rs/python/api/deltalake.table.html#deltalake.table.DeltaTable.alter
            existing_table.alter.add_columns(delta_fields)

    def _check_commit_succeeded(self, table: "DeltaTable") -> bool:
        """Check if commit succeeded using app_transactions.

        Uses transaction_version to verify if our app_transaction was recorded,
        indicating the commit succeeded even if client saw an exception.

        Args:
            table: DeltaTable to check.

        Returns:
            True if commit succeeded (transaction_version matches), False otherwise.
        """
        if not self._write_uuid:
            return False

        try:
            app_transaction = create_app_transaction_id(self._write_uuid)
            app_id = "ray.data.write_delta"
            # Check transaction_version for this app_id
            # If it matches our expected version, commit succeeded
            # Note: delta-rs may not expose transaction_version directly,
            # so we check if table version increased (heuristic)
            # For now, return False to be safe (don't delete potentially committed files)
            # TODO: Use delta-rs transaction_version API when available
            return False  # Conservative: assume commit may have succeeded
        except Exception:
            # If we can't check, be conservative and assume commit may have succeeded
            return False

    def _commit_by_mode(
        self,
        table: "DeltaTable",
        file_actions: List["AddAction"],
        upsert_keys: Optional[pa.Table],
    ) -> None:
        """Commit files to existing table based on write mode.
        
        Note: app_transactions are added in on_write_complete() before calling this.
        """
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
    ) -> tuple[List["AddAction"], Optional[pa.Table], List[pa.Schema], List[str]]:
        """Collect all results from distributed write tasks.

        Args:
            write_result: WriteResult containing DeltaWriteResult from each task.

        Returns:
            Tuple of (all_file_actions, combined_upsert_keys, all_schemas, all_written_files).
        """
        if not write_result.write_returns:
            return [], None, [], []

        all_actions = []
        upsert_keys_tables = []
        all_schemas = []
        all_written_files = []

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
            # Collect written files from all workers for cleanup
            if result.written_files:
                all_written_files.extend(result.written_files)

        # Validate no duplicate file paths (shouldn't happen but check for safety)
        seen_paths = set()
        for action in all_actions:
            if action.path in seen_paths:
                raise ValueError(f"Duplicate file paths detected: {action.path}")
            seen_paths.add(action.path)

        # Combine upsert keys from all workers
        upsert_keys = concat(upsert_keys_tables) if upsert_keys_tables else None

        return all_actions, upsert_keys, all_schemas, all_written_files

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure - attempt cleanup of orphaned files.

        Attempts to clean up files written before the failure. Files are tracked
        in two ways:
        1. If write() completed, files are in DeltaWriteResult.written_files
        2. If write() failed early, files are stored in error._delta_written_files

        Args:
            error: The exception that caused the write to fail.
        """
        # Try to get written files from exception (set in write() on failure)
        written_files = getattr(error, "_delta_written_files", [])

        # Also try to get from write_result if available (for failures after write() returns)
        # Note: This may not be available if write() never returned
        if not written_files:
            # Fallback: check if error has write_result attached
            write_result = getattr(error, "_write_result", None)
            if write_result and hasattr(write_result, "write_returns"):
                all_written_files = []
                for result in write_result.write_returns:
                    if result and hasattr(result, "written_files"):
                        all_written_files.extend(result.written_files)
                written_files = all_written_files

        if written_files:
            logger.warning(
                f"Delta write failed for {self.table_uri}. "
                f"Attempting to cleanup {len(written_files)} orphaned files."
            )
            self._cleanup_files(written_files)
        else:
            logger.error(
                f"Delta write failed for {self.table_uri}: {error}. "
                "Could not determine which files were written. "
                "Orphaned files may require manual cleanup using Delta Lake VACUUM."
            )

    def _cleanup_files(self, file_paths: List[str]) -> None:
        """Clean up files by their relative paths.

        Args:
            file_paths: List of relative file paths from table root (filesystem is SubTreeFileSystem).
        """
        for file_path in file_paths:
            try:
                file_info = get_file_info_with_retry(self.filesystem, file_path)
                if file_info.type != pa_fs.FileType.NotFound:
                    self.filesystem.delete_file(file_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {file_path}: {e}")
