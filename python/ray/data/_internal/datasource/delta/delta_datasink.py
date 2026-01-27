"""Datasink for writing Ray Data to Delta Lake tables."""

import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.arrow_ops.transform_pyarrow import concat as concat_arrow_tables
from ray.data._internal.datasource.delta.utils import (
    UPSERT_JOIN_COLS,
    DeltaWriteResult,
    build_partition_path,
    compute_parquet_statistics,
    convert_schema_to_delta,
    get_file_info_with_retry,
    get_storage_options,
    infer_partition_type,
    join_delta_path,
    safe_dirname,
    schemas_compatible,
    to_pyarrow_schema,
    try_get_deltatable,
    types_compatible,
    validate_file_path,
    validate_partition_column_names,
    validate_partition_columns_in_table,
    validate_partition_value,
)
from ray.data._internal.datasource.parquet_datasink import (
    WRITE_FILE_MAX_ATTEMPTS,
    WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
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

# Maximum number of partitions to prevent filesystem issues
_MAX_PARTITIONS = 10000


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
            if mode not in (
                SaveMode.APPEND,
                SaveMode.OVERWRITE,
                SaveMode.UPSERT,
                SaveMode.ERROR,
                SaveMode.IGNORE,
            ):
                raise ValueError(f"Unsupported SaveMode for Delta: {mode}")
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
        return not _is_local_scheme(self.table_uri)

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
        _check_import(self, module="deltalake", package="deltalake")

        # Store schema from first input bundle
        if schema is not None and self.schema is None:
            self.schema = schema

        # Check if table exists at start time
        existing_table = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing_table is not None

        if existing_table:
            self._validate_partition_columns_match_existing(existing_table)

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

        _check_import(self, module="deltalake", package="deltalake")

        # Capture write_uuid from TaskContext (set by plan_write_op)
        ctx_kwargs = getattr(ctx, "kwargs", None) or {}
        self._write_uuid = ctx_kwargs.get(WRITE_UUID_KWARG_NAME)

        all_actions = []
        upsert_keys_tables = []
        block_schemas = []
        block_idx = 0
        empty_block_count = 0
        use_upsert = self.mode == SaveMode.UPSERT

        try:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)
                if block_accessor.num_rows() == 0:
                    empty_block_count += 1
                    continue

                table = block_accessor.to_arrow()
                self._validate_table_schema(table)
                validate_partition_columns_in_table(self.partition_cols, table)
                self._validate_partition_column_types(table)

                # Collect schema for validation
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

            if empty_block_count > 0 and len(all_actions) == 0:
                logger.warning(
                    f"All {empty_block_count} blocks were empty. "
                    f"No files written to {self.table_uri}"
                )

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
                if not types_compatible(field.type, table[field.name].type):
                    raise ValueError(
                        f"Type mismatch for '{field.name}': expected {field.type}, "
                        f"got {table[field.name].type}"
                    )

    def _validate_partition_column_types(self, table: pa.Table) -> None:
        """Validate partition column types are supported (not nested/dictionary)."""
        if not self.partition_cols:
            return
        for col in self.partition_cols:
            if col not in table.column_names:
                continue  # Missing columns are caught by validate_partition_columns_in_table
            col_type = table.schema.field(col).type
            if pa.types.is_nested(col_type) or pa.types.is_dictionary(col_type):
                raise ValueError(
                    f"Partition column '{col}' has unsupported type {col_type}. "
                    "Partition columns must be primitive types (not nested, dictionary, etc.)"
                )

    def _write_table_data(
        self, table: pa.Table, task_idx: int, block_idx: int = 0
    ) -> List["AddAction"]:
        """Write table data as partitioned or non-partitioned Parquet files."""
        if self.partition_cols:
            partitioned_tables = self._partition_table(table, self.partition_cols)
            actions = [
                self._write_partition(
                    partition_table, partition_values, task_idx, block_idx
                )
                for partition_values, partition_table in partitioned_tables.items()
            ]
            return [a for a in actions if a is not None]
        # For non-partitioned writes, pass empty tuple for partition_values
        # This is validated in _write_partition which expects tuple format
        action = self._write_partition(table, (), task_idx, block_idx)
        return [action] if action is not None else []

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """Partition table by columns efficiently using vectorized operations.

        Uses PyArrow compute functions for efficient partitioning. For single partition
        column, uses pc.unique() and pc.equal() filters. For multiple columns, groups
        by partition value tuples.

        PyArrow compute functions: https://arrow.apache.org/docs/python/api/compute.html
        """
        from collections import defaultdict

        import pyarrow.compute as pc

        if len(table) == 0:
            return {}

        partitions = {}
        if len(partition_cols) == 1:
            col = partition_cols[0]
            unique_vals = pc.unique(table[col])
            if len(unique_vals) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_vals)}). Max: {_MAX_PARTITIONS}"
                )
            for val in unique_vals:
                val_py = val.as_py()
                validate_partition_value(val_py)
                if val_py is None:
                    filtered = table.filter(pc.is_null(table[col]))
                elif pa.types.is_floating(table[col].type) and (
                    isinstance(val_py, float) and val_py != val_py
                ):  # NaN check: val_py != val_py is True only for NaN
                    filtered = table.filter(pc.is_nan(table[col]))
                else:
                    filtered = table.filter(pc.equal(table[col], val))
                if len(filtered) > 0:
                    partitions[(val_py,)] = filtered
        else:
            import math

            # Sentinel object for NaN values to ensure consistent tuple keys
            # (NaN != NaN in Python, so we use a sentinel object)
            _NAN_SENTINEL = object()

            val_lists = [table[col].to_pylist() for col in partition_cols]
            indices = defaultdict(list)
            for idx, tup in enumerate(zip(*val_lists)):
                # Normalize NaN values to sentinel to ensure consistent tuple keys
                # (NaN != NaN in Python, so we normalize to a sentinel object)
                normalized_tup = tuple(
                    _NAN_SENTINEL if isinstance(v, float) and math.isnan(v) else v
                    for v in tup
                )
                for v in tup:
                    validate_partition_value(v)
                indices[normalized_tup].append(idx)
            if len(indices) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition combinations ({len(indices)}). Max: {_MAX_PARTITIONS}"
                )
            for tup, idxs in indices.items():
                # Convert sentinel back to NaN for partition key
                # (Delta Lake uses NaN as partition value)
                partition_key = tuple(
                    float("nan") if v is _NAN_SENTINEL else v for v in tup
                )
                partitioned = table.take(idxs)
                if len(partitioned) > 0:
                    partitions[partition_key] = partitioned
        return partitions

    def _write_partition(
        self,
        table: pa.Table,
        partition_values: tuple,
        task_idx: int,
        block_idx: int = 0,
    ) -> Optional["AddAction"]:
        """Write a single partition to Parquet file and create AddAction metadata.

        Writes Parquet file to storage, computes statistics, and creates AddAction
        object with file metadata. AddAction is used later to commit to Delta log.

        deltalake AddAction: https://delta-io.github.io/delta-rs/python/api/deltalake.transaction.html#deltalake.transaction.AddAction
        """
        from deltalake.transaction import AddAction

        if len(table) == 0:
            return None

        filename = self._generate_filename(task_idx, block_idx)
        partition_path, partition_dict = build_partition_path(
            self.partition_cols, partition_values
        )
        relative_path = partition_path + filename

        validate_file_path(relative_path)
        # Use filesystem-safe path joining to prevent path traversal
        # relative_path is already validated to not contain ".."
        full_path = join_delta_path(self.path, relative_path)

        self._written_files.add(full_path)
        table_to_write = self._prepare_table_for_write(table)
        file_size = self._write_parquet_file(table_to_write, full_path)
        file_statistics = compute_parquet_statistics(table_to_write)

        return AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=file_statistics,
        )

    def _generate_filename(self, task_idx: int, block_idx: int = 0) -> str:
        """Generate unique Parquet filename.

        Format: part-{write_uuid[:8]}-{task_idx:05d}-{block_idx:05d}-{unique_id}.parquet
        Includes write_uuid prefix for correlation, task and block indices for debugging,
        plus per-file UUID for uniqueness.
        """
        unique_id = uuid.uuid4().hex[:16]
        # Handle None, empty string, or too-short write_uuid
        if self._write_uuid and len(self._write_uuid) >= 8:
            write_prefix = self._write_uuid[:8]
        elif self._write_uuid:
            write_prefix = self._write_uuid.ljust(8, "0")
        else:
            write_prefix = "00000000"
        return f"part-{write_prefix}-{task_idx:05d}-{block_idx:05d}-{unique_id}.parquet"

    def _write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """Write PyArrow table to Parquet file and return file size.

        Uses PyArrow Parquet writer with compression and statistics enabled.
        Uses call_with_retry for robust retry behavior with exponential backoff.

        PyArrow Parquet: https://arrow.apache.org/docs/python/parquet.html
        """
        from ray._common.retry import call_with_retry

        compression = self.write_kwargs.get("compression", "snappy")
        valid_compressions = ["snappy", "gzip", "brotli", "zstd", "lz4", "none"]
        if compression not in valid_compressions:
            raise ValueError(
                f"Invalid compression '{compression}'. Supported: {valid_compressions}"
            )

        self._ensure_parent_directory(file_path)
        write_statistics = self.write_kwargs.get("write_statistics", True)
        data_context = DataContext.get_current()

        # Store result in list to capture from inner function
        file_size_result = [0]

        def write_and_verify():
            pq.write_table(
                table,
                file_path,
                filesystem=self.filesystem,
                compression=compression,
                write_statistics=write_statistics,
            )
            file_info = get_file_info_with_retry(self.filesystem, file_path)
            if file_info.size == 0:
                try:
                    self.filesystem.delete_file(file_path)
                except Exception as delete_err:
                    logger.debug(
                        f"Failed to delete empty file {file_path}: {delete_err}"
                    )
                raise RuntimeError(f"Written file is empty: {file_path}")
            file_size_result[0] = file_info.size

        call_with_retry(
            write_and_verify,
            description=f"write Parquet file '{file_path}'",
            match=data_context.retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

        return file_size_result[0]

    def _prepare_table_for_write(self, table: pa.Table) -> pa.Table:
        """Prepare table for writing by removing partition columns."""
        return table if not self.partition_cols else table.drop(self.partition_cols)

    def _ensure_parent_directory(self, file_path: str) -> None:
        """Create parent directory for file if needed."""
        parent_dir = safe_dirname(file_path)
        if parent_dir:
            try:
                self.filesystem.create_dir(parent_dir, recursive=True)
            except (FileExistsError, Exception):
                pass  # OK - directory exists or cloud storage doesn't need it

    def on_write_complete(self, write_result: WriteResult[DeltaWriteResult]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        all_file_actions, upsert_keys = self._collect_write_results(write_result)
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

        # Handle empty writes
        if not all_file_actions:
            if self._table_existed_at_start and existing_table is not None:
                # For OVERWRITE mode with empty dataset, clear the table
                if self.mode == SaveMode.OVERWRITE:
                    self._commit_to_existing_table(existing_table, [])
                    return
                # For IGNORE mode, do nothing if table exists
                if self.mode == SaveMode.IGNORE:
                    return
            # Create empty table if schema provided and table doesn't exist
            if self.schema and not existing_table:
                self._create_empty_table()
            return

        # Wrap commit phase in try/except to ensure cleanup on failure
        # (files were written by workers, so _written_files is empty on driver)
        try:
            self._validate_file_actions(all_file_actions)

            # Commit based on mode
            if self._table_existed_at_start:
                if self.mode == SaveMode.IGNORE:
                    self._cleanup_written_files(all_file_actions)
                    return
                if existing_table is None and self.mode == SaveMode.OVERWRITE:
                    self._create_table_with_files(all_file_actions)
                elif existing_table is not None:
                    self._commit_by_mode(existing_table, all_file_actions, upsert_keys)
                else:
                    raise ValueError(
                        f"Delta table was deleted at {self.table_uri} after write started."
                    )
            else:
                self._create_table_with_files(all_file_actions)
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
            self._commit_upsert(table, file_actions, upsert_keys)
        else:
            self._commit_to_existing_table(table, file_actions)

    def _commit_upsert(
        self,
        table: "DeltaTable",
        file_actions: List["AddAction"],
        upsert_keys: Optional[pa.Table],
    ) -> None:
        """Commit upsert: delete matching rows, then append. NOT fully atomic."""
        logger.warning(
            "UPSERT uses two transactions (delete then append) and is not atomic. "
            "For atomic upsert, use deltalake's native merge() API."
        )
        import functools

        import pyarrow.compute as pc
        from deltalake.transaction import CommitProperties

        upsert_cols = self._get_upsert_cols()

        # Build delete predicate from upsert keys
        if upsert_keys is not None and len(upsert_keys) > 0:
            # Filter out rows with NULL or NaN values in join columns
            # (NULL != NULL and NaN != NaN in SQL semantics, causing silent duplicates)
            masks = []
            for col in upsert_cols:
                col_arr = upsert_keys[col]
                # Filter NULL values
                valid_mask = pc.is_valid(col_arr)
                # Also filter NaN for float columns
                if pa.types.is_floating(col_arr.type):
                    not_nan_mask = pc.invert(pc.is_nan(col_arr))
                    valid_mask = pc.and_(valid_mask, not_nan_mask)
                masks.append(valid_mask)
            mask = functools.reduce(pc.and_, masks)
            keys_table = upsert_keys.filter(mask)

            if len(keys_table) > 0:
                # Build IN predicate for each key column
                delete_predicate = self._build_delete_predicate(keys_table, upsert_cols)
                if delete_predicate:
                    table.delete(delete_predicate)

        # Append new data files
        commit_properties = self.write_kwargs.get("commit_properties")
        if isinstance(commit_properties, dict):
            commit_properties = CommitProperties(**commit_properties)

        # Get schema for transaction
        # Use delta schema directly for transaction
        delta_schema = table.schema()

        table.create_write_transaction(
            file_actions,
            mode="append",
            schema=delta_schema,
            partition_by=self.partition_cols or None,
            commit_properties=commit_properties,
        )

    def _quote_identifier(self, name: str) -> str:
        """Quote column name with backticks for SQL predicates."""
        return f"`{name.replace('`', '``')}`"

    def _build_delete_predicate(
        self, keys_table: pa.Table, upsert_cols: List[str]
    ) -> Optional[str]:
        """Build SQL delete predicate: (`col1` = 'val1' AND `col2` = 'val2') OR ..."""
        if len(keys_table) == 0:
            return None

        # For efficiency with large key sets, use IN clause for single column
        if len(upsert_cols) == 1:
            col = upsert_cols[0]
            quoted_col = self._quote_identifier(col)
            values = keys_table.column(col).unique().to_pylist()
            if not values:
                return None

            # Limit single-column IN clause to prevent excessively large predicates
            max_in_values = 10000
            if len(values) > max_in_values:
                raise ValueError(
                    f"Upsert has {len(values)} unique key values, exceeding limit of "
                    f"{max_in_values}. Batch your upserts into smaller chunks."
                )

            # Format values for SQL (filter out NaN which can't be compared)
            formatted_vals = ", ".join(self._format_sql_value(v) for v in values)
            return f"{quoted_col} IN ({formatted_vals})"

        # For compound keys, deduplicate first then build OR of ANDed conditions
        # Use group_by to get unique compound key combinations

        unique_keys = keys_table.group_by(upsert_cols).aggregate([])

        max_keys = 1000
        if len(unique_keys) > max_keys:
            raise ValueError(
                f"Upsert has {len(unique_keys)} unique compound keys, exceeding "
                f"limit of {max_keys}. Batch your upserts into smaller chunks."
            )

        conditions = []
        for i in range(len(unique_keys)):
            row_conditions = []
            for col in upsert_cols:
                quoted_col = self._quote_identifier(col)
                val = unique_keys.column(col)[i].as_py()
                row_conditions.append(f"{quoted_col} = {self._format_sql_value(val)}")
            conditions.append(f"({' AND '.join(row_conditions)})")

        return " OR ".join(conditions)

    def _format_sql_value(self, value: Any) -> str:
        """Format a Python value for SQL predicate."""
        import datetime
        import math

        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                raise ValueError(f"Cannot format {value} for SQL predicate")
            return str(value)
        if isinstance(value, (datetime.datetime, datetime.date)):
            return f"'{value.isoformat()}'"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _collect_write_results(
        self, write_result: WriteResult[DeltaWriteResult]
    ) -> tuple[List["AddAction"], Optional[pa.Table]]:
        """Collect all results from distributed write tasks.

        Args:
            write_result: WriteResult containing DeltaWriteResult from each task.

        Returns:
            Tuple of (all_file_actions, combined_upsert_keys).
        """
        if not write_result.write_returns:
            return [], None

        all_actions = []
        upsert_keys_tables = []

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

        # Check for duplicate paths
        seen_paths = set()
        duplicates = set()
        for action in all_actions:
            if action.path in seen_paths:
                duplicates.add(action.path)
            seen_paths.add(action.path)
        if duplicates:
            raise ValueError(f"Duplicate file paths detected: {duplicates}")

        # Combine upsert keys from all workers
        upsert_keys = (
            concat_arrow_tables(upsert_keys_tables) if upsert_keys_tables else None
        )

        return all_actions, upsert_keys

    def _validate_file_actions(self, file_actions: List["AddAction"]) -> None:
        """Validate file actions before committing."""
        for action in file_actions:
            validate_file_path(action.path)
            full_path = join_delta_path(self.path, action.path)
            file_info = get_file_info_with_retry(self.filesystem, full_path)
            if file_info.type == pa_fs.FileType.NotFound:
                raise ValueError(f"File does not exist: {full_path}")
            if file_info.size == 0:
                raise ValueError(f"File is empty: {full_path}")

    def _create_empty_table(self) -> None:
        """Create empty Delta table with specified schema."""
        if not self.schema:
            raise ValueError("Cannot create empty table without explicit schema")
        self._create_table_with_files([])

    def _create_table_with_files(self, file_actions: List["AddAction"]) -> None:
        """Create new Delta table and commit files atomically."""
        from deltalake.transaction import create_table_with_add_actions

        schema = self.schema if not file_actions else self._infer_schema(file_actions)
        delta_schema = convert_schema_to_delta(schema)

        create_table_with_add_actions(
            table_uri=self.table_uri,
            schema=delta_schema,
            add_actions=file_actions,
            mode=self.mode.value,
            partition_by=self.partition_cols or None,
            name=self.write_kwargs.get("name"),
            description=self.write_kwargs.get("description"),
            configuration=self.write_kwargs.get("configuration"),
            storage_options=self.storage_options,
            commit_properties=self.write_kwargs.get("commit_properties"),
            post_commithook_properties=self.write_kwargs.get(
                "post_commithook_properties"
            ),
        )

    def _commit_to_existing_table(
        self, existing_table: "DeltaTable", file_actions: List["AddAction"]
    ) -> None:
        """Commit files to existing Delta table using write transaction.

        Validates schema compatibility, then creates a write transaction to commit
        file metadata atomically to the Delta transaction log.

        Delta Lake transactions: https://delta.io/specification/#transaction-log-entries
        deltalake write transaction: https://delta-io.github.io/delta-rs/python/api/deltalake.table.html#deltalake.table.DeltaTable.create_write_transaction
        """
        # Always validate schema compatibility, even for empty writes
        existing_schema = to_pyarrow_schema(existing_table.schema())
        if file_actions:
            inferred_schema = self._infer_schema(file_actions)
        elif self.schema:
            inferred_schema = self.schema
        else:
            # No file actions and no schema - skip validation
            inferred_schema = None

        if inferred_schema and not schemas_compatible(existing_schema, inferred_schema):
            existing_cols = {f.name: f.type for f in existing_schema}
            inferred_cols = {f.name: f.type for f in inferred_schema}
            # For append operations, inferred can have fewer columns (missing is OK)
            # but all inferred columns must match existing schema
            extra = sorted(set(inferred_cols) - set(existing_cols))
            mismatches = [
                c
                for c in existing_cols
                if c in inferred_cols
                and not types_compatible(existing_cols[c], inferred_cols[c])
            ]
            msg = "Schema mismatch"
            if extra:
                msg += f": extra columns in data that don't exist in table {extra}"
            if mismatches:
                msg += f": type mismatches {mismatches}"
            raise ValueError(msg)

        self._validate_partition_columns_match_existing(existing_table)
        transaction_mode = "overwrite" if self.mode == SaveMode.OVERWRITE else "append"
        # Use Delta schema directly for create_write_transaction
        delta_schema = existing_table.schema()
        existing_table.create_write_transaction(
            actions=file_actions,
            mode=transaction_mode,
            schema=delta_schema,
            partition_by=self.partition_cols or None,
            commit_properties=self.write_kwargs.get("commit_properties"),
            post_commithook_properties=self.write_kwargs.get(
                "post_commithook_properties"
            ),
        )

    def _validate_partition_columns_match_existing(
        self, existing_table: "DeltaTable"
    ) -> None:
        """Validate partition columns align with the existing table metadata."""
        existing_partitions = existing_table.metadata().partition_columns
        if self.partition_cols:
            if existing_partitions and existing_partitions != self.partition_cols:
                raise ValueError(
                    f"Partition columns mismatch. Existing: {existing_partitions}, requested: {self.partition_cols}"
                )
            if not existing_partitions and self.partition_cols:
                raise ValueError(
                    f"Partition columns provided {self.partition_cols} but table is not partitioned."
                )
        elif existing_partitions:
            raise ValueError(
                f"Table is partitioned by {existing_partitions} but no partition columns were provided."
            )

    def _infer_schema(self, add_actions: List["AddAction"]) -> pa.Schema:
        """Infer schema from first Parquet file and partition columns."""
        if self.schema:
            return self.schema

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

        first_file = join_delta_path(self.path, first_action.path)
        try:
            with self.filesystem.open_input_file(first_file) as file_obj:
                parquet_file = pq.ParquetFile(file_obj)
                schema = parquet_file.schema_arrow
        except Exception as e:
            raise ValueError(f"Failed to read schema from {first_file}: {e}") from e

        if len(schema) == 0:
            raise ValueError(
                f"Cannot infer schema from file with no columns: {first_file}"
            )

        # Add partition columns to schema if not present
        if self.partition_cols:
            for col in self.partition_cols:
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

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure - attempt cleanup of orphaned files.

        Note: In distributed execution, cleanup may be incomplete as file paths
        are tracked in worker processes. Orphaned files may remain and require
        manual cleanup.
        """
        # Get write_id safely handling None, empty, or short strings
        if self._write_uuid and len(self._write_uuid) >= 8:
            write_id = self._write_uuid[:8]
        elif self._write_uuid:
            write_id = self._write_uuid
        else:
            write_id = "unknown"

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
            for action in file_actions:
                if action and action.path:
                    full_path = join_delta_path(self.path, action.path)
                    files_to_cleanup.add(full_path)
        else:
            files_to_cleanup = self._written_files.copy()

        for file_path in files_to_cleanup:
            try:
                file_info = get_file_info_with_retry(self.filesystem, file_path)
                if file_info.type != pa_fs.FileType.NotFound:
                    self.filesystem.delete_file(file_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {file_path}: {e}")
        self._written_files.clear()
