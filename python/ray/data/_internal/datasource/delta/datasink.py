"""Datasink for writing Ray Data to Delta Lake tables.

This module orchestrates the two-phase commit protocol: write Parquet files,
then commit to transaction log. It delegates details to focused modules.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

from .committer import (
    CommitInputs,
    commit_to_existing_table,
    create_table_with_files,
    validate_file_actions,
    validate_partition_columns_match_existing,
)
from .fs import make_fs_config, worker_filesystem
from .schema import (
    SchemaPolicy,
    evolve_schema,
    existing_table_pyarrow_schema,
    reconcile_worker_schemas,
    validate_and_plan_evolution,
)
from .upsert import commit_upsert
from .writer import DeltaFileWriter
from ray.data._internal.arrow_ops.transform_pyarrow import concat
from ray.data._internal.datasource.delta.utils import (
    UPSERT_JOIN_COLS,
    DeltaWriteResult,
    create_app_transaction_id,
    get_file_info_with_retry,
    get_storage_options,
    normalize_commit_properties,
    try_get_deltatable,
    validate_partition_column_names,
    validate_partition_columns_in_table,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import _check_import, _is_local_scheme
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction, CommitProperties

logger = logging.getLogger(__name__)


def _build_commit_properties(
    write_kwargs: Dict[str, Any], write_uuid: Optional[str]
) -> Optional["CommitProperties"]:
    """Build CommitProperties for THIS commit only. Does not mutate write_kwargs.

    Adds app_transactions (deduped) and max_commit_retries if provided.

    Args:
        write_kwargs: Write kwargs dict (read-only).
        write_uuid: Unique identifier for this write operation.

    Returns:
        CommitProperties with app_transaction merged and retries configured.
    """
    from deltalake.transaction import CommitProperties

    existing = normalize_commit_properties(write_kwargs.get("commit_properties"))
    max_retries = write_kwargs.get("max_commit_retries")  # new knob (optional)

    # app_transactions for idempotence
    app_txn = create_app_transaction_id(write_uuid) if write_uuid else None

    if existing is None:
        return CommitProperties(
            custom_metadata=None,
            max_commit_retries=max_retries,
            app_transactions=[app_txn] if app_txn else None,
        )

    # Dedup app_transactions by (app_id, version) if present
    txns = list(existing.app_transactions or [])
    if app_txn:
        key = (app_txn.app_id, app_txn.version)
        seen = {(t.app_id, t.version) for t in txns}
        if key not in seen:
            txns.append(app_txn)

    return CommitProperties(
        custom_metadata=existing.custom_metadata,
        max_commit_retries=max_retries
        if max_retries is not None
        else existing.max_commit_retries,
        app_transactions=txns or None,
    )


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
            **write_kwargs: Additional options passed to Delta writer:
                - target_file_size_bytes: Target file size for buffering (optional).
                  When set, buffers data per partition until threshold is reached.
                - max_commit_retries: Maximum retries for commit operations (optional).
                - compression: Compression codec (default: "snappy").
                - write_statistics: Whether to write Parquet statistics (default: True).
                - partition_overwrite_mode: For OVERWRITE mode with partitioned tables:
                  - "static": Delete all data before writing (default)
                  - "dynamic": Only delete partitions being written (more efficient)
        """
        _check_import(self, module="deltalake", package="deltalake")

        self.table_uri = path
        self.mode = self._validate_mode(mode)
        self.partition_cols = validate_partition_column_names(partition_cols or [])
        self.schema = schema
        self.write_kwargs = write_kwargs
        self._upsert_kwargs = dict(upsert_kwargs or {})
        self._schema_policy = SchemaPolicy(mode=schema_mode.lower())

        if self._upsert_kwargs and self.mode != SaveMode.UPSERT:
            raise ValueError("upsert_kwargs can only be specified with SaveMode.UPSERT")

        self._skip_write = False
        self._table_existed_at_start = False

        self.storage_options = get_storage_options(
            self.table_uri, write_kwargs.get("storage_options")
        )
        self._fs_config, self.filesystem = make_fs_config(
            self.table_uri, filesystem, self.storage_options
        )

        # per-worker cache (used in write())
        self._worker_fs: Optional[pa_fs.FileSystem] = None
        # new: control small files
        self._target_file_size_bytes: Optional[int] = write_kwargs.get(
            "target_file_size_bytes"
        )
        if (
            self._target_file_size_bytes is not None
            and self._target_file_size_bytes <= 0
        ):
            raise ValueError("target_file_size_bytes must be > 0")

    def __getstate__(self) -> dict:
        """Exclude non-serializable state during pickling."""
        d = self.__dict__.copy()
        d.pop("filesystem", None)
        d.pop("_worker_fs", None)
        return d

    def __setstate__(self, state: dict) -> None:
        """Restore state and re-create filesystem on unpickling."""
        self.__dict__.update(state)
        # driver fs not needed on workers; worker fs built lazily in write()
        self.filesystem = None
        self._worker_fs = None

    @property
    def supports_distributed_writes(self) -> bool:
        """If False, only launch write tasks on the driver's node."""
        if _is_local_scheme(self.table_uri):
            return False
        u = self.table_uri.lower()
        if u.startswith("file://") or "://" not in self.table_uri:
            return False
        return True

    def get_name(self) -> str:
        return "Delta"

    def _validate_mode(self, mode: Any) -> SaveMode:
        """Validate and normalize write mode to SaveMode enum."""
        if isinstance(mode, SaveMode):
            return mode
        if isinstance(mode, str):
            m = mode.lower()
            mp = {
                "append": SaveMode.APPEND,
                "overwrite": SaveMode.OVERWRITE,
                "upsert": SaveMode.UPSERT,
                "error": SaveMode.ERROR,
                "ignore": SaveMode.IGNORE,
            }
            if m not in mp:
                raise ValueError(f"Invalid mode '{mode}'. Supported: {list(mp.keys())}")
            return mp[m]
        raise ValueError(f"Invalid mode type: {type(mode).__name__}")

    def _upsert_cols(self) -> List[str]:
        """Get join columns for upsert operations."""
        return self._upsert_kwargs.get(UPSERT_JOIN_COLS, [])

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        """Initialize table for writing and validate constraints."""
        if schema is not None and self.schema is None:
            self.schema = schema

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing is not None

        # Check ERROR mode BEFORE any table modifications
        if self.mode == SaveMode.ERROR and existing:
            raise ValueError(
                f"Delta table already exists at {self.table_uri}. Use APPEND or OVERWRITE."
            )

        if existing:
            if not self.partition_cols:
                self.partition_cols = existing.metadata().partition_columns or []
            else:
                validate_partition_columns_match_existing(existing, self.partition_cols)

            if self.schema is not None:
                existing_schema = existing_table_pyarrow_schema(existing)
                new_fields = validate_and_plan_evolution(
                    self._schema_policy, existing_schema, self.schema
                )
                if new_fields:
                    evolve_schema(existing, new_fields)

        self._skip_write = self.mode == SaveMode.IGNORE and existing is not None

        if self.mode == SaveMode.UPSERT:
            if not existing:
                raise ValueError(
                    "UPSERT requires an existing Delta table. Create it first with APPEND."
                )
            if not self._upsert_cols():
                raise ValueError(
                    "UPSERT requires join_cols in upsert_kwargs, e.g. {'join_cols': ['id']}"
                )

            logger.warning("UPSERT is NOT fully atomic (delete then append).")

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> DeltaWriteResult:
        """Phase 1: Write Parquet files and return metadata for commit."""
        if self._skip_write:
            return DeltaWriteResult()

        # lazily build per-worker filesystem once (streaming-safe, avoids per-block rebuild)
        if self._worker_fs is None:
            self._worker_fs = worker_filesystem(self._fs_config)

        ctx_kwargs = getattr(ctx, "kwargs", None) or {}
        write_uuid = ctx_kwargs.get(WRITE_UUID_KWARG_NAME)

        all_actions = []
        upsert_keys_tables = []
        block_schemas = []
        written_files: Set[str] = set()

        use_upsert = self.mode == SaveMode.UPSERT
        upsert_cols = self._upsert_cols()

        # Create writer ONCE per task (perf win)
        writer = DeltaFileWriter(
            filesystem=self._worker_fs,
            partition_cols=self.partition_cols,
            write_uuid=write_uuid,
            write_kwargs=self.write_kwargs,
            written_files=written_files,
            target_file_size_bytes=self._target_file_size_bytes,
        )

        try:
            for block_idx, block in enumerate(blocks):
                t = BlockAccessor.for_block(block).to_arrow()
                if t.num_rows == 0:
                    continue

                validate_partition_columns_in_table(self.partition_cols, t)
                self._validate_block_against_declared_schema(t)
                block_schemas.append(t.schema)

                if use_upsert and upsert_cols:
                    missing = [c for c in upsert_cols if c not in t.column_names]
                    if missing:
                        raise ValueError(
                            f"UPSERT join columns not found: {missing}. Available: {t.column_names}"
                        )
                    upsert_keys_tables.append(t.select(upsert_cols))

                # Buffered path (reduces small files); falls back to immediate writes if target not set
                all_actions.extend(writer.add_table(t, ctx.task_idx))

            # Flush remaining buffers at end of task
            all_actions.extend(writer.flush(ctx.task_idx))

            upsert_keys = None
            if upsert_keys_tables:
                upsert_keys = concat(upsert_keys_tables)

            return DeltaWriteResult(
                add_actions=all_actions,
                upsert_keys=upsert_keys,
                schemas=block_schemas,
                written_files=list(written_files),
                write_uuid=write_uuid,
            )
        except Exception as e:
            e._delta_written_files = list(written_files)
            self._cleanup_files_worker(list(written_files))
            raise

    def on_write_complete(self, write_result: WriteResult[DeltaWriteResult]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        (
            actions,
            upsert_keys,
            schemas,
            written_files,
            write_uuid,
        ) = self._collect(write_result)
        existing = try_get_deltatable(self.table_uri, self.storage_options)
        existed_before = existing is not None

        existing = self._handle_races(existing, written_files)
        if (
            self.mode == SaveMode.IGNORE
            and (not self._table_existed_at_start)
            and existed_before
            and existing is None
        ):
            return

        if not actions:
            self._handle_empty(existing, write_uuid)
            return

        # Build commit_properties for THIS commit only (do not mutate self.write_kwargs)
        commit_props = _build_commit_properties(self.write_kwargs, write_uuid)

        # Pass a shallow-copied write_kwargs with commit_properties injected
        # so committer/upsert sees it, without polluting the instance state.
        write_kwargs_for_commit = dict(self.write_kwargs)
        write_kwargs_for_commit["commit_properties"] = commit_props

        # reconcile schema (driver-side) before committing
        existing_schema = existing_table_pyarrow_schema(existing) if existing else None
        reconciled = reconcile_worker_schemas(schemas, existing_schema)
        if reconciled is not None:
            self.schema = reconciled

        validate_file_actions(actions, self._driver_fs())
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=self.mode.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=write_kwargs_for_commit,
        )

        try:
            if self._table_existed_at_start:
                if self.mode == SaveMode.UPSERT:
                    commit_upsert(
                        existing,
                        actions,
                        upsert_keys,
                        self._upsert_cols(),
                        self.partition_cols,
                        write_kwargs_for_commit,
                    )
                else:
                    commit_to_existing_table(
                        inputs, existing, actions, self.schema, self._driver_fs()
                    )
            else:
                create_table_with_files(inputs, actions, self.schema, self._driver_fs())
        except Exception as e:
            e._delta_written_files = written_files
            logger.warning(
                f"Delta commit failed for table {self.table_uri}. Files not cleaned up to avoid deleting committed data."
            )
            raise

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure - attempt cleanup of orphaned files."""
        files = getattr(error, "_delta_written_files", []) or []
        if not files:
            logger.error(
                f"Delta write failed for {self.table_uri}: {error}. Could not determine files to cleanup."
            )
            return
        logger.warning(
            f"Delta write failed for {self.table_uri}. Cleaning up {len(files)} orphaned files."
        )
        self._cleanup_files_driver(files)

    def _driver_fs(self) -> pa_fs.FileSystem:
        """Get driver filesystem (recreate if needed)."""
        if self.filesystem is None:
            _, fs = make_fs_config(self.table_uri, None, self.storage_options)
            self.filesystem = fs
        return self.filesystem

    def _validate_block_against_declared_schema(self, table: pa.Table) -> None:
        """Validate table schema matches expected schema if provided."""
        if not self.schema:
            return
        table_cols = set(table.column_names)
        missing = set(self.schema.names) - table_cols
        if missing:
            raise ValueError(
                f"Missing columns: {sorted(missing)}. Table has: {sorted(table_cols)}"
            )

        for f in self.schema:
            if f.name in table_cols and f.name not in self.partition_cols:
                col = table[f.name]
                if f.nullable and pa.types.is_null(col.type):
                    if pc.all(pa.compute.is_null(col)).as_py():
                        continue
                from ray.data._internal.datasource.delta.utils import (
                    types_compatible,
                )

                if not types_compatible(f.type, col.type):
                    raise ValueError(
                        f"Type mismatch for '{f.name}': expected {f.type}, got {col.type}"
                    )

    def _collect(
        self, wr: WriteResult[DeltaWriteResult]
    ) -> tuple[
        List["AddAction"], Optional[pa.Table], List[pa.Schema], List[str], Optional[str]
    ]:
        """Collect all results from distributed write tasks."""
        if not wr.write_returns:
            return [], None, [], [], None

        actions = []
        schemas = []
        files = []
        upsert_tables = []
        write_uuid = None

        seen = set()
        for r in wr.write_returns:
            if r is None:
                continue
            if not isinstance(r, DeltaWriteResult):
                raise ValueError(
                    f"Invalid write return: expected DeltaWriteResult, got {type(r).__name__}"
                )
            for a in r.add_actions:
                if a.path in seen:
                    raise ValueError(f"Duplicate file paths detected: {a.path}")
                seen.add(a.path)
                actions.append(a)
            schemas.extend(r.schemas or [])
            files.extend(r.written_files or [])
            if r.upsert_keys is not None:
                upsert_tables.append(r.upsert_keys)
            write_uuid = write_uuid or r.write_uuid

        upsert_keys = concat(upsert_tables) if upsert_tables else None
        return actions, upsert_keys, schemas, files, write_uuid

    def _handle_races(
        self, existing, written_files: List[str]
    ) -> Optional["DeltaTable"]:
        """Validate race conditions and raise errors or cleanup as needed.

        Handles concurrent table creation/deletion scenarios during write operations.
        This ensures ACID guarantees by detecting and handling race conditions where
        the table state changes between write start and commit.

        Args:
            existing: Current DeltaTable instance (may be None if table doesn't exist).
            written_files: List of files written by workers (for cleanup on error).

        Returns:
            DeltaTable instance to use for commit, or None if table should be created.

        Raises:
            ValueError: If race condition is detected and cannot be handled gracefully.
        """
        if (
            not self._table_existed_at_start
            and existing is not None
            and self.mode == SaveMode.ERROR
        ):
            self._cleanup_files_driver(written_files)
            raise ValueError(
                f"Race condition: table was created at {self.table_uri} after write started."
            )

        if (
            not self._table_existed_at_start
            and existing is not None
            and self.mode == SaveMode.IGNORE
        ):
            self._cleanup_files_driver(written_files)
            return None

        if (
            self._table_existed_at_start
            and existing is None
            and self.mode in (SaveMode.APPEND, SaveMode.UPSERT, SaveMode.OVERWRITE)
        ):
            if self.mode == SaveMode.OVERWRITE:
                # For OVERWRITE, table deletion is expected - create new table
                # Don't cleanup files - they're needed to create the new table
                self._table_existed_at_start = False
                return None
            # For APPEND/UPSERT, table deletion is unexpected - cleanup and raise error
            self._cleanup_files_driver(written_files)
            raise ValueError(
                f"Delta table was deleted at {self.table_uri} after write started. Use OVERWRITE."
            )

        if (
            not self._table_existed_at_start
            and existing is not None
            and self.mode in (SaveMode.APPEND, SaveMode.OVERWRITE)
        ):
            validate_partition_columns_match_existing(existing, self.partition_cols)
            self._table_existed_at_start = True
            return existing

        return existing

    def _handle_empty(self, existing, write_uuid: Optional[str]):
        """Handle empty writes (no files written)."""
        # Build commit_properties for idempotency (same as non-empty writes)
        commit_props = _build_commit_properties(self.write_kwargs, write_uuid)
        write_kwargs_for_commit = dict(self.write_kwargs)
        write_kwargs_for_commit["commit_properties"] = commit_props

        if (
            self._table_existed_at_start
            and existing
            and self.mode == SaveMode.OVERWRITE
        ):
            inputs = CommitInputs(
                self.table_uri,
                self.mode.value,
                self.partition_cols,
                self.storage_options,
                write_kwargs_for_commit,
            )
            commit_to_existing_table(
                inputs, existing, [], self.schema, self._driver_fs()
            )
            return

        if self.schema and not existing and (not self._table_existed_at_start):
            inputs = CommitInputs(
                self.table_uri,
                self.mode.value,
                self.partition_cols,
                self.storage_options,
                write_kwargs_for_commit,
            )
            create_table_with_files(inputs, [], self.schema, self._driver_fs())

    def _cleanup_files_driver(self, file_paths: List[str]) -> None:
        """Clean up files on driver."""
        fs = self._driver_fs()
        for p in file_paths:
            try:
                info = get_file_info_with_retry(fs, p)
                if info.type != pa_fs.FileType.NotFound:
                    fs.delete_file(p)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {p}: {e}")

    def _cleanup_files_worker(self, file_paths: List[str]) -> None:
        """Clean up files on worker (best-effort)."""
        fs = self._worker_fs
        if fs is None:
            return
        for p in file_paths:
            try:
                info = get_file_info_with_retry(fs, p)
                if info.type != pa_fs.FileType.NotFound:
                    fs.delete_file(p)
            except Exception:
                pass
