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
)
from .fs import make_fs_config, worker_filesystem
from .writer import DeltaFileWriter
from ray.data._internal.datasource.delta.utils import (
    DeltaWriteResult,
    create_app_transaction_id,
    get_file_info_with_retry,
    get_storage_options,
    normalize_commit_properties,
    to_pyarrow_schema,
    try_get_deltatable,
    validate_schema_type_compatibility,
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
    PR 1: Only APPEND mode is supported.

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
        **write_kwargs,
    ):
        """Initialize DeltaDatasink.

        Args:
            path: Path to Delta table (local or cloud storage).
            mode: Write mode - PR 1: Only APPEND is supported.
            partition_cols: Columns to partition by (Hive-style). PR 1: Not supported.
            filesystem: Optional PyArrow filesystem.
                PyArrow filesystems: https://arrow.apache.org/docs/python/api/filesystems.html
                Note: For distributed writes, filesystem must be reconstructible from
                storage_options. Consider passing storage_options instead.
            schema: Optional explicit schema for the table.
            **write_kwargs: Additional options passed to Delta writer:
                - compression: Compression codec (default: "snappy").
                - write_statistics: Whether to write Parquet statistics (default: True).
        """
        _check_import(self, module="deltalake", package="deltalake")

        self.table_uri = path
        self.mode = self._validate_mode(mode)
        # PR 1: No partitioning support yet
        self.partition_cols = []
        self.schema = schema
        self.write_kwargs = write_kwargs

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
        # PR 1: No file buffering support
        self._target_file_size_bytes: Optional[int] = None
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
            if mode != SaveMode.APPEND:
                raise ValueError(f"PR 1: Only APPEND mode is supported. Got {mode}")
            return mode
        if isinstance(mode, str):
            m = mode.lower()
            if m != "append":
                raise ValueError(
                    f"PR 1: Only 'append' mode is supported. Got mode='{mode}'. "
                    "Other modes will be added in PR 2."
                )
            return SaveMode.APPEND
        raise ValueError(f"Invalid mode type: {type(mode).__name__}")

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        """Initialize table for writing and validate constraints."""
        if schema is not None and self.schema is None:
            self.schema = schema

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing is not None

        # PR 1: Basic schema validation for append mode
        if existing and self.schema is not None:
            existing_schema = to_pyarrow_schema(existing.schema())
            # PR 1: Only validate compatibility, no schema evolution
            validate_schema_type_compatibility(existing_schema, self.schema)

        self._skip_write = False  # PR 1: Only append mode, no skip logic

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
        block_schemas = []
        written_files: Set[str] = set()

        # Create writer ONCE per task (perf win)
        writer = DeltaFileWriter(
            filesystem=self._worker_fs,
            partition_cols=self.partition_cols,
            write_uuid=write_uuid,
            write_kwargs=self.write_kwargs,
            written_files=written_files,
            target_file_size_bytes=None,  # PR 1: No file buffering
        )

        try:
            for block_idx, block in enumerate(blocks):
                t = BlockAccessor.for_block(block).to_arrow()
                if t.num_rows == 0:
                    continue

                # PR 1: No partitioning validation needed
                self._validate_block_against_declared_schema(t)
                block_schemas.append(t.schema)

                # PR 1: Immediate writes (no buffering)
                all_actions.extend(writer.add_table(t, ctx.task_idx))

            # PR 1: No flush needed (no buffering)

            return DeltaWriteResult(
                add_actions=all_actions,
                upsert_keys=None,  # PR 1: No upsert
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

        existing = self._handle_races(existing, written_files)

        if not actions:
            self._handle_empty(existing, write_uuid)
            return

        # Build commit_properties for THIS commit only (do not mutate self.write_kwargs)
        commit_props = _build_commit_properties(self.write_kwargs, write_uuid)

        # Pass a shallow-copied write_kwargs with commit_properties injected
        write_kwargs_for_commit = dict(self.write_kwargs)
        write_kwargs_for_commit["commit_properties"] = commit_props

        # PR 1: Basic schema reconciliation - use first schema or merge manually
        if schemas:
            # For PR 1, just use the first schema (no complex reconciliation)
            # In later PRs, this will use reconcile_worker_schemas
            if self.schema is None:
                self.schema = schemas[0]
            elif existing:
                # Validate compatibility with existing table
                existing_schema = to_pyarrow_schema(existing.schema())
                validate_schema_type_compatibility(existing_schema, self.schema)

        validate_file_actions(actions, self._driver_fs())
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=self.mode.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=write_kwargs_for_commit,
        )

        try:
            # PR 1: Only append mode - commit to existing table or create new
            if self._table_existed_at_start:
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
            write_uuid = write_uuid or r.write_uuid

        # PR 1: No upsert support
        upsert_keys = None
        return actions, upsert_keys, schemas, files, write_uuid

    def _handle_races(
        self, existing, written_files: List[str]
    ) -> Optional["DeltaTable"]:
        """Validate race conditions and raise errors or cleanup as needed.

        PR 1: Only APPEND mode is supported. Handles basic race conditions.

        Args:
            existing: Current DeltaTable instance (may be None if table doesn't exist).
            written_files: List of files written by workers (for cleanup on error).

        Returns:
            DeltaTable instance to use for commit, or None if table should be created.

        Raises:
            ValueError: If race condition is detected and cannot be handled gracefully.
        """
        # PR 1: Only APPEND mode - handle race conditions
        if self._table_existed_at_start and existing is None:
            # Table was deleted during write - cleanup and raise error
            self._cleanup_files_driver(written_files)
            raise ValueError(
                f"Delta table was deleted at {self.table_uri} after write started."
            )

        if not self._table_existed_at_start and existing is not None:
            # Table was created during write - use it
            # PR 1: No partition validation needed (no partitioning support)
            self._table_existed_at_start = True
            return existing

        return existing

    def _handle_empty(self, existing, write_uuid: Optional[str]):
        """Handle empty writes (no files written)."""
        # PR 1: Only APPEND mode - empty writes are idempotent (do nothing)
        return

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
