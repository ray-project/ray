"""Delta Lake adapter (MVP, APPEND only).

This MVP build supports only ``SaveMode.APPEND``. Subsequent PRs extend the
adapter:

* PR 4 adds OVERWRITE / ERROR / IGNORE modes and the race-condition handler.
* PR 5 adds partition_cols + dynamic partition overwrite + buffered writes.
* PR 6 adds schema_mode="merge" schema evolution.
* PR 7 adds cloud storage_options auto-detection, orphan-file cleanup, and
  pickling robustness for distributed writes.

Delta Lake: https://delta.io/
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

from ray._common.retry import call_with_retry
from ray.data._internal.datasource.delta.committer import (
    CommitInputs,
    commit_to_existing_table,
    create_table_with_files,
    validate_file_actions,
    validate_partition_columns_match_existing,
)
from ray.data._internal.datasource.delta.fs import make_fs_config, worker_filesystem
from ray.data._internal.datasource.delta.schema import (
    SchemaPolicy,
    evolve_schema,
    existing_table_pyarrow_schema,
    reconcile_worker_schemas,
    validate_and_plan_evolution,
)
from ray.data._internal.datasource.delta.utils import (
    create_app_transaction_id,
    create_filesystem_from_storage_options,
    get_file_info_with_retry,
    get_storage_options,
    normalize_commit_properties,
    resolve_under_table_root,
    try_get_deltatable,
    validate_partition_column_names,
    validate_partition_columns_in_table,
)
from ray.data._internal.datasource.delta.writer import DeltaFileWriter
from ray.data._internal.datasource.table import (
    SaveMode,
    TableAdapter,
    _extract_retry_overrides,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.util import _check_import, _is_local_scheme

if TYPE_CHECKING:
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


class DeltaAdapter(TableAdapter["AddAction", str]):
    """``TableAdapter`` for Delta Lake (MVP, APPEND-only)."""

    def __init__(
        self,
        path: str,
        *,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        schema_mode: str = "error",
        **write_kwargs,
    ):
        _check_import(self, module="deltalake", package="deltalake")

        self.table_uri = path
        self.partition_cols = validate_partition_column_names(
            list(partition_cols or [])
        )
        self.schema = schema
        self.write_kwargs = dict(write_kwargs)
        self._schema_policy = SchemaPolicy(mode=schema_mode.lower())

        # Driver-side retry config. The shared helper drains the three
        # recognised override keys from ``write_kwargs`` so they don't leak
        # through to delta-rs (which would raise on unknown kwargs).
        # Resolution chain (in _resolved_retry_config):
        #   per-call > DeltaConfig (if explicitly set) > TableWriteConfig.
        (
            self._commit_retry_max_attempts,
            self._commit_retry_max_backoff_s,
            self._commit_retried_errors,
        ) = _extract_retry_overrides(self.write_kwargs)

        target = write_kwargs.get("target_file_size_bytes")
        if target is not None and target <= 0:
            raise ValueError("target_file_size_bytes must be > 0")
        self._target_file_size_bytes: Optional[int] = target

        self.storage_options = get_storage_options(
            self.table_uri, write_kwargs.get("storage_options")
        )
        self._fs_config, self.filesystem = make_fs_config(
            self.table_uri, filesystem, self.storage_options
        )
        self._local_filesystem_root = self._fs_config.local_filesystem_root

        # Driver-side state.
        self._mode: Optional[SaveMode] = None
        self._table_existed_at_start: bool = False
        self._skip_write: bool = False
        self._aggregated_write_uuid: Optional[str] = None

        # Worker-side state.
        self._worker_fs: Optional[pa_fs.FileSystem] = None
        self._writer: Optional[DeltaFileWriter] = None
        self._task_write_uuid: Optional[str] = None
        self._task_idx: int = 0
        self._task_written_files: Set[str] = set()

    # ------------------------------------------------------------------
    # Pickling.
    # ------------------------------------------------------------------
    def __getstate__(self) -> dict:
        d = self.__dict__.copy()
        d.pop("filesystem", None)
        d.pop("_worker_fs", None)
        d.pop("_writer", None)
        return d

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self.filesystem = None
        self._worker_fs = None
        self._writer = None

    # ------------------------------------------------------------------
    # Introspection.
    # ------------------------------------------------------------------
    @property
    def supported_modes(self) -> Set[SaveMode]:
        return {
            SaveMode.APPEND,
            SaveMode.OVERWRITE,
            SaveMode.ERROR,
            SaveMode.IGNORE,
        }

    # NOTE: this adapter intentionally does NOT implement the
    # ``SupportsUpserts`` Protocol — UPSERT is out of scope for the current
    # delivery train. The framework refuses ``mode=SaveMode.UPSERT`` at
    # ``TableDatasink`` construction time via ``isinstance(...,
    # SupportsUpserts)``.

    @property
    def supports_distributed_writes(self) -> bool:
        if _is_local_scheme(self.table_uri):
            return False
        u = self.table_uri.lower()
        if u.startswith("file://") or "://" not in self.table_uri:
            return False
        return True

    def get_name(self) -> str:
        return "Delta"

    def path_for_action(self, action: "AddAction") -> str:
        # deltalake's ``AddAction`` exposes the relative path as ``.path``.
        return action.path

    # ------------------------------------------------------------------
    # Driver lifecycle.
    # ------------------------------------------------------------------
    def preflight(
        self,
        mode: SaveMode,
        partition_cols: List[str],
        declared_schema: Optional[pa.Schema],
    ) -> None:
        self._mode = mode
        if declared_schema is not None and self.schema is None:
            self.schema = declared_schema

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing is not None

        if existing is not None:
            if not self.partition_cols:
                self.partition_cols = list(existing.metadata().partition_columns) or []
            else:
                validate_partition_columns_match_existing(existing, self.partition_cols)

            if self.schema is not None:
                existing_schema = existing_table_pyarrow_schema(existing)
                new_fields = validate_and_plan_evolution(
                    self._schema_policy, existing_schema, self.schema
                )
                if new_fields:
                    evolve_schema(existing, new_fields)

        if mode == SaveMode.ERROR and existing is not None:
            raise ValueError(
                f"Delta table already exists at {self.table_uri}. "
                "Use APPEND or OVERWRITE."
            )

        self._skip_write = mode == SaveMode.IGNORE and existing is not None

    def on_write_start(
        self, schema_from_first_bundle: Optional[pa.Schema] = None
    ) -> None:
        if schema_from_first_bundle is not None and self.schema is None:
            self.schema = schema_from_first_bundle

    # ------------------------------------------------------------------
    # Worker lifecycle.
    # ------------------------------------------------------------------
    def start_task(self, ctx: TaskContext) -> None:
        if self._skip_write:
            return
        if self._worker_fs is None:
            self._worker_fs = worker_filesystem(self._fs_config)
        if self._local_filesystem_root:
            os.makedirs(self._local_filesystem_root, exist_ok=True)
        ctx_kwargs = getattr(ctx, "kwargs", None) or {}
        self._task_write_uuid = ctx_kwargs.get(WRITE_UUID_KWARG_NAME)
        self._task_idx = int(getattr(ctx, "task_idx", 0) or 0)
        self._task_written_files = set()
        self._writer = DeltaFileWriter(
            filesystem=self._worker_fs,
            partition_cols=self.partition_cols,
            write_uuid=self._task_write_uuid,
            write_kwargs=self.write_kwargs,
            written_files=self._task_written_files,
            target_file_size_bytes=self._target_file_size_bytes,
            local_filesystem_root=self._local_filesystem_root,
        )

    def write_block(
        self, arrow_table: pa.Table
    ) -> Tuple[List["AddAction"], pa.Schema, Optional[pa.Table]]:
        if self._skip_write or self._writer is None:
            return ([], arrow_table.schema, None)
        try:
            validate_partition_columns_in_table(self.partition_cols, arrow_table)
            self._validate_block_against_declared_schema(arrow_table)
            actions = self._writer.add_table(arrow_table, self._task_idx)
        except Exception as e:
            paths = list(self._task_written_files)
            # Dynamic attribute the framework reads in on_write_failed to drive
            # orphan-file cleanup.
            e._table_written_paths = paths
            self._cleanup_files_worker(paths)
            raise
        return (actions, arrow_table.schema, None)

    def _validate_block_against_declared_schema(self, table: pa.Table) -> None:
        """Raise if the incoming block is missing a declared schema column or
        contains a type-incompatible value for an existing column.

        Partition columns are exempt (they are stripped from the on-disk
        payload by the writer). All-null columns against a nullable declared
        type are also accepted.
        """
        schema = self.schema
        if not schema:
            return
        from ray.data._internal.datasource.delta.utils import types_compatible

        table_cols: Set[str] = set(table.column_names)
        missing: Set[str] = set(schema.names) - table_cols
        if missing:
            raise ValueError(
                f"Missing columns: {sorted(missing)}. "
                f"Table has: {sorted(table_cols)}"
            )

        for f in schema:
            if f.name in table_cols and f.name not in self.partition_cols:
                col = table[f.name]
                if f.nullable and pa.types.is_null(col.type):
                    if pc.all(pc.is_null(col)).as_py():
                        continue
                if not types_compatible(f.type, col.type):
                    raise ValueError(
                        f"Type mismatch for '{f.name}': "
                        f"expected {f.type}, got {col.type}"
                    )

    def finalize_task(self) -> Tuple[List["AddAction"], List[pa.Schema]]:
        if self._skip_write or self._writer is None:
            return ([], [])
        try:
            tail = self._writer.flush(self._task_idx)
        except Exception as e:
            paths = list(self._task_written_files)
            e._table_written_paths = paths
            self._cleanup_files_worker(paths)
            raise
        return (tail, [])

    def task_metadata(self) -> Dict[str, Any]:
        return {"write_uuid": self._task_write_uuid} if self._task_write_uuid else {}

    def gather_task_metadata(self, task_metadata: List[Dict[str, Any]]) -> None:
        for md in task_metadata:
            uuid_val = md.get("write_uuid")
            if uuid_val:
                self._aggregated_write_uuid = uuid_val
                return

    # ------------------------------------------------------------------
    # Driver finalization.
    # ------------------------------------------------------------------
    def reconcile_schema(self, unified_schema: Optional[pa.Schema]) -> None:
        """Driver-side schema reconciliation.

        The framework hands us the already-promoted union of every worker's
        schema. We additionally fold in the existing table's schema (if any)
        via ``reconcile_worker_schemas`` so type promotions across workers +
        table are handled consistently.
        """
        if unified_schema is None:
            return
        existing = try_get_deltatable(self.table_uri, self.storage_options)
        existing_schema = existing_table_pyarrow_schema(existing) if existing else None
        merged = reconcile_worker_schemas([unified_schema], existing_schema)
        if merged is not None:
            self.schema = merged

    def commit_append(
        self,
        file_actions: List["AddAction"],
        unified_schema: Optional[pa.Schema],
    ) -> None:
        # IGNORE against an existing table: the framework still ran the write
        # tasks, so workers wrote data files we are NOT going to commit. Delete
        # them best-effort so they don't leak as orphans (they are unreferenced
        # by the Delta log, hence invisible to readers but wasting storage).
        # Ideally the framework would skip launching the tasks entirely, but it
        # has no skip-before-write seam yet; until then the adapter cleans up
        # its own skipped writes.
        if self._skip_write:
            if file_actions:
                self._cleanup_files_driver(
                    [self.path_for_action(a) for a in file_actions]
                )
            return

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        # Race: ERROR mode passed preflight because no table existed then, but a
        # concurrent writer created one before our commit. Refuse to silently
        # append to it (ERROR semantics: the table must not already exist).
        if self._mode == SaveMode.ERROR and existing is not None:
            raise ValueError(
                f"Delta table at {self.table_uri} was created by a concurrent "
                f"writer after this mode='error' write started; refusing to "
                f"append. Re-run with mode='append' to add to the existing table."
            )
        write_kwargs_for_commit = self._build_commit_kwargs()
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=SaveMode.APPEND.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=write_kwargs_for_commit,
            local_filesystem_root=self._local_filesystem_root,
        )

        desc_commit = f"commit to Delta table at {self.table_uri}"
        desc_create = f"create Delta table at {self.table_uri}"

        if not file_actions:
            # Empty APPEND: create empty table if needed; otherwise no-op.
            if not self._table_existed_at_start and self.schema is not None:
                self._with_retry(
                    lambda: create_table_with_files(
                        inputs, [], self.schema, self._driver_fs()
                    ),
                    description=desc_create,
                )
            return

        validate_file_actions(
            file_actions, self._driver_fs(), self._local_filesystem_root
        )
        if existing:
            self._with_retry(
                lambda: commit_to_existing_table(
                    inputs, existing, file_actions, self.schema, self._driver_fs()
                ),
                description=desc_commit,
            )
        else:
            self._with_retry(
                lambda: create_table_with_files(
                    inputs, file_actions, self.schema, self._driver_fs()
                ),
                description=desc_create,
            )

    # ------------------------------------------------------------------
    # OVERWRITE. Static overwrite: delete all rows in the existing table,
    # then append. Dynamic partition overwrite arrives in PR 5.
    # ------------------------------------------------------------------
    def build_overwrite_predicate(
        self, overwrite_filter: Optional[Any]
    ) -> Optional[str]:
        # Static overwrite: the committer handles the delete via
        # ``table.delete()`` (no predicate needed); dynamic partition
        # overwrite (PR 5) will return a per-partition SQL predicate here.
        return None

    def commit_overwrite(
        self,
        file_actions: List["AddAction"],
        unified_schema: Optional[pa.Schema],
        delete_predicate: Optional[str],
    ) -> None:
        if self._skip_write:
            return

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        write_kwargs_for_commit = self._build_commit_kwargs()
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=SaveMode.OVERWRITE.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=write_kwargs_for_commit,
            local_filesystem_root=self._local_filesystem_root,
        )

        desc_commit = f"commit to Delta table at {self.table_uri}"
        desc_create = f"create Delta table at {self.table_uri}"

        if not file_actions:
            # OVERWRITE with no data: truncate but keep schema if table
            # exists, else create empty table.
            if self._table_existed_at_start and existing is not None:
                self._with_retry(
                    lambda: commit_to_existing_table(
                        inputs, existing, [], self.schema, self._driver_fs()
                    ),
                    description=desc_commit,
                )
                return
            if not self._table_existed_at_start and self.schema is not None:
                self._with_retry(
                    lambda: create_table_with_files(
                        inputs, [], self.schema, self._driver_fs()
                    ),
                    description=desc_create,
                )
            return

        validate_file_actions(
            file_actions, self._driver_fs(), self._local_filesystem_root
        )
        if existing:
            # Either the table existed at start, or it appeared between
            # preflight and commit; in both cases OVERWRITE delete+append
            # is the correct outcome.
            self._with_retry(
                lambda: commit_to_existing_table(
                    inputs, existing, file_actions, self.schema, self._driver_fs()
                ),
                description=desc_commit,
            )
        else:
            self._with_retry(
                lambda: create_table_with_files(
                    inputs, file_actions, self.schema, self._driver_fs()
                ),
                description=desc_create,
            )

    def on_failure(self, written_paths: List[str]) -> None:
        if not written_paths:
            logger.error(
                "Delta write failed for %s. Could not determine files to clean up.",
                self.table_uri,
            )
            return
        logger.warning(
            "Delta write failed for %s. Cleaning up %d orphaned files.",
            self.table_uri,
            len(written_paths),
        )
        self._cleanup_files_driver(written_paths)

    # ------------------------------------------------------------------
    # Helpers.
    # ------------------------------------------------------------------
    def _resolved_retry_config(self) -> Tuple[int, int, List[str]]:
        """Resolve (max_attempts, max_backoff_s, retried_errors).

        Three-level precedence chain:
          1. per-call kwargs (extracted from ``**write_kwargs`` at __init__)
          2. ``DataContext.delta_config.commit_*`` (if explicitly set)
          3. ``DataContext.table_write_config`` (the shared cross-format default)

        ``DataContext`` is consulted lazily so adapters constructed before a
        context exists still work in tests.
        """
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        delta_cfg = ctx.delta_config
        shared = ctx.table_write_config

        max_attempts = (
            self._commit_retry_max_attempts
            if self._commit_retry_max_attempts is not None
            else delta_cfg.commit_max_attempts
            if delta_cfg.commit_max_attempts is not None
            else shared.max_attempts
        )
        max_backoff_s = (
            self._commit_retry_max_backoff_s
            if self._commit_retry_max_backoff_s is not None
            else delta_cfg.commit_retry_max_backoff_s
            if delta_cfg.commit_retry_max_backoff_s is not None
            else shared.max_backoff_s
        )
        retried_errors = (
            self._commit_retried_errors
            if self._commit_retried_errors is not None
            else delta_cfg.commit_retried_errors
            if delta_cfg.commit_retried_errors is not None
            else shared.retried_errors
        )
        return max_attempts, max_backoff_s, list(retried_errors)

    def _with_retry(self, func: Callable, description: str) -> Any:
        """Driver-side retry wrapper mirroring ``IcebergAdapter._with_retry``.

        Targets transient I/O / HTTP errors during the commit metadata
        write. Concurrency-conflict retries are still handled inside
        delta-rs via ``CommitProperties.max_commit_retries`` plumbed
        through ``_build_commit_kwargs``.
        """
        max_attempts, max_backoff_s, retried_errors = self._resolved_retry_config()
        return call_with_retry(
            func,
            description=description,
            match=retried_errors,
            max_attempts=max_attempts,
            max_backoff_s=max_backoff_s,
        )

    def _driver_fs(self) -> pa_fs.FileSystem:
        if self.filesystem is None:
            cloud_fs = create_filesystem_from_storage_options(
                self.table_uri, self.storage_options
            )
            if cloud_fs is not None:
                self.filesystem = cloud_fs
            else:
                _, fs = make_fs_config(self.table_uri, None, self.storage_options)
                self.filesystem = fs
        return self.filesystem

    def _build_commit_kwargs(self) -> Dict[str, Any]:
        """Return write_kwargs augmented with idempotent CommitProperties."""
        from deltalake.transaction import CommitProperties

        existing = normalize_commit_properties(
            self.write_kwargs.get("commit_properties")
        )
        max_retries = self.write_kwargs.get("max_commit_retries")
        app_txn = (
            create_app_transaction_id(self._aggregated_write_uuid)
            if self._aggregated_write_uuid
            else None
        )
        if existing is None:
            commit_props = CommitProperties(
                custom_metadata=None,
                max_commit_retries=max_retries,
                app_transactions=[app_txn] if app_txn else None,
            )
        else:
            txns = list(existing.app_transactions or [])
            if app_txn:
                key = (app_txn.app_id, app_txn.version)
                if all((t.app_id, t.version) != key for t in txns):
                    txns.append(app_txn)
            commit_props = CommitProperties(
                custom_metadata=existing.custom_metadata,
                max_commit_retries=(
                    max_retries
                    if max_retries is not None
                    else existing.max_commit_retries
                ),
                app_transactions=txns or None,
            )
        result = dict(self.write_kwargs)
        result["commit_properties"] = commit_props
        return result

    def _cleanup_files_driver(self, file_paths: List[str]) -> None:
        fs = self._driver_fs()
        for p in file_paths:
            try:
                phys = resolve_under_table_root(self._local_filesystem_root, p)
                info = get_file_info_with_retry(fs, phys)
                if info.type != pa_fs.FileType.NotFound:
                    fs.delete_file(phys)
            except Exception as e:
                logger.warning("Failed to cleanup file %s: %s", p, e)

    def _cleanup_files_worker(self, file_paths: List[str]) -> None:
        fs = self._worker_fs
        if fs is None:
            return
        for p in file_paths:
            try:
                phys = resolve_under_table_root(self._local_filesystem_root, p)
                info = get_file_info_with_retry(fs, phys)
                if info.type != pa_fs.FileType.NotFound:
                    fs.delete_file(phys)
            except Exception:
                pass
