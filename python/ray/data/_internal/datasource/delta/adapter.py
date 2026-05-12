"""Delta Lake adapter that plugs into ``LakehouseDatasink``.

This module is the format-specific half of the Delta Lake write path. The
generic ``LakehouseDatasink`` owns lifecycle orchestration; this adapter owns:

* loading / validating the Delta table at the configured URI,
* writing Parquet files inside each worker (via the existing
  ``DeltaFileWriter``),
* committing those files through ``deltalake``'s transaction APIs,
* handling Delta-specific race conditions and empty-write edge cases,
* cleaning up orphaned files on failure.

Delta Lake: https://delta.io/
delta-rs Python library: https://delta-io.github.io/delta-rs/python/
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

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
    validate_and_plan_evolution,
)
from ray.data._internal.datasource.delta.upsert import (
    build_delete_predicate as build_upsert_delete_predicate,
    commit_upsert,
)
from ray.data._internal.datasource.delta.utils import (
    UPSERT_JOIN_COLS,
    create_app_transaction_id,
    get_file_info_with_retry,
    get_storage_options,
    normalize_commit_properties,
    try_get_deltatable,
    validate_partition_column_names,
    validate_partition_columns_in_table,
)
from ray.data._internal.datasource.delta.writer import DeltaFileWriter
from ray.data._internal.datasource.lakehouse import (
    LakehouseAdapter,
    SaveMode,
    UpsertSemantics,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.util import _check_import, _is_local_scheme

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction, CommitProperties

logger = logging.getLogger(__name__)


# A predicate handed from build_delete_predicate to commit. For Delta it can
# be either a SQL predicate string (UPSERT or dynamic-partition OVERWRITE) or
# the sentinel below for a full OVERWRITE.
_FULL_OVERWRITE = object()


def _build_commit_properties(
    write_kwargs: Dict[str, Any], write_uuid: Optional[str]
) -> Optional["CommitProperties"]:
    """Build CommitProperties for one commit. Lifted unchanged from the
    previous DeltaDatasink so behaviour is identical."""
    from deltalake.transaction import CommitProperties

    existing = normalize_commit_properties(write_kwargs.get("commit_properties"))
    max_retries = write_kwargs.get("max_commit_retries")
    app_txn = create_app_transaction_id(write_uuid) if write_uuid else None

    if existing is None:
        return CommitProperties(
            custom_metadata=None,
            max_commit_retries=max_retries,
            app_transactions=[app_txn] if app_txn else None,
        )

    txns = list(existing.app_transactions or [])
    if app_txn:
        key = (app_txn.app_id, app_txn.version)
        seen = {(t.app_id, t.version) for t in txns}
        if key not in seen:
            txns.append(app_txn)

    return CommitProperties(
        custom_metadata=existing.custom_metadata,
        max_commit_retries=(
            max_retries if max_retries is not None else existing.max_commit_retries
        ),
        app_transactions=txns or None,
    )


class DeltaAdapter(LakehouseAdapter["AddAction"]):
    """``LakehouseAdapter`` implementation for Delta Lake.

    Holds all of the state that used to live inside ``DeltaDatasink`` plus the
    per-task ``DeltaFileWriter``. The driver-instance state (``self._table``,
    ``self._table_existed_at_start``, etc.) is populated in ``preflight``;
    workers receive a freshly pickled adapter copy and rebuild their per-task
    state in ``start_task``.
    """

    def __init__(
        self,
        path: str,
        *,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
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
        self._upsert_kwargs = dict(upsert_kwargs or {})
        self._schema_policy = SchemaPolicy(mode=schema_mode.lower())

        self.storage_options = get_storage_options(
            self.table_uri, write_kwargs.get("storage_options")
        )
        self._fs_config, self.filesystem = make_fs_config(
            self.table_uri, filesystem, self.storage_options
        )

        target = write_kwargs.get("target_file_size_bytes")
        if target is not None and target <= 0:
            raise ValueError("target_file_size_bytes must be > 0")
        self._target_file_size_bytes: Optional[int] = target

        # Driver-side state — populated in preflight.
        self._mode: Optional[SaveMode] = None
        self._table_existed_at_start: bool = False
        self._skip_write: bool = False

        # Worker-side state — populated in start_task.
        self._worker_fs: Optional[pa_fs.FileSystem] = None
        self._writer: Optional[DeltaFileWriter] = None
        self._task_write_uuid: Optional[str] = None
        self._task_written_files: Set[str] = set()
        self._task_idx: int = 0

        # Aggregated write_uuid surfaced from workers to driver via
        # task_metadata; consumed in commit() to build commit_properties.
        self._aggregated_write_uuid: Optional[str] = None

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
            SaveMode.UPSERT,
            SaveMode.ERROR,
            SaveMode.IGNORE,
        }

    @property
    def upsert_semantics(self) -> UpsertSemantics:
        return UpsertSemantics.COPY_ON_WRITE

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

    # ------------------------------------------------------------------
    # Driver lifecycle — preflight + on_write_start.
    # ------------------------------------------------------------------
    def preflight(
        self,
        mode: SaveMode,
        partition_cols: List[str],
        declared_schema: Optional[pa.Schema],
    ) -> None:
        """Load the table, validate the mode, and stage any pre-write schema
        evolution. Mirrors the original DeltaDatasink.on_write_start."""
        self._mode = mode
        if partition_cols and not self.partition_cols:
            # The framework may pass partition_cols separately from the adapter
            # constructor; honour whichever is non-empty.
            self.partition_cols = validate_partition_column_names(list(partition_cols))
        if declared_schema is not None and self.schema is None:
            self.schema = declared_schema

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing is not None

        if mode == SaveMode.ERROR and existing:
            raise ValueError(
                f"Delta table already exists at {self.table_uri}. "
                "Use APPEND or OVERWRITE."
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

        self._skip_write = mode == SaveMode.IGNORE and existing is not None

        if mode == SaveMode.UPSERT:
            if not existing:
                raise ValueError(
                    "UPSERT requires an existing Delta table. "
                    "Create it first with APPEND."
                )
            if not self._upsert_cols():
                raise ValueError(
                    "UPSERT requires join_cols in upsert_kwargs, "
                    "e.g. {'join_cols': ['id']}"
                )
            logger.warning("UPSERT is NOT fully atomic (delete then append).")

    def on_write_start(
        self, schema_from_first_bundle: Optional[pa.Schema] = None
    ) -> None:
        """Driver-side: if the framework supplied a bundle schema and the user
        did not pre-declare one, remember it so preflight-style schema checks
        (run later in commit) see it. Delta evolves at commit time, so no
        table mutation here."""
        if schema_from_first_bundle is not None and self.schema is None:
            self.schema = schema_from_first_bundle

    # ------------------------------------------------------------------
    # Worker lifecycle — start_task, write_block, finalize_task.
    # ------------------------------------------------------------------
    def start_task(self, ctx: TaskContext) -> None:
        if self._skip_write:
            return

        if self._worker_fs is None:
            self._worker_fs = worker_filesystem(self._fs_config)

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
        )

    def write_block(
        self, arrow_table: pa.Table
    ) -> Tuple[List["AddAction"], pa.Schema, Optional[pa.Table]]:
        if self._skip_write or self._writer is None:
            return ([], arrow_table.schema, None)

        validate_partition_columns_in_table(self.partition_cols, arrow_table)
        self._validate_block_against_declared_schema(arrow_table)

        upsert_keys: Optional[pa.Table] = None
        if self._mode == SaveMode.UPSERT:
            upsert_cols = self._upsert_cols()
            if upsert_cols:
                missing = [c for c in upsert_cols if c not in arrow_table.column_names]
                if missing:
                    raise ValueError(
                        f"UPSERT join columns not found: {missing}. "
                        f"Available: {arrow_table.column_names}"
                    )
                upsert_keys = arrow_table.select(upsert_cols)

        try:
            actions = self._writer.add_table(arrow_table, self._task_idx)
        except Exception as e:
            # Attach orphan-file list to the exception in the legacy attribute
            # name for callers that introspect it, plus the framework's name.
            paths = list(self._task_written_files)
            e._delta_written_files = paths  # legacy
            e._lakehouse_written_paths = paths
            self._cleanup_files_worker(paths)
            raise

        return (actions, arrow_table.schema, upsert_keys)

    def finalize_task(self) -> Tuple[List["AddAction"], List[pa.Schema]]:
        if self._skip_write or self._writer is None:
            return ([], [])
        try:
            tail = self._writer.flush(self._task_idx)
        except Exception as e:
            paths = list(self._task_written_files)
            e._delta_written_files = paths
            e._lakehouse_written_paths = paths
            self._cleanup_files_worker(paths)
            raise
        return (tail, [])

    def task_metadata(self) -> Dict[str, Any]:
        if self._task_write_uuid:
            return {"write_uuid": self._task_write_uuid}
        return {}

    # ------------------------------------------------------------------
    # Driver lifecycle — gather, reconcile, build_delete_predicate, commit.
    # ------------------------------------------------------------------
    def gather_task_metadata(self, task_metadata: List[Dict[str, Any]]) -> None:
        for md in task_metadata:
            uuid_val = md.get("write_uuid")
            if uuid_val:
                self._aggregated_write_uuid = uuid_val
                return

    def reconcile_schema(self, unified_schema: Optional[pa.Schema]) -> None:
        """For Delta, schema evolution happens inside commit. This hook records
        the unified schema (already type-promoted by the framework) so commit
        can use it as the table's authoritative schema."""
        if unified_schema is not None:
            self.schema = unified_schema

    def build_delete_predicate(
        self,
        mode: SaveMode,
        file_actions: List["AddAction"],
        upsert_keys: Optional[pa.Table],
        join_cols: List[str],
        overwrite_filter: Optional[Any],
    ) -> Optional[Any]:
        # Allow either kwargs-supplied or LakehouseDatasink-supplied join cols.
        if mode == SaveMode.UPSERT:
            cols = self._upsert_cols() or list(join_cols)
            self._stashed_upsert_keys = upsert_keys
            self._stashed_upsert_cols = cols
            if upsert_keys is None or len(upsert_keys) == 0 or not cols:
                return None
            import functools

            masks = []
            for c in cols:
                arr = upsert_keys[c]
                m = pc.is_valid(arr)
                if pa.types.is_floating(arr.type):
                    m = pc.and_(m, pc.invert(pc.is_nan(arr)))
                masks.append(m)
            keys = upsert_keys.filter(functools.reduce(pc.and_, masks))
            if len(keys) == 0:
                return None
            return build_upsert_delete_predicate(keys, cols)

        if mode == SaveMode.OVERWRITE:
            # The committer handles partition-overwrite predicate generation
            # internally based on partition_overwrite_mode. Returning the
            # sentinel keeps the contract explicit.
            return _FULL_OVERWRITE

        return None

    def commit(
        self,
        mode: SaveMode,
        file_actions: List["AddAction"],
        delete_predicate: Optional[Any],
    ) -> None:
        write_uuid = self._aggregated_write_uuid
        existing = try_get_deltatable(self.table_uri, self.storage_options)
        existed_before = existing is not None
        existing = self._handle_races(
            existing, [getattr(a, "path", "") for a in file_actions]
        )

        if (
            mode == SaveMode.IGNORE
            and not self._table_existed_at_start
            and existed_before
            and existing is None
        ):
            return

        if not file_actions:
            self._handle_empty(existing, write_uuid)
            return

        commit_props = _build_commit_properties(self.write_kwargs, write_uuid)
        write_kwargs_for_commit = dict(self.write_kwargs)
        write_kwargs_for_commit["commit_properties"] = commit_props

        validate_file_actions(file_actions, self._driver_fs())

        if mode == SaveMode.UPSERT and self._table_existed_at_start and existing:
            # Reuse the existing helper for parity with the previous Delta
            # implementation. The keys/cols were stashed by
            # build_delete_predicate so we hand them back here verbatim.
            commit_upsert(
                existing,
                file_actions,
                getattr(self, "_stashed_upsert_keys", None),
                getattr(self, "_stashed_upsert_cols", None) or self._upsert_cols(),
                self.partition_cols,
                write_kwargs_for_commit,
            )
            return

        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=mode.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=write_kwargs_for_commit,
        )

        try:
            if self._table_existed_at_start and existing:
                commit_to_existing_table(
                    inputs, existing, file_actions, self.schema, self._driver_fs()
                )
            else:
                create_table_with_files(
                    inputs, file_actions, self.schema, self._driver_fs()
                )
        except Exception as e:
            e._delta_written_files = [getattr(a, "path", "") for a in file_actions]
            e._lakehouse_written_paths = e._delta_written_files
            logger.warning(
                "Delta commit failed for table %s. Files not cleaned up to avoid "
                "deleting committed data.",
                self.table_uri,
            )
            raise

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
    # Internal helpers — lifted from the previous DeltaDatasink.
    # ------------------------------------------------------------------
    def _upsert_cols(self) -> List[str]:
        return self._upsert_kwargs.get(UPSERT_JOIN_COLS, [])

    def _driver_fs(self) -> pa_fs.FileSystem:
        if self.filesystem is None:
            _, fs = make_fs_config(self.table_uri, None, self.storage_options)
            self.filesystem = fs
        return self.filesystem

    def _validate_block_against_declared_schema(self, table: pa.Table) -> None:
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
                from ray.data._internal.datasource.delta.utils import types_compatible

                if not types_compatible(f.type, col.type):
                    raise ValueError(
                        f"Type mismatch for '{f.name}': expected {f.type}, "
                        f"got {col.type}"
                    )

    def _handle_races(
        self, existing, written_files: List[str]
    ) -> Optional["DeltaTable"]:
        mode = self._mode
        if (
            not self._table_existed_at_start
            and existing is not None
            and mode == SaveMode.ERROR
        ):
            self._cleanup_files_driver(written_files)
            raise ValueError(
                f"Race condition: table was created at {self.table_uri} "
                "after write started."
            )

        if (
            not self._table_existed_at_start
            and existing is not None
            and mode == SaveMode.IGNORE
        ):
            self._cleanup_files_driver(written_files)
            return None

        if (
            self._table_existed_at_start
            and existing is None
            and mode in (SaveMode.APPEND, SaveMode.UPSERT, SaveMode.OVERWRITE)
        ):
            if mode == SaveMode.OVERWRITE:
                self._table_existed_at_start = False
                return None
            self._cleanup_files_driver(written_files)
            raise ValueError(
                f"Delta table was deleted at {self.table_uri} after write "
                "started. Use OVERWRITE."
            )

        if (
            not self._table_existed_at_start
            and existing is not None
            and mode in (SaveMode.APPEND, SaveMode.OVERWRITE)
        ):
            validate_partition_columns_match_existing(existing, self.partition_cols)
            self._table_existed_at_start = True
            return existing

        return existing

    def _handle_empty(self, existing, write_uuid: Optional[str]) -> None:
        commit_props = _build_commit_properties(self.write_kwargs, write_uuid)
        write_kwargs_for_commit = dict(self.write_kwargs)
        write_kwargs_for_commit["commit_properties"] = commit_props

        if (
            self._table_existed_at_start
            and existing
            and self._mode == SaveMode.OVERWRITE
        ):
            inputs = CommitInputs(
                self.table_uri,
                self._mode.value,
                self.partition_cols,
                self.storage_options,
                write_kwargs_for_commit,
            )
            commit_to_existing_table(
                inputs, existing, [], self.schema, self._driver_fs()
            )
            return

        if self.schema and not existing and not self._table_existed_at_start:
            inputs = CommitInputs(
                self.table_uri,
                self._mode.value,
                self.partition_cols,
                self.storage_options,
                write_kwargs_for_commit,
            )
            create_table_with_files(inputs, [], self.schema, self._driver_fs())

    def _cleanup_files_driver(self, file_paths: List[str]) -> None:
        fs = self._driver_fs()
        for p in file_paths:
            try:
                info = get_file_info_with_retry(fs, p)
                if info.type != pa_fs.FileType.NotFound:
                    fs.delete_file(p)
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to cleanup file %s: %s", p, e)

    def _cleanup_files_worker(self, file_paths: List[str]) -> None:
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
