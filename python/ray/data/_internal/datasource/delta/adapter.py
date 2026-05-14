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
    reconcile_worker_schemas,
    validate_and_plan_evolution,
)
from ray.data._internal.datasource.delta.utils import (
    get_storage_options,
    try_get_deltatable,
    validate_partition_column_names,
    validate_partition_columns_in_table,
)
from ray.data._internal.datasource.delta.writer import DeltaFileWriter
from ray.data._internal.datasource.table import (
    SaveMode,
    TableAdapter,
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
        validate_partition_columns_in_table(self.partition_cols, arrow_table)
        self._validate_block_against_declared_schema(arrow_table)
        actions = self._writer.add_table(arrow_table, self._task_idx)
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
        tail = self._writer.flush(self._task_idx)
        return (tail, [])

    def task_metadata(self) -> Dict[str, Any]:
        return {}

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
        # IGNORE against an existing table: framework still calls commit_append
        # but ``_skip_write`` was set in preflight, so no-op here.
        if self._skip_write:
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
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=SaveMode.APPEND.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=dict(self.write_kwargs),
            local_filesystem_root=self._local_filesystem_root,
        )

        if not file_actions:
            # Empty write: create empty table if needed.
            if not self._table_existed_at_start and self.schema is not None:
                create_table_with_files(inputs, [], self.schema, self._driver_fs())
            return

        validate_file_actions(
            file_actions, self._driver_fs(), self._local_filesystem_root
        )
        if existing:
            commit_to_existing_table(
                inputs, existing, file_actions, self.schema, self._driver_fs()
            )
        else:
            create_table_with_files(
                inputs, file_actions, self.schema, self._driver_fs()
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
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=SaveMode.OVERWRITE.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=dict(self.write_kwargs),
            local_filesystem_root=self._local_filesystem_root,
        )

        if not file_actions:
            # OVERWRITE with no data: truncate but keep schema if table
            # exists, else create empty table.
            if self._table_existed_at_start and existing is not None:
                commit_to_existing_table(
                    inputs, existing, [], self.schema, self._driver_fs()
                )
                return
            if not self._table_existed_at_start and self.schema is not None:
                create_table_with_files(inputs, [], self.schema, self._driver_fs())
            return

        validate_file_actions(
            file_actions, self._driver_fs(), self._local_filesystem_root
        )
        if existing:
            # Either the table existed at start, or it appeared between
            # preflight and commit; in both cases OVERWRITE delete+append
            # is the correct outcome.
            commit_to_existing_table(
                inputs, existing, file_actions, self.schema, self._driver_fs()
            )
        else:
            create_table_with_files(
                inputs, file_actions, self.schema, self._driver_fs()
            )

    # ------------------------------------------------------------------
    # Helpers.
    # ------------------------------------------------------------------
    def _driver_fs(self) -> pa_fs.FileSystem:
        if self.filesystem is None:
            _, fs = make_fs_config(self.table_uri, None, self.storage_options)
            self.filesystem = fs
        return self.filesystem
