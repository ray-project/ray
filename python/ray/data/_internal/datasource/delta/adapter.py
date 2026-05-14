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
import pyarrow.fs as pa_fs

from ray.data._internal.datasource.delta.committer import (
    CommitInputs,
    commit_to_existing_table,
    create_table_with_files,
    validate_file_actions,
)
from ray.data._internal.datasource.delta.fs import make_fs_config, worker_filesystem
from ray.data._internal.datasource.delta.utils import (
    get_storage_options,
    try_get_deltatable,
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
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        **write_kwargs,
    ):
        _check_import(self, module="deltalake", package="deltalake")

        self.table_uri = path
        self.partition_cols: List[str] = []
        self.schema = schema
        self.write_kwargs = dict(write_kwargs)

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

        if mode == SaveMode.ERROR and existing is not None:
            raise ValueError(
                f"Delta table already exists at {self.table_uri}. "
                "Use APPEND or OVERWRITE."
            )

        self._skip_write = mode == SaveMode.IGNORE and existing is not None
        # PR 6 adds schema-evolution planning; PR 5 adds partition-column
        # validation against the existing table.

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
            write_uuid=self._task_write_uuid,
            write_kwargs=self.write_kwargs,
            written_files=self._task_written_files,
            local_filesystem_root=self._local_filesystem_root,
        )

    def write_block(
        self, arrow_table: pa.Table
    ) -> Tuple[List["AddAction"], pa.Schema, Optional[pa.Table]]:
        if self._skip_write or self._writer is None:
            return ([], arrow_table.schema, None)
        actions = self._writer.add_table(arrow_table, self._task_idx)
        return (actions, arrow_table.schema, None)

    def finalize_task(self) -> Tuple[List["AddAction"], List[pa.Schema]]:
        return ([], [])

    def task_metadata(self) -> Dict[str, Any]:
        return {}

    # ------------------------------------------------------------------
    # Driver finalization.
    # ------------------------------------------------------------------
    def reconcile_schema(self, unified_schema: Optional[pa.Schema]) -> None:
        if unified_schema is not None:
            self.schema = unified_schema

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
