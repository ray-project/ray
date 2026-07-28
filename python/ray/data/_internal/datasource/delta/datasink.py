"""Delta Lake datasink.

``DeltaDatasink`` directly implements Ray Data's ``Datasink`` write lifecycle
(``on_write_start`` -> workers' ``write`` -> ``on_write_complete`` ->
``on_write_failed``) -- there is no generic multi-format adapter layer; this
is the only concrete write path Delta needs.

Supports ``SaveMode.{APPEND,OVERWRITE,ERROR,IGNORE}``. Partitioning and
schema evolution are handled inline. UPSERT is not supported.

Delta Lake: https://delta.io/
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple

import pyarrow as pa
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
    DeltaWriteResult,
    get_file_info_with_retry,
    get_storage_options,
    resolve_under_table_root,
    try_get_deltatable,
    types_compatible,
    validate_partition_column_names,
    validate_partition_columns_in_table,
)
from ray.data._internal.datasource.delta.writer import DeltaFileWriter
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import _check_import, _is_local_scheme
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

_SUPPORTED_MODES = {
    SaveMode.APPEND,
    SaveMode.OVERWRITE,
    SaveMode.ERROR,
    SaveMode.IGNORE,
}

if TYPE_CHECKING:
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


def _attach_written_paths(exc: BaseException, paths: List[str]) -> None:
    """Append ``paths`` to ``exc._table_written_paths`` for orphan cleanup.

    Used by both the worker write path and driver ``on_write_complete`` to
    carry the list of files that were written (and therefore need best-effort
    cleanup) to ``on_write_failed`` via the raised exception.

    Setting an attribute can raise ``AttributeError`` for exceptions that use
    ``__slots__`` without ``__dict__`` (some optimized / C-extension errors).
    In that case we swallow it: failing to record orphan paths must never mask
    the original error.
    """
    if not paths:
        return
    try:
        existing = getattr(exc, "_table_written_paths", None) or []
        # Dynamic attribute carried to on_write_failed for orphan cleanup;
        # BaseException has no static slot for it.
        exc._table_written_paths = list(existing) + list(paths)
    except AttributeError:
        pass


def _unify_schemas(schemas: List[pa.Schema]) -> Optional[pa.Schema]:
    """Type-promoted ``pa.unify_schemas``."""
    if not schemas:
        return None
    from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

    return unify_schemas(schemas, promote_types=True)


class DeltaDatasink(Datasink[DeltaWriteResult]):
    """Datasink for writing to Delta Lake tables."""

    def __init__(
        self,
        path: str,
        *,
        mode: Any = SaveMode.APPEND,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        schema_mode: str = "error",
        **write_kwargs,
    ):
        _check_import(self, module="deltalake", package="deltalake")

        self.table_uri = path
        self.mode = self._coerce_mode(mode)
        self.partition_cols = validate_partition_column_names(
            list(partition_cols or [])
        )
        # ``self.schema`` is live -- it may be adopted from the first input
        # bundle in ``on_write_start`` and later reconciled/promoted in
        # ``_aggregate_and_commit``. ``self._declared_schema`` is the frozen
        # user-supplied value, used only as the schema-unification fallback
        # when every block written was empty (see ``_aggregate_and_commit``).
        self.schema = schema
        self._declared_schema = schema
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
        self._skip_write: bool = False

        # Worker-side state.
        self._worker_fs: Optional[pa_fs.FileSystem] = None
        self._writer: Optional[DeltaFileWriter] = None
        self._task_write_uuid: Optional[str] = None
        self._task_idx: int = 0
        self._task_written_files: Set[str] = set()

    # ------------------------------------------------------------------
    # Mode coercion.
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_mode(mode: Any) -> SaveMode:
        if isinstance(mode, str):
            try:
                mode = SaveMode(mode.lower())
            except ValueError as e:
                raise ValueError(
                    f"Invalid mode '{mode}'. Supported: "
                    f"{sorted(m.value for m in _SUPPORTED_MODES)}"
                ) from e
        elif not isinstance(mode, SaveMode):
            raise TypeError(f"Invalid mode type: {type(mode).__name__}")
        if mode not in _SUPPORTED_MODES:
            raise ValueError(
                f"write_delta does not support mode={mode.value!r}. Supported: "
                f"{sorted(m.value for m in _SUPPORTED_MODES)}"
            )
        return mode

    # ------------------------------------------------------------------
    # Pickling -- filesystem/writer are rebuilt on each worker, not shipped.
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
    def get_name(self) -> str:
        return "Delta"

    @property
    def supports_distributed_writes(self) -> bool:
        if _is_local_scheme(self.table_uri):
            return False
        u = self.table_uri.lower()
        if u.startswith("file://") or "://" not in self.table_uri:
            return False
        return True

    @staticmethod
    def path_for_action(action: "AddAction") -> str:
        # deltalake's ``AddAction`` exposes the relative path as ``.path``.
        return action.path

    # ------------------------------------------------------------------
    # Driver lifecycle -- preflight.
    # ------------------------------------------------------------------
    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        existing = try_get_deltatable(self.table_uri, self.storage_options)
        self._table_existed_at_start = existing is not None

        if existing is not None:
            if not self.partition_cols:
                self.partition_cols = list(existing.metadata().partition_columns) or []
            else:
                validate_partition_columns_match_existing(existing, self.partition_cols)

            # Schema evolution is deferred to commit time in
            # ``_aggregate_and_commit``, where the schema is fully
            # reconciled across all workers (and available even when the
            # caller didn't pass an explicit ``schema=`` and it's inferred
            # from the data instead). Evolving here would be non-atomic
            # with the write: if the tasks below fail, the table's schema
            # would already have been permanently altered for nothing.

        if self.mode == SaveMode.ERROR and existing is not None:
            raise ValueError(
                f"Delta table already exists at {self.table_uri}. "
                "Use APPEND or OVERWRITE."
            )

        self._skip_write = self.mode == SaveMode.IGNORE and existing is not None

        # Adopt Ray Data's own inferred schema only if the user didn't
        # declare one explicitly.
        if schema is not None and self.schema is None:
            self.schema = schema

    # ------------------------------------------------------------------
    # Worker lifecycle -- write.
    # ------------------------------------------------------------------
    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> DeltaWriteResult:
        self._start_task(ctx)

        add_actions: List["AddAction"] = []
        emitted_schemas: List[pa.Schema] = []
        written_paths: List[str] = []

        try:
            for block in blocks:
                arrow_table = BlockAccessor.for_block(block).to_arrow()
                if arrow_table.num_rows == 0:
                    continue
                actions, emitted_schema = self._write_block(arrow_table)
                if actions:
                    add_actions.extend(actions)
                    for action in actions:
                        written_paths.append(self.path_for_action(action))
                if emitted_schema is not None:
                    emitted_schemas.append(emitted_schema)

            tail_actions = self._finalize_task()
            if tail_actions:
                add_actions.extend(tail_actions)
                for action in tail_actions:
                    written_paths.append(self.path_for_action(action))

            task_metadata: Dict[str, Any] = (
                {"write_uuid": self._task_write_uuid} if self._task_write_uuid else {}
            )
        except Exception as e:
            # Surface the orphan-path list to the driver via the exception so
            # ``on_write_failed`` can clean it up. Include any in-flight paths
            # the writer started but hadn't yet returned as completed actions
            # (e.g. a Parquet file registered right before ``pq.write_table``
            # raised mid-write) -- these never appear in ``written_paths`` and
            # would otherwise be orphaned.
            orphan_paths = list(set(written_paths) | self._task_written_files)
            _attach_written_paths(e, orphan_paths)
            raise

        return DeltaWriteResult(
            add_actions=add_actions,
            emitted_schemas=emitted_schemas,
            written_paths=written_paths,
            task_id=getattr(ctx, "task_idx", None),
            task_metadata=task_metadata,
        )

    def _start_task(self, ctx: TaskContext) -> None:
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

    def _write_block(
        self, arrow_table: pa.Table
    ) -> Tuple[List["AddAction"], pa.Schema]:
        if self._skip_write or self._writer is None:
            return ([], arrow_table.schema)
        try:
            validate_partition_columns_in_table(self.partition_cols, arrow_table)
            self._validate_block_against_declared_schema(arrow_table)
            actions = self._writer.add_table(arrow_table, self._task_idx)
        except Exception as e:
            paths = list(self._task_written_files)
            # Dynamic attribute the framework reads in on_write_failed to
            # drive orphan-file cleanup.
            e._table_written_paths = paths
            self._cleanup_files_worker(paths)
            raise
        return (actions, arrow_table.schema)

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
                # A pa.null()-typed column is null by construction -- no
                # need to scan it to confirm.
                if f.nullable and pa.types.is_null(col.type):
                    continue
                if not types_compatible(f.type, col.type):
                    raise ValueError(
                        f"Type mismatch for '{f.name}': "
                        f"expected {f.type}, got {col.type}"
                    )

    def _finalize_task(self) -> List["AddAction"]:
        if self._skip_write or self._writer is None:
            return []
        try:
            return self._writer.flush(self._task_idx)
        except Exception as e:
            paths = list(self._task_written_files)
            e._table_written_paths = paths
            self._cleanup_files_worker(paths)
            raise

    # ------------------------------------------------------------------
    # Driver lifecycle -- on_write_complete (aggregate, reconcile, commit).
    # ------------------------------------------------------------------
    def on_write_complete(self, write_result: WriteResult[DeltaWriteResult]) -> None:
        returns = [r for r in (write_result.write_returns or []) if r is not None]
        # Snapshot every path workers wrote *before* doing anything that can
        # fail. If aggregation / dedup / schema reconciliation / commit raises
        # below, those files are orphans (the commit is atomic and did not
        # complete), so ``on_write_failed`` must clean them up. This
        # complements the worker-side tracking, which only covers tasks that
        # themselves failed -- not a driver-side commit failure after all
        # workers succeeded.
        all_written_paths: List[str] = []
        for r in returns:
            all_written_paths.extend(r.written_paths or [])

        try:
            self._aggregate_and_commit(returns)
        except Exception as e:
            _attach_written_paths(e, all_written_paths)
            raise

    def _aggregate_and_commit(self, returns: List[DeltaWriteResult]) -> None:
        all_actions: List["AddAction"] = []
        all_schemas: List[pa.Schema] = []
        aggregated_write_uuid: Optional[str] = None
        seen_paths: Set[str] = set()

        for r in returns:
            for action in r.add_actions:
                path = self.path_for_action(action)
                if path in seen_paths:
                    raise ValueError(f"Duplicate file paths detected: {path}")
                seen_paths.add(path)
                all_actions.append(action)
            if r.emitted_schemas:
                all_schemas.extend(r.emitted_schemas)
            uuid_val = r.task_metadata.get("write_uuid") if r.task_metadata else None
            if uuid_val and aggregated_write_uuid is None:
                aggregated_write_uuid = uuid_val

        unified_schema = (
            _unify_schemas(all_schemas) if all_schemas else self._declared_schema
        )

        # Driver-side schema reconciliation: fold the already-promoted union
        # of every worker's schema into the existing table's schema (if any),
        # so type promotions across workers + table are handled consistently.
        if unified_schema is not None:
            existing = try_get_deltatable(self.table_uri, self.storage_options)
            existing_schema = (
                existing_table_pyarrow_schema(existing) if existing else None
            )

            # Schema evolution happens here -- at commit time, using the
            # fully worker-reconciled schema -- rather than in
            # ``on_write_start`` (see comment there). This is also the only
            # place ``schema_mode`` is enforced when the schema came from
            # Ray's inference rather than an explicit ``schema=``. Skipped
            # for ``IGNORE``-against-existing-table: that write is a
            # complete no-op, and ``_write_block`` returns the block's
            # Arrow schema even when ``self._skip_write`` short-circuits
            # the actual file write, so ``unified_schema`` can be non-None
            # here even though nothing was (or should be) written.
            #
            # Validate *before* calling ``reconcile_worker_schemas`` below:
            # that helper unifies ``existing_schema`` and ``unified_schema``
            # via PyArrow's ``unify_schemas``, which raises a raw, unfriendly
            # ``ArrowTypeError`` for genuinely incompatible types (e.g.
            # string vs int64). Checking compatibility ourselves first (via
            # ``types_compatible``, a plain Python comparison) turns that
            # into the same clean ``ValueError`` this validation has always
            # produced for an explicit ``schema=``.
            new_fields: List[Tuple[str, pa.DataType, bool]] = []
            if existing is not None and not self._skip_write:
                new_fields = validate_and_plan_evolution(
                    self._schema_policy, existing_schema, unified_schema
                )

            merged = reconcile_worker_schemas([unified_schema], existing_schema)
            if merged is not None:
                self.schema = merged
                if new_fields:
                    # Residual non-atomicity, inherent to delta-rs's public
                    # Python API rather than something this call ordering
                    # can avoid: ``evolve_schema`` issues its own commit
                    # (``DeltaTable.alter.add_columns``) that lands before
                    # the data commit below. Passing the merged schema
                    # directly to ``create_write_transaction`` instead
                    # (avoiding the extra commit) does NOT work -- verified
                    # empirically that its ``schema=`` argument does not
                    # drive schema evolution; a schema wider than the
                    # table's current one is silently ignored and the
                    # extra column's data is dropped on read-back. So if
                    # the data commit below fails after this succeeds, the
                    # table permanently gains the new (nullable) column
                    # with no corresponding data -- a schema-only,
                    # backward-compatible state, not data loss or
                    # corruption; the next successful write is unaffected.
                    evolve_schema(existing, new_fields)

        # Mode-specific commit. ``_coerce_mode`` at __init__ time guarantees
        # ``self.mode`` is one of these four.
        if self.mode in (SaveMode.APPEND, SaveMode.ERROR, SaveMode.IGNORE):
            self._commit_append(all_actions)
        elif self.mode == SaveMode.OVERWRITE:
            self._commit_overwrite(all_actions)
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    def _commit_append(self, file_actions: List["AddAction"]) -> None:
        # IGNORE against an existing table: the write tasks still ran, so
        # workers wrote data files we are NOT going to commit. Delete them
        # best-effort so they don't leak as orphans (they are unreferenced by
        # the Delta log, hence invisible to readers but wasting storage).
        if self._skip_write:
            if file_actions:
                self._cleanup_files_driver(
                    [self.path_for_action(a) for a in file_actions]
                )
            return

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        # Race: ERROR mode passed preflight because no table existed then,
        # but a concurrent writer created one before our commit. Refuse to
        # silently append to it (ERROR semantics: the table must not already
        # exist).
        if self.mode == SaveMode.ERROR and existing is not None:
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
            write_kwargs=self.write_kwargs,
            local_filesystem_root=self._local_filesystem_root,
        )

        if not file_actions:
            # Empty APPEND: create empty table if needed; otherwise no-op.
            if existing is None and self.schema is not None:
                create_table_with_files(inputs, [], self.schema, self.filesystem)
            return

        validate_file_actions(
            file_actions, self.filesystem, self._local_filesystem_root
        )
        if existing:
            commit_to_existing_table(
                inputs, existing, file_actions, self.schema, self.filesystem
            )
        else:
            create_table_with_files(inputs, file_actions, self.schema, self.filesystem)

    def _commit_overwrite(self, file_actions: List["AddAction"]) -> None:
        if self._skip_write:
            return

        existing = try_get_deltatable(self.table_uri, self.storage_options)
        inputs = CommitInputs(
            table_uri=self.table_uri,
            mode=SaveMode.OVERWRITE.value,
            partition_cols=self.partition_cols,
            storage_options=self.storage_options,
            write_kwargs=self.write_kwargs,
            local_filesystem_root=self._local_filesystem_root,
        )

        if not file_actions:
            # OVERWRITE with no data. Static mode truncates the whole
            # table (matches its "replace everything" semantics even when
            # "everything" is empty). Dynamic mode's whole point is "only
            # replace partitions present in the new data" -- an empty
            # write has no partitions in it, so it correctly touches
            # nothing (mirrors Spark's dynamic partitionOverwriteMode,
            # which likewise no-ops on an empty write: there's no
            # information available about which partition the caller
            # meant to replace). This is intentional, not a bug -- see
            # ``commit_to_existing_table``'s dynamic-overwrite branch,
            # which resolves to ``commit_mode="append"`` with no actions
            # here, i.e. a true no-op.
            if existing is not None:
                commit_to_existing_table(
                    inputs, existing, [], self.schema, self.filesystem
                )
                return
            if self.schema is not None:
                create_table_with_files(inputs, [], self.schema, self.filesystem)
            return

        validate_file_actions(
            file_actions, self.filesystem, self._local_filesystem_root
        )
        if existing:
            # Either the table existed at start, or it appeared between
            # preflight and commit; in both cases OVERWRITE delete+append is
            # the correct outcome.
            commit_to_existing_table(
                inputs, existing, file_actions, self.schema, self.filesystem
            )
        else:
            create_table_with_files(inputs, file_actions, self.schema, self.filesystem)

    # ------------------------------------------------------------------
    # Driver lifecycle -- on_write_failed (orphan cleanup).
    # ------------------------------------------------------------------
    def on_write_failed(self, error: Exception) -> None:
        paths = list(getattr(error, "_table_written_paths", None) or [])
        if not paths:
            logger.error(
                "Delta write failed for %s. Could not determine files to clean up.",
                self.table_uri,
            )
            return
        logger.warning(
            "Delta write failed for %s. Cleaning up %d orphaned files.",
            self.table_uri,
            len(paths),
        )
        try:
            self._cleanup_files_driver(paths)
        except Exception as cleanup_error:  # noqa: BLE001
            logger.warning(
                "Cleanup raised %s; ignoring to avoid masking the primary error.",
                cleanup_error,
            )

    # ------------------------------------------------------------------
    # Cleanup helpers.
    # ------------------------------------------------------------------
    def _cleanup_files_driver(self, file_paths: List[str]) -> None:
        fs = self.filesystem
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
