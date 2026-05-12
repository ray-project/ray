"""Generic lakehouse datasink that drives a ``LakehouseAdapter``.

This is the "Lakehouse framework" participant in the design sequence diagram.
It owns the Ray Data write lifecycle (``on_write_start`` → workers' ``write``
→ ``on_write_complete`` → ``on_write_failed``) and delegates every
format-specific decision to a ``LakehouseAdapter``.

The orchestration is intentionally simple and identical for every adapter, so
that supporting a new lakehouse format reduces to implementing a new adapter.
"""

import logging
from typing import Any, Dict, Generic, Iterable, List, Optional, TypeVar

import pyarrow as pa

from .adapter import LakehouseAdapter
from .modes import SaveMode, UpsertSemantics
from .result import LakehouseWriteTaskResult
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

logger = logging.getLogger(__name__)

FileAction = TypeVar("FileAction")


class LakehouseDatasink(
    Datasink[LakehouseWriteTaskResult],
    Generic[FileAction],
):
    """Generic datasink for any lakehouse format with a pluggable adapter.

    Args:
        adapter: ``LakehouseAdapter`` providing the format-specific behaviour.
        mode: One of ``SaveMode.{APPEND,OVERWRITE,UPSERT,ERROR,IGNORE}``. Must
            be in ``adapter.supported_modes``.
        partition_cols: Optional Hive-style partition columns. Forwarded to
            ``adapter.preflight``.
        declared_schema: Optional user-declared schema. Forwarded to
            ``adapter.preflight``.
        join_cols: Columns to match on in UPSERT mode (ignored otherwise).
        overwrite_filter: Predicate for partial OVERWRITE (forwarded to
            ``adapter.build_delete_predicate``).
        name: Human-readable name override for write tasks.
    """

    def __init__(
        self,
        adapter: LakehouseAdapter[FileAction],
        mode: SaveMode,
        *,
        partition_cols: Optional[List[str]] = None,
        declared_schema: Optional[pa.Schema] = None,
        join_cols: Optional[List[str]] = None,
        overwrite_filter: Optional[Any] = None,
        name: Optional[str] = None,
    ):
        self._adapter = adapter
        self._mode = self._coerce_mode(mode)
        self._partition_cols = list(partition_cols or [])
        self._declared_schema = declared_schema
        self._join_cols = list(join_cols or [])
        self._overwrite_filter = overwrite_filter
        self._name_override = name

        self._validate_mode_against_adapter()

    # ------------------------------------------------------------------
    # Helpers.
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_mode(mode: Any) -> SaveMode:
        if isinstance(mode, SaveMode):
            return mode
        if isinstance(mode, str):
            try:
                return SaveMode(mode.lower())
            except ValueError as e:
                raise ValueError(
                    f"Invalid mode '{mode}'. Supported: {[m.value for m in SaveMode]}"
                ) from e
        raise TypeError(f"Invalid mode type: {type(mode).__name__}")

    def _validate_mode_against_adapter(self) -> None:
        supported = self._adapter.supported_modes
        if self._mode not in supported:
            raise ValueError(
                f"{type(self._adapter).__name__} does not support mode "
                f"{self._mode}. Supported: {sorted(m.value for m in supported)}"
            )
        if self._mode == SaveMode.UPSERT and self._adapter.upsert_semantics not in (
            UpsertSemantics.COPY_ON_WRITE,
            UpsertSemantics.MERGE_ON_READ,
        ):
            raise ValueError(
                f"{type(self._adapter).__name__} declared an invalid "
                f"upsert_semantics: {self._adapter.upsert_semantics}"
            )

    # ------------------------------------------------------------------
    # Datasink overrides — Ray Data write lifecycle.
    # ------------------------------------------------------------------

    def get_name(self) -> str:
        if self._name_override:
            return self._name_override
        return self._adapter.get_name()

    @property
    def supports_distributed_writes(self) -> bool:
        return self._adapter.supports_distributed_writes

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self._adapter.min_rows_per_write

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        """Driver-side lifecycle: preflight then adapter pre-write hook.

        Matches steps 3 and 4 of the design sequence diagram.
        """
        self._adapter.preflight(
            mode=self._mode,
            partition_cols=self._partition_cols,
            declared_schema=self._declared_schema,
        )
        self._adapter.on_write_start(schema_from_first_bundle=schema)

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> LakehouseWriteTaskResult[FileAction]:
        """Worker-side lifecycle: per-Arrow-table ``write_block`` calls.

        Matches step 5 of the design sequence diagram.
        """
        self._adapter.start_task(ctx)

        file_actions: List[FileAction] = []
        emitted_schemas: List[pa.Schema] = []
        key_chunks: List[pa.Table] = []
        written_paths: List[str] = []

        try:
            for block in blocks:
                arrow_table = BlockAccessor.for_block(block).to_arrow()
                if arrow_table.num_rows == 0:
                    continue
                actions, emitted_schema, upsert_keys = self._adapter.write_block(
                    arrow_table
                )
                if actions:
                    file_actions.extend(actions)
                    for action in actions:
                        path = getattr(action, "path", None)
                        if isinstance(path, str):
                            written_paths.append(path)
                if emitted_schema is not None:
                    emitted_schemas.append(emitted_schema)
                if upsert_keys is not None:
                    key_chunks.append(upsert_keys)

            extra_actions, extra_schemas = self._adapter.finalize_task()
            if extra_actions:
                file_actions.extend(extra_actions)
                for action in extra_actions:
                    path = getattr(action, "path", None)
                    if isinstance(path, str):
                        written_paths.append(path)
            if extra_schemas:
                emitted_schemas.extend(extra_schemas)
        except Exception as e:
            # Surface the orphan-path list to the driver via the exception so
            # the framework can hand it to ``adapter.on_failure``.
            existing = getattr(e, "_lakehouse_written_paths", None) or []
            e._lakehouse_written_paths = list(existing) + written_paths
            raise

        upsert_keys = _concat_tables(key_chunks)
        return LakehouseWriteTaskResult(
            file_actions=file_actions,
            emitted_schemas=emitted_schemas,
            upsert_keys=upsert_keys,
            written_paths=written_paths,
            task_id=getattr(ctx, "task_idx", None),
            task_metadata=dict(self._adapter.task_metadata() or {}),
        )

    def on_write_complete(
        self, write_result: WriteResult[LakehouseWriteTaskResult[FileAction]]
    ) -> None:
        """Driver-side: aggregate, reconcile, commit.

        Matches steps 6, 7, 8 of the design sequence diagram.
        """
        all_actions: List[FileAction] = []
        all_schemas: List[pa.Schema] = []
        all_key_chunks: List[pa.Table] = []
        all_task_metadata: List[Dict[str, Any]] = []
        seen_paths = set()

        for r in write_result.write_returns or []:
            if r is None:
                continue
            for action in r.file_actions:
                path = getattr(action, "path", None)
                if isinstance(path, str):
                    if path in seen_paths:
                        raise ValueError(f"Duplicate file paths detected: {path}")
                    seen_paths.add(path)
                all_actions.append(action)
            if r.emitted_schemas:
                all_schemas.extend(r.emitted_schemas)
            if r.upsert_keys is not None:
                all_key_chunks.append(r.upsert_keys)
            if r.task_metadata:
                all_task_metadata.append(r.task_metadata)

        unified_schema = (
            _unify_schemas(all_schemas) if all_schemas else self._declared_schema
        )
        upsert_keys = _concat_tables(all_key_chunks)

        # Hand the adapter every task's metadata before commit.
        self._adapter.gather_task_metadata(all_task_metadata)

        # Step 7 — schema reconciliation + delete-predicate construction.
        self._adapter.reconcile_schema(unified_schema)
        predicate = self._adapter.build_delete_predicate(
            mode=self._mode,
            file_actions=all_actions,
            upsert_keys=upsert_keys,
            join_cols=self._join_cols,
            overwrite_filter=self._overwrite_filter,
        )

        # Step 8 — single commit; the adapter dispatches on mode internally.
        # Always invoked, even with empty actions, so adapters can honour
        # "create empty table" / IGNORE / no-op semantics uniformly.
        self._adapter.commit(
            mode=self._mode,
            file_actions=all_actions,
            delete_predicate=predicate,
        )

    def on_write_failed(self, error: Exception) -> None:
        """Driver-side: hand the worker's orphan-path list to the adapter."""
        paths = list(getattr(error, "_lakehouse_written_paths", None) or [])
        if paths:
            logger.warning(
                "Lakehouse write failed; attempting cleanup of %d orphaned files.",
                len(paths),
            )
        try:
            self._adapter.on_failure(paths)
        except Exception as cleanup_error:  # noqa: BLE001
            logger.warning(
                "Adapter on_failure raised %s; ignoring to avoid masking the "
                "primary error.",
                cleanup_error,
            )


# ----------------------------------------------------------------------
# Shared helpers, exposed for adapters that want to call them directly.
# ----------------------------------------------------------------------


def _unify_schemas(schemas: List[pa.Schema]) -> Optional[pa.Schema]:
    """Type-promoted ``pa.unify_schemas``; tolerant of older PyArrow versions."""
    if not schemas:
        return None
    from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

    return unify_schemas(schemas, promote_types=True)


def _concat_tables(tables: List[pa.Table]) -> Optional[pa.Table]:
    if not tables:
        return None
    from ray.data._internal.arrow_ops.transform_pyarrow import concat

    return concat(tables)
