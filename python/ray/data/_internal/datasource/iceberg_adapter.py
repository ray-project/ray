"""Apache Iceberg adapter that plugs into ``TableDatasink``.

This module is the format-specific half of the Iceberg write path. The
generic ``TableDatasink`` owns lifecycle orchestration; this adapter owns:

* loading the Iceberg table from the configured catalog,
* evolving the table schema (pre-write and at commit time),
* writing Parquet files inside each worker via PyIceberg's
  ``_dataframe_to_data_files``,
* committing those files through a PyIceberg ``Transaction`` for APPEND,
  OVERWRITE, or copy-on-write UPSERT.

UPSERT uses a distributed scan-merge: the driver computes an O(1) coarse
range filter covering the upsert key values, plans candidate files, and
dispatches one Ray task per file. Each task streams its file, anti-joins
against the upsert keys, and writes preserved rows as new data files. The
driver then issues a single atomic overwrite snapshot: delete each
candidate file, append preserved files, append the new upsert payload.

PyIceberg: https://py.iceberg.apache.org/
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

import ray
from ray._common.retry import call_with_retry
from ray.data._internal.datasource.parquet_datasource import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
from ray.data._internal.datasource.table import (
    SaveMode,
    SupportsUpserts,
    TableAdapter,
    UpsertSemantics,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import MiB
from ray.data.context import DataContext

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, FileScanTask, Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.update.schema import UpdateSchema

logger = logging.getLogger(__name__)


_UPSERT_COLS_ID = "join_cols"
_REWRITE_STALL_TIMEOUT_S = 600


@dataclass
class IcebergUpsertPayload:
    """Payload returned from :meth:`IcebergAdapter.build_upsert_predicate`.

    The framework passes this back to :meth:`IcebergAdapter.commit_upsert`
    as ``delete_predicate``. Carries the keys + columns needed to drive the
    distributed scan-merge (anti-join, plan, rewrite, atomic overwrite). The
    ``coarse_filter`` is precomputed on the driver to avoid recomputing
    inside ``commit_upsert``.
    """

    keys_table: "pa.Table"
    upsert_cols: List[str]
    coarse_filter: Any  # pyiceberg BooleanExpression (avoid import-time pyiceberg dep)


@ray.remote
def _rewrite_iceberg_file(
    file_scan_task: "FileScanTask",
    keys_ref: "pa.Table",
    upsert_cols: List[str],
    table_metadata: "TableMetadata",
    io: "FileIO",
) -> "tuple[Optional[DataFile], List[DataFile]]":
    """Read one Iceberg file, anti-join against upsert keys, write preserved rows.

    Preserved rows are rows in the file that are not in the upsert batch. The
    coarse range filter would delete them (see
    ``IcebergAdapter._build_coarse_range_filter``), so we preserve them by
    writing them as new data files before the delete.

    The file is read in streaming fashion via ``ArrowScan.to_record_batches()``
    so the full file is never materialised at once. The anti-join is applied
    per RecordBatch and preserved rows are accumulated, then concatenated and
    written as a single output once the stream is exhausted.

    Returns (original DataFile to delete, list of new preserved DataFiles).
    If the entire file is matched (no preserved rows), returns (file, []).
    If the file has no matched rows at all, returns (None, []), leave it untouched.
    """
    import hashlib
    import time as _time
    import uuid as _uuid

    import numpy as np
    import pyarrow as pa
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files

    file_path = file_scan_task.file.file_path
    file_name = file_path.split("/")[-1]
    file_size_mb = file_scan_task.file.file_size_in_bytes / MiB
    t_start = _time.perf_counter()

    # Cast target pulled from keys_ref once.  Applied per batch so PyArrow's join
    # doesn't raise ArrowInvalid on utf8/large_utf8 or similar width mismatches.
    target_key_schema = pa.schema([keys_ref.schema.field(c) for c in upsert_cols])

    record_batches = ArrowScan(
        table_metadata=table_metadata,
        io=io,
        projected_schema=table_metadata.schema(),
        row_filter=AlwaysTrue(),
    ).to_record_batches(tasks=[file_scan_task])

    preserved_rows: Optional["pa.Table"] = None
    total_in_rows = 0
    total_preserved_rows = 0
    n_batches = 0

    for rb in record_batches:
        n_batches += 1
        batch_table = pa.Table.from_batches([rb])
        if len(batch_table) == 0:
            continue
        total_in_rows += len(batch_table)

        batch_keys = batch_table.select(upsert_cols).cast(target_key_schema)

        idx_col = pa.array(np.arange(len(batch_table), dtype=np.int64))
        preserved_keys = batch_keys.append_column("__row_idx__", idx_col).join(
            keys_ref, keys=upsert_cols, join_type="left anti"
        )

        if len(preserved_keys) > 0:
            new_rows = batch_table.take(preserved_keys["__row_idx__"])
            if preserved_rows is None:
                preserved_rows = new_rows
            else:
                preserved_rows = pa.concat_tables(
                    [preserved_rows, new_rows], promote_options="permissive"
                )
            total_preserved_rows += len(preserved_keys)

    t_read = _time.perf_counter()
    logger.debug(
        "[rewrite] stream-read+join %d rows / %.1f MB (compressed) from %s "
        "across %d batch(es) in %.2fs",
        total_in_rows,
        file_size_mb,
        file_name,
        n_batches,
        t_read - t_start,
    )

    if total_in_rows == 0:
        return (None, [])

    if total_preserved_rows == 0:
        # Every row in this file is being upserted — delete the whole file, no preserved file needed.
        logger.debug(
            "[rewrite] %s: all %d rows matched -> whole-file delete",
            file_name,
            total_in_rows,
        )
        return (file_scan_task.file, [])

    if total_preserved_rows == total_in_rows:
        # No rows in this file match any upsert key — leave it alone entirely.
        logger.debug("[rewrite] %s: 0 rows matched -> untouched", file_name)
        return (None, [])

    # Derive a deterministic write_uuid from the source file path so that
    # task retries overwrite the same object rather than leaking orphan files.
    preserved_write_uuid = _uuid.UUID(hashlib.md5(file_path.encode()).hexdigest())
    preserved_files = list(
        _dataframe_to_data_files(
            table_metadata=table_metadata,
            df=preserved_rows,
            io=io,
            write_uuid=preserved_write_uuid,
        )
    )
    logger.debug(
        "[rewrite] %s: %d/%d rows preserved -> wrote %d preserved file(s) in %.2fs",
        file_name,
        total_preserved_rows,
        total_in_rows,
        len(preserved_files),
        _time.perf_counter() - t_read,
    )
    return (file_scan_task.file, preserved_files)


class IcebergAdapter(
    TableAdapter["DataFile", Any],
    SupportsUpserts["DataFile", Any],
):
    """``TableAdapter`` for Apache Iceberg.

    Schema evolution is automatic: new columns in the incoming data are added
    to the table schema before writes start (``on_write_start``) and the
    unified schema across workers is re-applied at commit time
    (``reconcile_schema`` -> ``commit_*``). Copy-on-write UPSERT uses the
    distributed scan-merge described in the module docstring.

    The class also conforms to :class:`SupportsUpserts` (it declares
    ``upsert_semantics`` as a class attribute and implements
    ``build_upsert_predicate`` + ``commit_upsert``), so the framework allows
    ``SaveMode.UPSERT`` on this adapter.

    ``Any`` is the DeletePredicate type parameter because Iceberg's
    predicate carries either a PyIceberg ``BooleanExpression`` (OVERWRITE)
    or an :class:`IcebergUpsertPayload` (UPSERT). Strictly speaking this is
    a ``Union`` but we use ``Any`` to keep the generic signature readable.
    """

    #: Copy-on-write semantics: UPSERT rewrites whole candidate files rather
    #: than overlaying changes (no merge-on-read merge tables).
    upsert_semantics: UpsertSemantics = UpsertSemantics.COPY_ON_WRITE

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        overwrite_filter: Optional[Any] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
        overwrite_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.table_identifier = table_identifier
        self._catalog_kwargs = dict(catalog_kwargs or {})
        self._snapshot_properties = dict(snapshot_properties or {})
        self._overwrite_filter = overwrite_filter
        self._upsert_kwargs = dict(upsert_kwargs or {})
        self._overwrite_kwargs = dict(overwrite_kwargs or {})

        # Drop invalid params from overwrite_kwargs to match prior behaviour.
        for invalid_param, reason in [
            (
                "overwrite_filter",
                "should be passed as a separate parameter to write_iceberg()",
            ),
            (
                "delete_filter",
                "is an internal PyIceberg parameter; use 'overwrite_filter' instead",
            ),
        ]:
            if self._overwrite_kwargs.pop(invalid_param, None) is not None:
                logger.warning(
                    "Removed '%s' from overwrite_kwargs: %s", invalid_param, reason
                )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        # Driver-side state — populated in preflight / on_write_start.
        self._mode: Optional[SaveMode] = None
        self._table: Optional["Table"] = None
        self._io: Optional["FileIO"] = None
        self._table_metadata: Optional["TableMetadata"] = None
        self._unified_schema: Optional["pa.Schema"] = None
        self._data_context = DataContext.get_current()

    # ------------------------------------------------------------------
    # Pickling.
    # ------------------------------------------------------------------
    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state.pop("_table", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._table = None

    # ------------------------------------------------------------------
    # Introspection.
    # ------------------------------------------------------------------
    @property
    def supported_modes(self) -> Set[SaveMode]:
        return {SaveMode.APPEND, SaveMode.OVERWRITE, SaveMode.UPSERT}

    def get_name(self) -> str:
        return "Iceberg"

    def path_for_action(self, action: "DataFile") -> Optional[str]:
        """PyIceberg's ``DataFile`` exposes the path as ``file_path`` (not
        ``path``), so we override the framework default. Without this, the
        framework's duplicate-file detection and orphan-file tracking would
        silently no-op for Iceberg writes."""
        return getattr(action, "file_path", None)

    # ------------------------------------------------------------------
    # Validation helpers (mode <-> kwargs).
    # ------------------------------------------------------------------
    def _validate_kwargs(self, mode: SaveMode) -> None:
        if self._upsert_kwargs and mode != SaveMode.UPSERT:
            raise ValueError(
                "upsert_kwargs can only be specified when mode is "
                f"SaveMode.UPSERT, but mode is {mode}"
            )
        if self._overwrite_kwargs and mode != SaveMode.OVERWRITE:
            raise ValueError(
                "overwrite_kwargs can only be specified when mode is "
                f"SaveMode.OVERWRITE, but mode is {mode}"
            )
        if self._overwrite_filter is not None and mode != SaveMode.OVERWRITE:
            raise ValueError(
                "overwrite_filter can only be specified when mode is "
                f"SaveMode.OVERWRITE, but mode is {mode}"
            )

    def _with_retry(self, func: Callable, description: str) -> Any:
        cfg = self._data_context.iceberg_config
        return call_with_retry(
            func,
            description=description,
            match=cfg.catalog_retried_errors,
            max_attempts=cfg.catalog_max_attempts,
            max_backoff_s=cfg.catalog_retry_max_backoff_s,
        )

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return self._with_retry(
            lambda: catalog.load_catalog(self._catalog_name, **self._catalog_kwargs),
            description=f"load Iceberg catalog '{self._catalog_name}'",
        )

    def _reload_table(self) -> None:
        cat = self._get_catalog()
        self._table = self._with_retry(
            lambda: cat.load_table(self.table_identifier),
            description=f"load Iceberg table '{self.table_identifier}'",
        )
        self._io = self._table.io
        self._table_metadata = self._table.metadata

    def _get_upsert_cols(self) -> List[str]:
        upsert_cols = list(self._upsert_kwargs.get(_UPSERT_COLS_ID, []))
        if not upsert_cols and self._table_metadata is not None:
            schema = self._table_metadata.schema()
            for field_id in schema.identifier_field_ids:
                col_name = schema.find_column_name(field_id)
                if col_name:
                    upsert_cols.append(col_name)
        # With case_sensitive=False, map each requested join column to the
        # table's actual column casing. The downstream pyarrow select/anti-join
        # (write_block, _rewrite_iceberg_file) is always case-sensitive, so
        # without this a join_col like "COL_A" against a "col_a" column raises
        # KeyError. Mirrors pyiceberg's own case_sensitive scan handling.
        if not self._upsert_kwargs.get("case_sensitive", True) and (
            self._table_metadata is not None
        ):
            actual = [field.name for field in self._table_metadata.schema().fields]
            lower_to_actual = {name.lower(): name for name in actual}
            upsert_cols = [lower_to_actual.get(c.lower(), c) for c in upsert_cols]
        return upsert_cols

    # ------------------------------------------------------------------
    # Driver lifecycle — preflight + on_write_start.
    # ------------------------------------------------------------------
    def preflight(
        self,
        mode: SaveMode,
        partition_cols: List[str],
        declared_schema: Optional["pa.Schema"],
    ) -> None:
        self._mode = mode
        self._validate_kwargs(mode)
        self._reload_table()

        if mode == SaveMode.UPSERT:
            upsert_cols = self._upsert_kwargs.get(_UPSERT_COLS_ID, [])
            if not upsert_cols:
                identifier_field_ids = (
                    self._table.metadata.schema().identifier_field_ids
                )
                if not identifier_field_ids:
                    raise ValueError(
                        "join_cols must be specified in upsert_kwargs for UPSERT "
                        "mode when table has no identifier fields"
                    )

    def on_write_start(
        self, schema_from_first_bundle: Optional["pa.Schema"] = None
    ) -> None:
        """Evolve the table schema before any files are written.

        This prevents PyIceberg name-mapping errors when incoming data has
        new columns. The schema unification done at commit time
        (``reconcile_schema``) deals with type promotions across workers.
        """
        if schema_from_first_bundle is None or self._table is None:
            return

        table_schema = self._table.metadata.schema()

        def _update_schema() -> None:
            with self._table.update_schema() as update:
                self._update_schema_with_union(
                    update, schema_from_first_bundle, table_schema
                )

        self._with_retry(
            _update_schema,
            description=f"update schema for Iceberg table '{self.table_identifier}'",
        )
        self._reload_table()

    # ------------------------------------------------------------------
    # Worker lifecycle.
    # ------------------------------------------------------------------
    def start_task(self, ctx: TaskContext) -> None:
        return None

    def write_block(
        self, arrow_table: "pa.Table"
    ) -> Tuple[List["DataFile"], "pa.Schema", Optional["pa.Table"]]:
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        upsert_keys = None
        if self._mode == SaveMode.UPSERT:
            upsert_cols = self._get_upsert_cols()
            if upsert_cols:
                upsert_keys = arrow_table.select(upsert_cols)

        def _write_data_files() -> List["DataFile"]:
            return list(
                _dataframe_to_data_files(
                    table_metadata=self._table_metadata,
                    df=arrow_table,
                    io=self._io,
                )
            )

        cfg = self._data_context.iceberg_config
        data_files = call_with_retry(
            _write_data_files,
            description=f"write data files to Iceberg table '{self.table_identifier}'",
            match=self._data_context.retried_io_errors,
            max_attempts=cfg.write_file_max_attempts,
            max_backoff_s=cfg.write_file_retry_max_backoff_s,
        )

        return (data_files, arrow_table.schema, upsert_keys)

    # ------------------------------------------------------------------
    # Driver lifecycle — schema reconciliation + per-mode commits.
    # ------------------------------------------------------------------
    def reconcile_schema(self, unified_schema: Optional["pa.Schema"]) -> None:
        """Stash the unified worker schema so commit can apply it inside the
        transaction (alongside the data-file append)."""
        self._unified_schema = unified_schema

    def _begin_txn(self) -> "Table.transaction":
        """Open a transaction with schema evolution staged (if needed).

        Returns the transaction object. ``self._unified_schema`` must already
        be set by :meth:`reconcile_schema`.
        """
        from pyiceberg.io import pyarrow as pyi_pa_io

        from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

        table_schema = pyi_pa_io.schema_to_pyarrow(self._table.schema())
        if self._unified_schema is not None:
            final_reconciled_schema = unify_schemas(
                [table_schema, self._unified_schema], promote_types=True
            )
        else:
            final_reconciled_schema = table_schema

        txn = self._table.transaction()
        if not final_reconciled_schema.equals(table_schema):
            current_table_schema = self._table.metadata.schema()
            with txn.update_schema() as update:
                self._update_schema_with_union(
                    update, final_reconciled_schema, current_table_schema
                )
        return txn

    def commit_append(
        self,
        file_actions: List["DataFile"],
        unified_schema: Optional["pa.Schema"],
    ) -> None:
        if not file_actions:
            return
        # ``unified_schema`` was already stashed via ``reconcile_schema``;
        # ``_begin_txn`` consults ``self._unified_schema``.
        txn = self._begin_txn()
        self._append_and_commit(txn, file_actions)

    def build_overwrite_predicate(
        self, overwrite_filter: Optional[Any]
    ) -> Optional["BooleanExpression"]:
        from pyiceberg.expressions import AlwaysTrue

        if overwrite_filter is not None:
            from ray.data._internal.datasource.iceberg_datasource import (
                _IcebergExpressionVisitor,
            )

            visitor = _IcebergExpressionVisitor()
            return visitor.visit(overwrite_filter)
        return AlwaysTrue()

    def commit_overwrite(
        self,
        file_actions: List["DataFile"],
        unified_schema: Optional["pa.Schema"],
        delete_predicate: Optional["BooleanExpression"],
    ) -> None:
        if not file_actions:
            return
        txn = self._begin_txn()
        from pyiceberg.expressions import AlwaysTrue

        delete_filter = (
            delete_predicate if delete_predicate is not None else AlwaysTrue()
        )
        txn.delete(
            delete_filter=delete_filter,
            snapshot_properties=self._snapshot_properties,
            **self._overwrite_kwargs,
        )
        # Append on the same branch the delete targeted (defaults to "main").
        # Mirrors ray-project/ray#63922 in the refactored adapter: without this,
        # a `branch` in overwrite_kwargs lands the delete on the staging branch
        # but the append on main.
        branch = self._overwrite_kwargs.get("branch", "main")
        self._append_and_commit(txn, file_actions, branch=branch)

    def build_upsert_predicate(
        self, upsert_keys: "pa.Table", join_cols: List[str]
    ) -> Optional[IcebergUpsertPayload]:
        """Build the upsert payload — keys, columns, and coarse range filter.

        SQL-style: rows whose join-column values are NULL never match, so
        we filter them out before building the coarse filter and dispatching
        the rewrite. Returning ``None`` means "no rows need deletion"; the
        framework will route the file_actions to a pure-insert commit.
        """
        import functools

        import pyarrow as pa

        if upsert_keys is None or len(upsert_keys) == 0:
            return None
        upsert_cols = self._get_upsert_cols() or list(join_cols)
        if not upsert_cols:
            return None

        masks = (pa.compute.is_valid(upsert_keys[col]) for col in upsert_cols)
        mask = functools.reduce(pa.compute.and_, masks)
        keys_table = upsert_keys.filter(mask)
        if len(keys_table) == 0:
            return None

        # Dedup keys to minimise per-task anti-join hash table size.
        keys_table = keys_table.group_by(upsert_cols).aggregate([])
        coarse_filter = self._build_coarse_range_filter(keys_table, upsert_cols)
        return IcebergUpsertPayload(
            keys_table=keys_table,
            upsert_cols=upsert_cols,
            coarse_filter=coarse_filter,
        )

    def commit_upsert(
        self,
        file_actions: List["DataFile"],
        unified_schema: Optional["pa.Schema"],
        delete_predicate: Optional[IcebergUpsertPayload],
    ) -> None:
        """Distributed scan-merge UPSERT.

        See module docstring for the four-stage pipeline. If
        ``delete_predicate`` is ``None`` (no candidate keys after NULL
        filtering), this degenerates into a pure insert.
        """
        if delete_predicate is None:
            # Pure insert (no candidate deletions).
            self.commit_append(file_actions, unified_schema)
            return

        txn = self._begin_txn()
        self._commit_upsert_scan_merge(
            txn,
            file_actions,
            delete_predicate.keys_table,
            delete_predicate.upsert_cols,
            delete_predicate.coarse_filter,
        )

    # ------------------------------------------------------------------
    # Commit helpers — lifted from the previous IcebergDatasink.
    # ------------------------------------------------------------------
    def _append_and_commit(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        branch: str = "main",
    ) -> None:
        """Append data files to a transaction and commit.

        Args:
            txn: PyIceberg transaction object.
            data_files: List of DataFile objects to append.
            branch: Iceberg branch to commit the snapshot to. Defaults to
                ``"main"`` to match pyiceberg's default.
        """
        with txn._append_snapshot_producer(
            self._snapshot_properties, branch=branch
        ) as append_files:
            for data_file in data_files:
                append_files.append_data_file(data_file)
        self._with_retry(
            txn.commit_transaction,
            description=(
                f"commit transaction to Iceberg table '{self.table_identifier}'"
            ),
        )

    def _build_coarse_range_filter(
        self,
        keys_table: "pa.Table",
        upsert_cols: List[str],
    ) -> "BooleanExpression":
        """Build an O(1) coarse range filter covering all upsert key values.

        For each upsert column computes AND(GTE(col, min), LTE(col, max)).
        The filter may match rows outside the upsert batch (filter overshoot);
        callers must anti-join to identify and preserve those rows.
        """
        import pyarrow.compute as pc
        from pyiceberg.expressions import (
            AlwaysTrue,
            And,
            GreaterThanOrEqual,
            LessThanOrEqual,
        )

        expr = None
        for col_name in upsert_cols:
            mm = pc.min_max(keys_table[col_name])
            min_val = mm["min"].as_py()
            max_val = mm["max"].as_py()
            if min_val is None:
                continue
            col_expr = And(
                GreaterThanOrEqual(col_name, min_val),
                LessThanOrEqual(col_name, max_val),
            )
            expr = col_expr if expr is None else And(expr, col_expr)

        return expr if expr is not None else AlwaysTrue()

    def _commit_upsert_scan_merge(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        keys_table: "pa.Table",
        upsert_cols: List[str],
        coarse_filter: "BooleanExpression",
    ) -> None:
        """Upsert commit using coarse range filter + per-file distributed anti-join.

        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 1: Build coarse filter (driver)                      │
        │    keys_table ──► min/max per col ──► coarse_filter         │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 2: Plan candidate files (driver)                     │
        │    table.scan(coarse_filter).plan_files()                   │
        │        ──► file_scan_tasks                                  │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 3: Rewrite (one _rewrite_iceberg_file task per file) │
        │    read file ─► anti-join keys ─► write preserved rows      │
        │    returns (old_file, preserved_files)                      │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 4: Atomic overwrite (driver)                         │
        │    delete  old_file         (each rewritten candidate)      │
        │    append  preserved_files  (preserved rows kept)           │
        │    append  data_files       (new upsert payload)            │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                            commit_transaction
        """
        import time

        case_sensitive = self._upsert_kwargs.get("case_sensitive", True)
        branch = self._upsert_kwargs.get("branch", "main")
        unknown = set(self._upsert_kwargs) - {
            _UPSERT_COLS_ID,
            "case_sensitive",
            "branch",
        }
        if unknown:
            logger.warning(
                "[scan-merge] ignoring unsupported upsert_kwargs: %s", sorted(unknown)
            )

        logger.debug("[scan-merge] coarse_filter=%s", coarse_filter)

        # plan_files() reads only manifest metadata, no Parquet data on the driver.
        t0 = time.perf_counter()
        scan: "DataScan" = self._table.scan(
            row_filter=coarse_filter, case_sensitive=case_sensitive
        )
        # Use the specific branch for the scan
        scan = scan.use_ref(branch)
        file_scan_tasks: List["FileScanTask"] = list(scan.plan_files())

        logger.info(
            "[scan-merge] planned %d candidate file(s) in %.2fs",
            len(file_scan_tasks),
            time.perf_counter() - t0,
        )

        if not file_scan_tasks:
            # No existing files match the coarse filter, so it's a pure insert.
            self._append_and_commit(txn, data_files, branch=branch)
            return

        # Put the deduped keys in the object store once; all tasks share one copy.
        keys_ref = ray.put(keys_table)

        t0 = time.perf_counter()
        refs = [
            _rewrite_iceberg_file.options(
                memory=int(
                    task.file.file_size_in_bytes
                    * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
                    * 3  # Bump memory estimate to account for the anti-join and the preserved rows (also since to_record_batches materializes the entire table in memory, see https://github.com/apache/iceberg-python/issues/3036)
                ),
                num_cpus=1,
            ).remote(task, keys_ref, upsert_cols, self._table_metadata, self._io)
            for task in file_scan_tasks
        ]
        logger.info("[scan-merge] dispatched %d rewrite task(s)", len(refs))

        # Collect results with periodic progress logs so long rewrites aren't silent.
        results = []
        pending = list(refs)
        _LOG_INTERVAL = max(1, len(refs) // 10)  # log ~10 times total
        while pending:
            done, pending = ray.wait(
                pending,
                num_returns=min(_LOG_INTERVAL, len(pending)),
                timeout=_REWRITE_STALL_TIMEOUT_S,
                fetch_local=True,
            )
            results.extend(ray.get(done))
            logger.debug(
                "[scan-merge] rewrite progress: %d/%d file(s) done (%.1fs elapsed)",
                len(results),
                len(refs),
                time.perf_counter() - t0,
            )

        logger.info(
            "[scan-merge] all %d file(s) rewritten in %.2fs",
            len(refs),
            time.perf_counter() - t0,
        )

        # Count how many files were wholly deleted vs partially rewritten.
        n_whole_delete = n_partial = n_untouched = 0
        for old, preserved_files in results:
            if old is None:
                n_untouched += 1
            elif preserved_files:
                n_partial += 1
            else:
                n_whole_delete += 1
        logger.info(
            "[scan-merge] files: %d whole-delete, %d partial-rewrite, %d untouched",
            n_whole_delete,
            n_partial,
            n_untouched,
        )

        # Single atomic commit: schema update (already staged in txn), and overwrite.
        # _OverwriteFiles handles both file-level deletes and appends in one snapshot.
        t0 = time.perf_counter()
        with txn.update_snapshot(
            snapshot_properties=self._snapshot_properties, branch=branch
        ).overwrite() as snap:
            for old_file, preserved_files in results:
                if old_file is not None:
                    snap.delete_data_file(old_file)
                for preserved_file in preserved_files:
                    snap.append_data_file(preserved_file)
            for df in data_files:
                snap.append_data_file(df)

        self._with_retry(
            txn.commit_transaction,
            description=f"commit upsert transaction to Iceberg table '{self.table_identifier}'",
        )
        logger.info("[scan-merge] committed in %.2fs", time.perf_counter() - t0)

    def _preserve_identifier_field_requirements(
        self, update: "UpdateSchema", table_schema: "Schema"
    ) -> None:
        from pyiceberg.types import NestedField

        identifier_field_ids = table_schema.identifier_field_ids
        for field_id in identifier_field_ids:
            if field_id in update._updates:
                updated_field = update._updates[field_id]
                if not updated_field.required:
                    update._updates[field_id] = NestedField(
                        field_id=updated_field.field_id,
                        name=updated_field.name,
                        field_type=updated_field.field_type,
                        doc=updated_field.doc,
                        required=True,
                        initial_default=updated_field.initial_default,
                        write_default=updated_field.write_default,
                    )

    def _update_schema_with_union(
        self,
        update: "UpdateSchema",
        new_schema: Any,
        table_schema: "Schema",
    ) -> None:
        update.union_by_name(new_schema)
        self._preserve_identifier_field_requirements(update, table_schema)
