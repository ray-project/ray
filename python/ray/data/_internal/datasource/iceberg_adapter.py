"""Apache Iceberg adapter that plugs into ``LakehouseDatasink``.

This module is the format-specific half of the Iceberg write path. The
generic ``LakehouseDatasink`` owns lifecycle orchestration; this adapter owns:

* loading the Iceberg table from the configured catalog,
* evolving the table schema (pre-write and at commit time),
* writing Parquet files inside each worker via PyIceberg's
  ``_dataframe_to_data_files``,
* committing those files through a PyIceberg ``Transaction`` for APPEND,
  OVERWRITE, or copy-on-write UPSERT.

PyIceberg: https://py.iceberg.apache.org/
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

from ray._common.retry import call_with_retry
from ray.data._internal.datasource.lakehouse import (
    LakehouseAdapter,
    SaveMode,
    UpsertSemantics,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.context import DataContext

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.update.schema import UpdateSchema

logger = logging.getLogger(__name__)


_UPSERT_COLS_ID = "join_cols"


class IcebergAdapter(LakehouseAdapter["DataFile"]):
    """``LakehouseAdapter`` for Apache Iceberg.

    Schema evolution is automatic: new columns in the incoming data are added
    to the table schema before writes start (``on_write_start``) and the
    unified schema across workers is re-applied at commit time
    (``reconcile_schema`` -> ``commit``). Copy-on-write UPSERT is supported
    via PyIceberg's ``create_match_filter`` helper.
    """

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

    @property
    def upsert_semantics(self) -> UpsertSemantics:
        return UpsertSemantics.COPY_ON_WRITE

    def get_name(self) -> str:
        return "Iceberg"

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
    # Driver lifecycle — gather, reconcile, build_delete_predicate, commit.
    # ------------------------------------------------------------------
    def reconcile_schema(self, unified_schema: Optional["pa.Schema"]) -> None:
        """Stash the unified worker schema so commit can apply it inside the
        transaction (alongside the data-file append)."""
        self._unified_schema = unified_schema

    def build_delete_predicate(
        self,
        mode: SaveMode,
        file_actions: List["DataFile"],
        upsert_keys: Optional["pa.Table"],
        join_cols: List[str],
        overwrite_filter: Optional[Any],
    ) -> Optional[Any]:
        if mode == SaveMode.OVERWRITE:
            from pyiceberg.expressions import AlwaysTrue

            if self._overwrite_filter is not None:
                from ray.data._internal.datasource.iceberg_datasource import (
                    _IcebergExpressionVisitor,
                )

                visitor = _IcebergExpressionVisitor()
                return visitor.visit(self._overwrite_filter)
            return AlwaysTrue()

        if mode == SaveMode.UPSERT:
            import functools

            import pyarrow as pa
            from pyiceberg.table.upsert_util import create_match_filter

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
            return create_match_filter(keys_table, upsert_cols)

        return None

    def commit(
        self,
        mode: SaveMode,
        file_actions: List["DataFile"],
        delete_predicate: Optional[Any],
    ) -> None:
        if not file_actions:
            return

        # Reconcile schema-with-promotion against the latest table schema.
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

        if mode == SaveMode.APPEND:
            self._append_and_commit(txn, file_actions)
        elif mode == SaveMode.OVERWRITE:
            self._commit_overwrite(txn, file_actions, delete_predicate)
        elif mode == SaveMode.UPSERT:
            self._commit_upsert(txn, file_actions, delete_predicate)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    # ------------------------------------------------------------------
    # Commit helpers — lifted from the previous IcebergDatasink.
    # ------------------------------------------------------------------
    def _append_and_commit(
        self, txn: "Table.transaction", data_files: List["DataFile"]
    ) -> None:
        with txn._append_snapshot_producer(self._snapshot_properties) as append_files:
            for data_file in data_files:
                append_files.append_data_file(data_file)
        self._with_retry(
            txn.commit_transaction,
            description=(
                f"commit transaction to Iceberg table '{self.table_identifier}'"
            ),
        )

    def _commit_overwrite(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        delete_filter: Optional[Any],
    ) -> None:
        if delete_filter is None:
            from pyiceberg.expressions import AlwaysTrue

            delete_filter = AlwaysTrue()
        txn.delete(
            delete_filter=delete_filter,
            snapshot_properties=self._snapshot_properties,
            **self._overwrite_kwargs,
        )
        self._append_and_commit(txn, data_files)

    def _commit_upsert(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        delete_filter: Optional[Any],
    ) -> None:
        if delete_filter is not None:
            delete_kwargs = dict(self._upsert_kwargs)
            delete_kwargs.pop(_UPSERT_COLS_ID, None)
            txn.delete(
                delete_filter=delete_filter,
                snapshot_properties=self._snapshot_properties,
                **delete_kwargs,
            )
        self._append_and_commit(txn, data_files)

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
