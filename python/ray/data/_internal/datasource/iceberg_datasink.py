"""Iceberg datasink — thin facade over TableDatasink + IcebergAdapter.

After the table-abstraction refactor, the Ray Data write lifecycle lives
in :class:`ray.data._internal.datasource.table.TableDatasink` and the
Iceberg-specific behaviour lives in
:class:`ray.data._internal.datasource.iceberg_adapter.IcebergAdapter`. This
module preserves the original ``IcebergDatasink`` class so existing callers
and tests keep working.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.datasource.iceberg_adapter import IcebergAdapter
from ray.data._internal.datasource.table import (
    SaveMode,
    TableDatasink,
)
from ray.data._internal.datasource.table.result import TableWriteTaskResult
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

# Back-compat: legacy name for code that imported IcebergWriteResult from
# this module before the table-abstraction refactor. New code should use
# ``TableWriteTaskResult`` directly.
IcebergWriteResult = TableWriteTaskResult

if TYPE_CHECKING:
    from pyiceberg.manifest import DataFile  # noqa: F401


@DeveloperAPI
class IcebergDatasink(TableDatasink["DataFile", Any]):
    """Datasink for writing a Ray Dataset to an existing Iceberg table.

    Workers write Parquet files to storage and return ``DataFile`` metadata
    to the driver; the driver collects every worker's metadata and performs
    a single atomic PyIceberg transaction. Schema evolution is automatic —
    new columns in the incoming data are added to the table schema; type
    promotion across blocks is reconciled at commit time.
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        mode: SaveMode = SaveMode.APPEND,
        overwrite_filter: Optional["Expr"] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
        overwrite_kwargs: Optional[Dict[str, Any]] = None,
        **write_kwargs: Any,
    ):
        """See ``Dataset.write_iceberg`` for argument semantics.

        ``**write_kwargs`` is forwarded to ``IcebergAdapter`` so per-call
        retry overrides (``commit_retry_max_attempts`` etc., recognised by
        the shared :func:`_extract_retry_overrides` helper) can take effect.
        """
        adapter = IcebergAdapter(
            table_identifier=table_identifier,
            catalog_kwargs=catalog_kwargs,
            snapshot_properties=snapshot_properties,
            overwrite_filter=overwrite_filter,
            upsert_kwargs=upsert_kwargs,
            overwrite_kwargs=overwrite_kwargs,
            **write_kwargs,
        )

        join_cols: List[str] = []
        if upsert_kwargs:
            cols = upsert_kwargs.get("join_cols") or []
            if isinstance(cols, list):
                join_cols = list(cols)

        super().__init__(
            adapter,
            mode,
            join_cols=join_cols,
            overwrite_filter=overwrite_filter,
            name="Iceberg",
        )

    # ------------------------------------------------------------------
    # Compatibility shims — surface the most commonly introspected fields
    # of the legacy IcebergDatasink so existing tests that poke at the
    # instance keep working.
    # ------------------------------------------------------------------
    @property
    def table_identifier(self) -> str:
        return self._adapter.table_identifier

    @property
    def _table(self):
        return self._adapter._table

    @property
    def _table_metadata(self):
        return self._adapter._table_metadata
