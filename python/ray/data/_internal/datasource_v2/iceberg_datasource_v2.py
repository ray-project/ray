"""Concrete ``DataSourceV2`` for Iceberg tables.

Wires the V2 listing (:class:`IcebergFileIndexer` and
:class:`IcebergFilePartitioner`, driven by the upstream ``ListFiles`` op),
scanning (:class:`IcebergScanner`) and reading (:class:`IcebergFileReader`)
components against a table in a PyIceberg catalog.
Constructed from ``read_api.read_iceberg`` when
``DataContext.use_iceberg_datasource_v2`` is set.

The catalog is loaded here, on the driver, for exactly two things: the schema
and the snapshot id. Everything else -- which files to read, and reading them --
happens in tasks.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa

from ray.data._internal.datasource_v2.datasource_v2 import (
    DatasourceCategory,
    DataSourceV2,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.iceberg_file_indexer import (
    IcebergFileIndexer,
)
from ray.data._internal.datasource_v2.partitioners.iceberg_file_partitioner import (
    IcebergFilePartitioner,
)
from ray.data._internal.datasource_v2.scanners.iceberg_scanner import IcebergScanner
from ray.data._internal.util import _check_import
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, Table

logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasourceV2(DataSourceV2[FileManifest]):
    """V2 Iceberg datasource.

    Takes the same arguments as the V1 :class:`IcebergDatasource` so
    ``read_iceberg``'s signature is unchanged.
    """

    def __init__(
        self,
        table_identifier: str,
        *,
        row_filter: Optional[Union[str, "BooleanExpression"]] = None,
        selected_fields: Tuple[str, ...] = ("*",),
        snapshot_id: Optional[int] = None,
        scan_kwargs: Optional[Dict[str, Any]] = None,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        bin_packing_bytes: Optional[int] = None,
    ):
        super().__init__(name="IcebergV2", category=DatasourceCategory.DATA_LAKE)
        _check_import(self, module="pyiceberg", package="pyiceberg")

        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.expressions.parser import parse

        self._table_identifier = table_identifier
        self._selected_fields = selected_fields
        self._scan_kwargs = dict(scan_kwargs) if scan_kwargs else {}
        self._catalog_kwargs = dict(catalog_kwargs) if catalog_kwargs else {}
        self._catalog_name = self._catalog_kwargs.pop("name", "default")
        # ``Table.scan`` parses a string filter itself, but ``ArrowScan``
        # requires a real expression and would fail in the worker, so parse
        # once here and only ever hold the parsed form.
        if row_filter is None:
            self._row_filter: "BooleanExpression" = AlwaysTrue()
        elif isinstance(row_filter, str):
            self._row_filter = parse(row_filter)
        else:
            self._row_filter = row_filter
        # V1 accepted the snapshot either way round; keep both spellings
        # working, with the explicit argument winning.
        self._snapshot_id = (
            snapshot_id
            if snapshot_id is not None
            else self._scan_kwargs.pop("snapshot_id", None)
        )
        self._case_sensitive = self._scan_kwargs.get("case_sensitive", True)
        self._limit = self._scan_kwargs.get("limit")
        # ``None`` means "whatever the partitioner defaults to"; the default
        # and its env override live there, next to the packing loop that uses
        # them.
        self._bin_packing_bytes = bin_packing_bytes
        self._table: Optional["Table"] = None
        self._snapshot_pinned = False

    # --- Driver-side catalog access -------------------------------------

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    @property
    def table(self) -> "Table":
        if self._table is None:
            self._table = self._get_catalog().load_table(self._table_identifier)
        return self._table

    @property
    def snapshot_id(self) -> Optional[int]:
        """The snapshot every task reads, resolved once and frozen.

        Listing now happens at execution time, so without pinning, two
        executions of the same ``Dataset`` -- or a retried task -- could read
        different versions of the table. ``None`` means the table has no
        snapshot at all, i.e. nothing has been written to it yet.
        """
        if not self._snapshot_pinned:
            if self._snapshot_id is None:
                snapshot = self.table.current_snapshot()
                self._snapshot_id = None if snapshot is None else snapshot.snapshot_id
            self._snapshot_pinned = True
        return self._snapshot_id

    def _data_scan(self) -> "DataScan":
        return self.table.scan(
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=self.snapshot_id,
            **self._scan_kwargs,
        )

    def _projected_schema(self) -> "Schema":
        return self._data_scan().projection()

    # --- ``DataSourceV2`` interface -------------------------------------

    @property
    def paths(self) -> List[str]:
        # ``ListFiles`` is path-driven and turns this into the one listing
        # task's input. The indexer ignores it -- a table identifier, not a
        # path set, is what says which files to read -- so this is a label,
        # and it is the label lineage tracking shows for the read.
        return [f"iceberg://{self._table_identifier}"]

    @property
    def filesystem(self) -> Optional["FileSystem"]:
        # PyIceberg does its own IO through the table's ``FileIO``.
        return None

    @property
    def file_extensions(self) -> Optional[List[str]]:
        # Extension-based pruning is meaningless here: the set of files is
        # whatever the table's manifests name.
        return None

    @property
    def schema_needs_file_sample(self) -> bool:
        # The table declares its schema, so there is nothing to sample. This
        # also makes an empty table readable rather than an error.
        return False

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # ``include_field_ids=False`` matches what the reader produces:
        # ``ArrowScan`` strips field ids from the batches it yields.
        return schema_to_pyarrow(self._projected_schema(), include_field_ids=False)

    def _get_file_indexer(self) -> IcebergFileIndexer:
        return IcebergFileIndexer(
            table_identifier=self._table_identifier,
            catalog_name=self._catalog_name,
            catalog_kwargs=dict(self._catalog_kwargs),
            scan_kwargs=dict(self._scan_kwargs),
            snapshot_id=self.snapshot_id,
            row_filter=self._row_filter,
        )

    def get_file_partitioner(self, **kwargs: Any) -> IcebergFilePartitioner:
        # The listing rows already carry each file's size and row count, so the
        # default size-estimating partitioner has nothing to add; ``kwargs``
        # (the estimator and the bucket bounds it would use) are ignored for
        # that reason.
        return IcebergFilePartitioner(max_bin_bytes=self._bin_packing_bytes)

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> IcebergScanner:
        table = self.table
        return IcebergScanner(
            schema=schema,
            projected_schema=self._projected_schema(),
            table_metadata=table.metadata,
            io=table.io,
            row_filter=self._row_filter,
            case_sensitive=self._case_sensitive,
            limit=self._limit,
        )
