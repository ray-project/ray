"""Concrete ``DataSourceV2`` for Iceberg tables.

Wires the V2 listing (:class:`IcebergFileIndexer`, driven by the upstream
``ListFiles`` op), scanning (:class:`IcebergScanner`) and reading
(:class:`IcebergFileReader`) components against a table in a PyIceberg catalog.
Constructed from ``read_api.read_iceberg`` when
``DataContext.use_datasource_v2`` is set, which is the default.

The catalog is loaded here, on the driver: the listing and read tasks get the
loaded table's metadata and ``FileIO``, as V1's read tasks do, because the
catalog's configuration may exist only where the driver runs. Which files to
read, and reading them, happen in those tasks. When the table's REST catalog
plans scans on its server, the listing task rebuilds that catalog from the
properties resolved here (``ServerSidePlanning``) and asks the server instead.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_indexer import (
    IcebergFileIndexer,
    ServerSidePlanning,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_scanner import (
    IcebergScanner,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    DataSourceWithMetadata,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data._internal.util import MiB, _check_import
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, Table

logger = logging.getLogger(__name__)

# Estimated decoded bytes per read task, as for Parquet. A Parquet file's size
# comes from its footer, else its read columns' bytes times 5; any other file's
# is its size on disk.
_BIN_PACKING_BYTES = env_integer("RAY_DATA_ICEBERG_BIN_PACKING_BYTES", 128 * MiB)


@DeveloperAPI
class IcebergDatasourceV2(DataSourceWithMetadata[FileManifest]):
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
            # pyrefly: ignore[missing-argument]  # pydantic fills ``root``.
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
        self._bin_packing_bytes = (
            bin_packing_bytes if bin_packing_bytes is not None else _BIN_PACKING_BYTES
        )
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
        """The snapshot every task reads, resolved once on the driver.

        Every task gets the table metadata loaded here, so listing at
        execution time still plans against this snapshot, whatever was written
        since. ``None`` means the table has no snapshot at all, i.e. nothing
        has been written to it yet.
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
        # task's input. The indexer ignores it -- the table's metadata, not a
        # path set, is what says which files to read -- so this is a label,
        # and it is the label lineage tracking shows for the read.
        return [f"iceberg://{self._table_identifier}"]

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # ``include_field_ids=False`` matches what the reader produces:
        # ``ArrowScan`` strips field ids from the batches it yields.
        return schema_to_pyarrow(self._projected_schema(), include_field_ids=False)

    def _get_file_indexer(
        self, schema: Optional[pa.Schema] = None
    ) -> IcebergFileIndexer:
        table = self.table
        return IcebergFileIndexer(
            table_metadata=table.metadata,
            io=table.io,
            scan_kwargs=dict(self._scan_kwargs),
            snapshot_id=self.snapshot_id,
            row_filter=self._row_filter,
            server_side_planning=ServerSidePlanning.for_table(table),
            selected_fields=self._selected_fields,
        )

    @override
    def get_file_partitioner(
        self, *, hints: Optional[PartitionHints] = None
    ) -> OnlineBinPacker:
        # Every listing row carries the file's estimated size (see
        # ``_BIN_PACKING_BYTES``), so pack against a byte budget rather than
        # estimate sizes from a sample. ``hints`` are sizing advice for an
        # estimator and have nothing to add here; ``PartitionHints`` documents
        # that a partitioner sizing from its own metadata ignores them.
        #
        # One Iceberg listing row is one whole file, a single read unit, so the
        # packer never splits a file: each bin is a set of whole files whose
        # estimated sizes sum to about the budget.
        return OnlineBinPacker(max_bin_bytes=self._bin_packing_bytes)

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
