"""``FileIndexer`` that lists an Iceberg table by asking PyIceberg to plan it.

Runs in the ``ListFiles`` task. It plans a ``DataScan`` from the table metadata
and ``FileIO`` the driver loaded, since the catalog's config may exist only on
the driver, and turns the planned ``FileScanTask``s into manifest blocks of up
to ``_FILES_PER_MANIFEST_BLOCK`` rows, one row per data file:

    plan_files() -> FileScanTask per data file -> manifest_from_scan_tasks
                 -> FileManifest blocks -> OnlineBinPacker -> read units

Planning reads Iceberg metadata only, never a data file. It prunes on
``read_iceberg``'s ``row_filter``; the scanner takes no Ray filters, so no
predicate reaches listing.

When the table's REST catalog plans scans on its server, the task rebuilds the
catalog from the properties the driver resolved (``ServerSidePlanning``) and
gives it to the ``DataScan``, so PyIceberg asks the server for the file list
instead of reading the manifests itself, as ``Table.scan`` does on the driver.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
)

from typing_extensions import override

from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    manifest_from_scan_tasks,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.table import FileScanTask, Table
    from pyiceberg.table.metadata import TableMetadata

    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Files encoded per manifest block. Big enough that building Arrow arrays once
# per block costs little next to planning, small enough that the packer can emit
# read units while later files are still being encoded. ``OnlineBinPacker``
# still decides the read units.
_FILES_PER_MANIFEST_BLOCK = 1024


@dataclass(frozen=True)
class ServerSidePlanning:
    """What a listing task needs to have the table's REST catalog plan the scan."""

    # The driver's catalog class, so a ``RestCatalog`` subclass plans the same way.
    catalog_class: Type["RestCatalog"]
    catalog_name: str
    # As the driver resolved them, server config included. Not new to the task:
    # ``table.io``'s properties already hold them. Never logged: they carry
    # credentials.
    catalog_properties: Dict[str, Any] = field(repr=False)
    table_identifier: Tuple[str, ...]

    @classmethod
    def for_table(cls, table: "Table") -> Optional["ServerSidePlanning"]:
        """Return how to reach the table's planning server.

        Args:
            table: A table the driver loaded.

        Returns:
            ``None`` unless the table's catalog is a REST catalog whose server
            plans scans and whose config turns that on; then listing plans
            locally.
        """
        from pyiceberg.catalog.rest import RestCatalog

        catalog = table.catalog
        # Older PyIceberg releases can't plan on the server.
        supports_server_side_planning = getattr(
            catalog, "supports_server_side_planning", None
        )
        if (
            not isinstance(catalog, RestCatalog)
            or supports_server_side_planning is None
            or not supports_server_side_planning()
        ):
            return None
        return cls(
            catalog_class=type(catalog),
            catalog_name=catalog.name,
            catalog_properties=dict(catalog.properties),
            table_identifier=table.name(),
        )

    def load_catalog(self) -> "RestCatalog":
        """Rebuild the driver's catalog, which fetches the server's config again."""
        return self.catalog_class(self.catalog_name, **self.catalog_properties)


class IcebergFileIndexer(FileIndexer):
    """Lists an Iceberg table's data files with ``plan_files()``, one row each."""

    def __init__(
        self,
        *,
        table_metadata: "TableMetadata",
        io: "FileIO",
        scan_kwargs: Dict[str, Any],
        snapshot_id: Optional[int],
        row_filter: "BooleanExpression",
        server_side_planning: Optional[ServerSidePlanning] = None,
    ):
        self._table_metadata = table_metadata
        self._io = io
        self._scan_kwargs = scan_kwargs
        self._snapshot_id = snapshot_id
        self._row_filter = row_filter
        self._server_side_planning = server_side_planning

    @override
    def as_whole_file_indexer(self) -> "IcebergFileIndexer":
        """This indexer, unchanged -- it already emits each file exactly once.

        The base class defaults to ``None`` because an indexer may emit one
        manifest row per *chunk* of a file, which would over-count it: that is
        the footer-based Parquet indexer, where one file's row groups become
        several rows. An Iceberg scan task names a whole file and is never
        split -- the manifest's chunk-metadata column is all null -- so one
        file is always one row here. The other half of the default, "does no
        per-file IO while listing", holds too: planning reads table metadata
        only.

        Returns:
            ``self``.
        """
        return self

    @override
    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
        excluded_read_unit_ids: Optional[AbstractSet[str]] = None,
    ) -> Iterable[FileManifest]:
        """Plan the table and yield the surviving files, one row each.

        ``excluded_read_unit_ids`` are file paths, since a read unit is a whole
        file. ``limit`` is applied by the reader; stopping listing early from
        Iceberg's per-file row counts is a follow-up. The other arguments don't
        apply and are ignored.
        """
        pending_tasks: List["FileScanTask"] = []
        for task in self._plan_files():
            if excluded_read_unit_ids and task.file.file_path in excluded_read_unit_ids:
                continue
            pending_tasks.append(task)
            if len(pending_tasks) >= _FILES_PER_MANIFEST_BLOCK:
                yield manifest_from_scan_tasks(tasks=pending_tasks)
                pending_tasks = []
        if pending_tasks:
            yield manifest_from_scan_tasks(tasks=pending_tasks)

    @override
    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        """Paths and sizes of the table's data files.

        Unused: this only feeds schema sampling, and the schema comes from
        table metadata.
        """
        for task in self._plan_files():
            yield FileInfo(path=task.file.file_path, size=task.file.file_size_in_bytes)

    def _plan_files(self) -> Iterator["FileScanTask"]:
        from pyiceberg.table import DataScan

        # Not ``Table.scan``: the ``Table`` stays on the driver. A ``DataScan``
        # with a catalog that plans on its server asks it for the file list.
        planning_kwargs: Dict[str, Any] = {}
        if self._server_side_planning is not None:
            planning_kwargs = {
                "catalog": self._server_side_planning.load_catalog(),
                "table_identifier": self._server_side_planning.table_identifier,
            }
        scan = DataScan(
            table_metadata=self._table_metadata,
            io=self._io,
            row_filter=self._row_filter,
            snapshot_id=self._snapshot_id,
            **planning_kwargs,
            **self._scan_kwargs,
        )
        logger.debug(
            "Planning Iceberg scan of %s (snapshot %s, filter %s)",
            self._table_metadata.location,
            self._snapshot_id,
            self._row_filter,
        )
        return iter(scan.plan_files())
