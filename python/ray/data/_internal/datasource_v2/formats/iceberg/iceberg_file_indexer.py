"""``FileIndexer`` that lists an Iceberg table by asking PyIceberg to plan it.

Runs in the ``ListFiles`` task. It plans a ``DataScan`` from the table metadata
and ``FileIO`` the driver loaded, since the catalog's config may exist only on
the driver, and turns the planned ``FileScanTask``s into manifest blocks of up
to ``_FILES_PER_MANIFEST_BLOCK`` rows, one row per data file:

    plan_files() -> FileScanTask per data file -> manifest_from_scan_tasks
                 -> FileManifest blocks -> OnlineBinPacker -> read units

Each row is sized by the columns the read decodes (``projected_columns``, else
the scan's projection), so a narrow read of a wide table packs many files per
read task. A Parquet file's size is its decoded bytes, estimated from its footer
by Parquet's footer indexer, which also gives the memory decoding it takes
(``iceberg_footer_sizing``); any other file is sized from Iceberg's per-column
sizes.

Planning reads Iceberg metadata only. It prunes on ``read_iceberg``'s
``row_filter`` ANDed with the predicate the scanner accepted, in the planning
form ``iceberg_filter_pushdown`` gives it. Each row is marked ``fully_matched``
when ``iceberg_row_match`` proves every row of the file passes that filter, and
listing stops once those files hold the pushed limit's rows, as Parquet's
footer indexer does. The listed Parquet files' footers are read once planning
is done.

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
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

from typing_extensions import override

from ray.data._internal.datasource_v2.formats.iceberg.iceberg_filter_pushdown import (
    split_predicate,
    to_iceberg_filter,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_footer_sizing import (
    IcebergFooterSizing,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    manifest_from_scan_tasks,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_row_match import (
    RowMatchCheck,
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
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, FileScanTask, Table
    from pyiceberg.table.metadata import TableMetadata

    from ray.data._internal.datasource_v2.formats.iceberg.iceberg_footer_sizing import (  # noqa: E501
        DecodedSize,
    )
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
        selected_fields: Tuple[str, ...] = ("*",),
        reads_footers: bool = True,
        server_side_planning: Optional[ServerSidePlanning] = None,
    ):
        self._table_metadata = table_metadata
        self._selected_fields = selected_fields
        self._io = io
        self._scan_kwargs = scan_kwargs
        self._snapshot_id = snapshot_id
        self._row_filter = row_filter
        self._server_side_planning = server_side_planning
        # Whether listing may read Parquet footers.
        self._reads_footers = reads_footers

    @override
    def as_whole_file_indexer(self) -> "IcebergFileIndexer":
        """This indexer without the footer reads, so listing reads metadata only.

        The count rule needs one row per file and no per-file IO while listing.
        An Iceberg scan task names a whole file and is never split, so one file
        is always one row here; only the Parquet footer reads that size a file
        are per-file IO, and the copy skips them.

        Returns:
            A copy that sizes every file from Iceberg's per-column sizes.
        """
        return IcebergFileIndexer(
            table_metadata=self._table_metadata,
            io=self._io,
            scan_kwargs=self._scan_kwargs,
            snapshot_id=self._snapshot_id,
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            reads_footers=False,
            server_side_planning=self._server_side_planning,
        )

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
        file. ``predicate`` is the scanner's accepted predicate, planned with
        ``read_iceberg``'s ``row_filter``. ``projected_columns`` sizes each
        row, and with ``reads_footers`` its Parquet files' footers are read to
        do so. ``limit`` stops listing once the ``fully_matched`` files listed
        so far hold that many rows; ``plan_files()`` still plans the whole
        table first. The other arguments don't apply and are ignored.
        """
        scan, filters_after_read = self._data_scan(predicate)
        field_ids = _projected_field_ids(
            projection=scan.projection(),
            columns=projected_columns,
            case_sensitive=scan.case_sensitive,
        )
        row_match = RowMatchCheck(
            schema=self._table_metadata.schema(),
            row_filter=scan.row_filter,
            case_sensitive=scan.case_sensitive,
            filters_after_read=filters_after_read,
        )
        tasks: List["FileScanTask"] = []
        matched: List[bool] = []
        matched_rows = 0
        for task in scan.plan_files():
            if excluded_read_unit_ids and task.file.file_path in excluded_read_unit_ids:
                continue
            tasks.append(task)
            matched.append(row_match.every_row_matches(task))
            if matched[-1]:
                matched_rows += task.file.record_count
            if limit is not None and matched_rows >= limit:
                # Only exact counts are summed, so the files listed so far
                # hold at least ``limit`` rows.
                break
        decoded_sizes = self._decoded_sizes(scan, projected_columns, tasks)
        for start in range(0, len(tasks), _FILES_PER_MANIFEST_BLOCK):
            end = start + _FILES_PER_MANIFEST_BLOCK
            yield manifest_from_scan_tasks(
                tasks=tasks[start:end],
                projected_field_ids=field_ids,
                decoded_sizes=decoded_sizes,
                fully_matched=matched[start:end],
            )

    def _decoded_sizes(
        self,
        scan: "DataScan",
        projected_columns: Optional[List[str]],
        tasks: List["FileScanTask"],
    ) -> Dict[str, "DecodedSize"]:
        """The footer-read sizes of ``tasks``' Parquet files, by path."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        if not self._reads_footers:
            return {}
        projection = scan.projection()
        if projected_columns is not None:
            projection = projection.select(
                *projected_columns, case_sensitive=scan.case_sensitive
            )
        sizing = IcebergFooterSizing.create(
            table_metadata=self._table_metadata,
            io=self._io,
            case_sensitive=scan.case_sensitive,
            read_schema=schema_to_pyarrow(projection, include_field_ids=False),
        )
        if sizing is None:
            return {}
        footer_tasks = [task for task in tasks if sizing.file_path(task) is not None]
        return sizing.size_files(footer_tasks) if footer_tasks else {}

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
        scan, _ = self._data_scan()
        for task in scan.plan_files():
            yield FileInfo(path=task.file.file_path, size=task.file.file_size_in_bytes)

    def _data_scan(self, predicate: Optional["Expr"] = None) -> Tuple["DataScan", bool]:
        """The scan to plan, and whether part of ``predicate`` runs after the read."""
        from pyiceberg.table import DataScan

        row_filter = self._row_filter
        filters_after_read = False
        if predicate is not None:
            schema = self._table_metadata.schema()
            case_sensitive = self._scan_kwargs.get("case_sensitive", True)
            parts = split_predicate(
                predicate, schema=schema, case_sensitive=case_sensitive
            )
            assert not parts.residual, f"the scanner never accepts {parts.residual}"
            row_filter = to_iceberg_filter(
                row_filter, parts.scan, schema=schema, case_sensitive=case_sensitive
            )
            row_filter = to_iceberg_filter(
                row_filter,
                parts.after_read,
                schema=schema,
                case_sensitive=case_sensitive,
                nan_safe=True,
            )
            filters_after_read = bool(parts.after_read)
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
            row_filter=row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=self._snapshot_id,
            **planning_kwargs,
            **self._scan_kwargs,
        )
        logger.debug(
            "Planning Iceberg scan of %s (snapshot %s, filter %s)",
            self._table_metadata.location,
            self._snapshot_id,
            row_filter,
        )
        return scan, filters_after_read


def _projected_field_ids(
    *, projection: "Schema", columns: Optional[List[str]], case_sensitive: bool
) -> Optional[Set[int]]:
    """Top-level field ids the read decodes; ``None`` sizes the whole file."""
    if columns is None:
        return {field.field_id for field in projection.columns}
    if not columns:
        # ``count()`` reads no columns. Sizing its files at zero would pack the
        # whole table into one read task, so size them as whole files instead.
        return None
    return {
        projection.find_field(name, case_sensitive=case_sensitive).field_id
        for name in columns
    }
