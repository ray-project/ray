"""``FileIndexer`` that lists an Iceberg table by asking PyIceberg to plan it."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    FileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer, FileInfo
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.iceberg_manifest import (
    manifest_from_scan_tasks,
)

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.table import FileScanTask, Table

    from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Scan tasks encoded into one manifest block. This is a batching detail, not a
# task-granularity decision -- ``IcebergFilePartitioner`` regroups these rows
# into read units. It exists because ``manifest_from_scan_tasks`` builds ten
# Arrow arrays per call, so encoding one file at a time would pay that per file
# and hand the partitioner one Arrow table per file.
_LISTING_BATCH_FILES = env_integer("RAY_DATA_ICEBERG_LISTING_BATCH_FILES", 1024)


class IcebergFileIndexer(FileIndexer):
    """Lists an Iceberg table's data files, one manifest row per file.

    PyIceberg's ``plan_files()`` replaces the directory walk a file-based
    indexer would do: it reads the table's manifests, prunes partitions and
    files against the row filter, and returns one ``FileScanTask`` per
    surviving data file, already carrying that file's delete files. All of the
    input this needs is metadata, so listing does no data IO.

    One row is one surviving data file. Grouping those rows into read units is
    ``IcebergFilePartitioner``'s job, which is also what makes listing a single
    task: that partitioner requires global input.
    """

    def __init__(
        self,
        *,
        table_identifier: str,
        catalog_name: str,
        catalog_kwargs: Dict[str, Any],
        scan_kwargs: Dict[str, Any],
        snapshot_id: Optional[int],
        row_filter: "BooleanExpression",
    ):
        self._table_identifier = table_identifier
        self._catalog_name = catalog_name
        self._catalog_kwargs = catalog_kwargs
        self._scan_kwargs = scan_kwargs
        self._snapshot_id = snapshot_id
        self._row_filter = row_filter

    @property
    def file_chunker(self) -> FileChunker:
        # A read unit is always whole files: an Iceberg scan task names a file,
        # never a byte range or row-group range within one.
        return WholeFileChunker()

    def as_whole_file_indexer(self) -> "IcebergFileIndexer":
        """This indexer, unchanged -- it already emits each file exactly once.

        The base class defaults to ``None`` because an indexer may emit one
        manifest row per *chunk* of a file, which would over-count it: that is
        the footer-based Parquet indexer, where one file's row groups become
        several rows. An Iceberg scan task names a whole file and is never
        split, so one file is always one row here. The other half of the
        default -- "does no per-file IO while listing" -- holds too: planning
        reads table metadata only.
        """
        return self

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
    ) -> Iterable[FileManifest]:
        """Plan the table and yield the surviving files, one row each.

        ``paths`` and ``pruners`` are ignored: the table identifier, not a path
        set, says what to list, and which files survive is PyIceberg's decision.
        ``predicate`` is the filter the optimizer pushed onto the read; it is
        ANDed into the scan's row filter, which is what lets partition and file
        pruning happen here instead of on already-listed files. ``limit`` and
        ``projected_columns`` are ignored: stopping the plan early once a
        limit's worth of rows has been listed, and sizing read units by
        projected columns rather than whole files, are both refinements on top
        of this. ``shuffle_config`` and ``execution_idx`` are ignored too --
        shuffling exists to spread contention over a directory listing, and
        there is no directory listing here.
        """
        table = self._load_table()
        batch: List["FileScanTask"] = []
        for task in self._plan_files(table, predicate):
            batch.append(task)
            if len(batch) >= _LISTING_BATCH_FILES:
                yield manifest_from_scan_tasks(batch, table.metadata)
                batch = []
        if batch:
            yield manifest_from_scan_tasks(batch, table.metadata)

    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        """Paths and sizes of the table's data files.

        Only used for driver-side schema sampling, which an Iceberg read skips
        (the schema comes from table metadata), so this exists to satisfy the
        interface.
        """
        for task in self._plan_files(self._load_table(), None):
            yield FileInfo(path=task.file.file_path, size=task.file.file_size_in_bytes)

    def _load_table(self) -> "Table":
        from pyiceberg import catalog as pyi_catalog

        catalog = pyi_catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)
        return catalog.load_table(self._table_identifier)

    def _plan_files(
        self, table: "Table", predicate: Optional["Expr"]
    ) -> Iterator["FileScanTask"]:
        from ray.data._internal.datasource_v2.scanners.iceberg_scanner import (
            combine_row_filter,
        )

        row_filter, residual = combine_row_filter(self._row_filter, predicate)
        assert residual is None, (
            "the scanner only reports a pushed predicate it could translate, "
            f"but this one did not translate: {residual}"
        )
        scan = table.scan(
            row_filter=row_filter,
            snapshot_id=self._snapshot_id,
            **self._scan_kwargs,
        )
        logger.debug(
            "Planning Iceberg scan of %s (snapshot %s, filter %s)",
            self._table_identifier,
            self._snapshot_id,
            row_filter,
        )
        return iter(scan.plan_files())
