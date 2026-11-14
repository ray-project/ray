"""``FileIndexer`` that lists an Iceberg table by asking PyIceberg to plan it."""

from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
)

from typing_extensions import override

from ray._common.utils import env_integer
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
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata

    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Files encoded per manifest block, to build Arrow arrays once per block rather
# than once per file. ``OnlineBinPacker`` still decides the read units.
_FILES_PER_MANIFEST_BLOCK = env_integer(
    "RAY_DATA_ICEBERG_FILES_PER_MANIFEST_BLOCK", 1024
)


class IcebergFileIndexer(FileIndexer):
    """Lists an Iceberg table's data files with ``plan_files()``, one row each.

    Plans from the table metadata the driver loaded, not from the catalog,
    whose config may exist only on the driver.
    """

    def __init__(
        self,
        *,
        table_metadata: "TableMetadata",
        io: "FileIO",
        scan_kwargs: Dict[str, Any],
        snapshot_id: Optional[int],
        row_filter: "BooleanExpression",
    ):
        self._table_metadata = table_metadata
        self._io = io
        self._scan_kwargs = scan_kwargs
        self._snapshot_id = snapshot_id
        self._row_filter = row_filter

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

        ``predicate`` is ANDed into the row filter so planning prunes files.
        ``excluded_read_unit_ids`` are file paths, since a read unit is a whole
        file. The other arguments don't apply and are ignored.
        """
        pending_tasks: List["FileScanTask"] = []
        for task in self._plan_files(predicate=predicate):
            if excluded_read_unit_ids and task.file.file_path in excluded_read_unit_ids:
                continue
            pending_tasks.append(task)
            if len(pending_tasks) >= _FILES_PER_MANIFEST_BLOCK:
                yield manifest_from_scan_tasks(
                    tasks=pending_tasks, table_metadata=self._table_metadata
                )
                pending_tasks = []
        if pending_tasks:
            yield manifest_from_scan_tasks(
                tasks=pending_tasks, table_metadata=self._table_metadata
            )

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
        for task in self._plan_files(predicate=None):
            yield FileInfo(path=task.file.file_path, size=task.file.file_size_in_bytes)

    def _plan_files(self, predicate: Optional["Expr"]) -> Iterator["FileScanTask"]:
        from pyiceberg.table import DataScan

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_scanner import (
            combine_row_filter,
        )

        row_filter, residual = combine_row_filter(
            row_filter=self._row_filter, predicate=predicate
        )
        assert residual is None, (
            "the scanner only reports a pushed predicate it could translate, "
            f"but this one did not translate: {residual}"
        )
        # Not ``Table.scan``: there is no catalog in this task.
        scan = DataScan(
            table_metadata=self._table_metadata,
            io=self._io,
            row_filter=row_filter,
            snapshot_id=self._snapshot_id,
            **self._scan_kwargs,
        )
        logger.debug(
            "Planning Iceberg scan of %s (snapshot %s, filter %s)",
            self._table_metadata.location,
            self._snapshot_id,
            row_filter,
        )
        return iter(scan.plan_files())
