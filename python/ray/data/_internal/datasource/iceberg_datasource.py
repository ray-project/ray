"""
Module to read an iceberg table into a Ray Dataset, by using the Ray Datasource API.
"""

import heapq
import itertools
import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile, DataFileContent
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, FileScanTask, Table
    from pyiceberg.table.metadata import TableMetadata

logger = logging.getLogger(__name__)


def _get_read_task(
    tasks: Iterable["FileScanTask"],
    table_io: "FileIO",
    table_metadata: "TableMetadata",
    row_filter: "BooleanExpression",
    case_sensitive: bool,
    limit: Optional[int],
    schema: "Schema",
) -> Iterable[Block]:
    from pyiceberg.io import pyarrow as pyi_pa_io

    # Use the PyIceberg API to read only a single task (specifically, a
    # FileScanTask) - note that this is not as simple as reading a single
    # parquet file, as there might be delete files, etc. associated, so we
    # must use the PyIceberg API for the projection.
    yield pyi_pa_io.project_table(
        tasks=tasks,
        table_metadata=table_metadata,
        io=table_io,
        row_filter=row_filter,
        projected_schema=schema,
        case_sensitive=case_sensitive,
        limit=limit,
    )


@DeveloperAPI
class IcebergDatasource(Datasource):
    """
    Iceberg datasource to read Iceberg tables into a Ray Dataset. This module heavily
    uses PyIceberg to read iceberg tables. All the routines in this class override
    `ray.data.Datasource`.
    """

    def __init__(
        self,
        table_identifier: str,
        row_filter: Union[str, "BooleanExpression"] = None,
        selected_fields: Tuple[str, ...] = ("*",),
        snapshot_id: Optional[int] = None,
        scan_kwargs: Optional[Dict[str, Any]] = None,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize an IcebergDatasource.

        Args:
            table_identifier: Fully qualified table identifier (i.e.,
                "db_name.table_name")
            row_filter: A PyIceberg BooleanExpression to use to filter the data *prior*
                 to reading
            selected_fields: Which columns from the data to read, passed directly to
                PyIceberg's load functions
            snapshot_id: Optional snapshot ID for the Iceberg table
            scan_kwargs: Optional arguments to pass to PyIceberg's Table.scan()
                function
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
        """
        _check_import(self, module="pyiceberg", package="pyiceberg")
        from pyiceberg.expressions import AlwaysTrue

        self._scan_kwargs = scan_kwargs if scan_kwargs is not None else {}
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self.table_identifier = table_identifier

        self._row_filter = row_filter if row_filter is not None else AlwaysTrue()
        self._selected_fields = selected_fields

        if snapshot_id:
            self._scan_kwargs["snapshot_id"] = snapshot_id

        self._plan_files = None
        self._table = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    @property
    def table(self) -> "Table":
        """
        Return the table reference from the catalog
        """
        if self._table is None:
            catalog = self._get_catalog()
            self._table = catalog.load_table(self.table_identifier)
        return self._table

    @property
    def plan_files(self) -> List["FileScanTask"]:
        """
        Return the plan files specified by this query
        """
        # Calculate and cache the plan_files if they don't already exist
        if self._plan_files is None:
            data_scan = self._get_data_scan()
            self._plan_files = data_scan.plan_files()

        return self._plan_files

    def _get_data_scan(self) -> "DataScan":

        data_scan = self.table.scan(
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            **self._scan_kwargs,
        )

        return data_scan

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # Approximate the size by using the plan files - this will not
        # incorporate the deletes, but that's a reasonable approximation
        # task
        return sum(task.file.file_size_in_bytes for task in self.plan_files)

    @staticmethod
    def _distribute_tasks_into_equal_chunks(
        plan_files: Iterable["FileScanTask"], n_chunks: int
    ) -> List[List["FileScanTask"]]:
        """
        Implement a greedy knapsack algorithm to distribute the files in the scan
        across tasks, based on their file size, as evenly as possible
        """
        chunks = [list() for _ in range(n_chunks)]

        chunk_sizes = [(0, chunk_id) for chunk_id in range(n_chunks)]
        heapq.heapify(chunk_sizes)

        # From largest to smallest, add the plan files to the smallest chunk one at a
        # time
        for plan_file in sorted(
            plan_files, key=lambda f: f.file.file_size_in_bytes, reverse=True
        ):
            smallest_chunk = heapq.heappop(chunk_sizes)
            chunks[smallest_chunk[1]].append(plan_file)
            heapq.heappush(
                chunk_sizes,
                (
                    smallest_chunk[0] + plan_file.file.file_size_in_bytes,
                    smallest_chunk[1],
                ),
            )

        return chunks

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        from pyiceberg.io import pyarrow as pyi_pa_io

        # Get the PyIceberg scan
        data_scan = self._get_data_scan()
        # Get the plan files in this query
        plan_files = self.plan_files

        # Get the projected schema for this scan, given all the row filters,
        # snapshot ID, etc.
        projected_schema = data_scan.projection()
        # Get the arrow schema, to set in the metadata
        pya_schema = pyi_pa_io.schema_to_pyarrow(projected_schema)

        # Set the n_chunks to the min of the number of plan files and the actual
        # requested n_chunks, so that there are no empty tasks
        if parallelism > len(list(plan_files)):
            parallelism = len(list(plan_files))
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the"
                "number of files"
            )

        # Get required properties for reading tasks - table IO, table metadata,
        # row filter, case sensitivity,limit and projected schema to pass
        # them directly to `_get_read_task` to avoid capture of `self` reference
        # within the closure carrying substantial overhead invoking these tasks
        #
        # See https://github.com/ray-project/ray/issues/49107 for more context
        table_io = self.table.io
        table_metadata = self.table.metadata
        row_filter = self._row_filter
        case_sensitive = self._scan_kwargs.get("case_sensitive", True)
        limit = self._scan_kwargs.get("limit")

        get_read_task = partial(
            _get_read_task,
            table_io=table_io,
            table_metadata=table_metadata,
            row_filter=row_filter,
            case_sensitive=case_sensitive,
            limit=limit,
            schema=projected_schema,
        )

        read_tasks = []
        # Chunk the plan files based on the requested parallelism
        for chunk_tasks in IcebergDatasource._distribute_tasks_into_equal_chunks(
            plan_files, parallelism
        ):
            unique_deletes: Set[DataFile] = set(
                itertools.chain.from_iterable(
                    [task.delete_files for task in chunk_tasks]
                )
            )
            # Get a rough estimate of the number of deletes by just looking at
            # position deletes. Equality deletes are harder to estimate, as they
            # can delete multiple rows.
            position_delete_count = sum(
                delete.record_count
                for delete in unique_deletes
                if delete.content == DataFileContent.POSITION_DELETES
            )
            metadata = BlockMetadata(
                num_rows=sum(task.file.record_count for task in chunk_tasks)
                - position_delete_count,
                size_bytes=sum(task.length for task in chunk_tasks),
                schema=pya_schema,
                input_files=[task.file.file_path for task in chunk_tasks],
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    read_fn=lambda tasks=chunk_tasks: get_read_task(tasks),
                    metadata=metadata,
                )
            )

        return read_tasks
