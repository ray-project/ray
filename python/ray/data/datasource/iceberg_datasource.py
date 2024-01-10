"""
Module to read an iceberg table into a Ray Dataset, by using the Ray Datasource API.
"""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, TYPE_CHECKING
from ray.data import Dataset, ReadTask, block, read_api
from ray.data.datasource import Datasource
from ray.util.annotations import DeveloperAPI
import logging

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table, DataScan, FileScanTask, Schema

logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasource(Datasource):
    """
    Iceberg datasource to read Iceberg tables into a Ray Dataset. This module heavily uses PyIceberg to read iceberg
    tables. All the routines in this class override `ray.data.Datasource`.
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_type: str = "glue",
        row_filter: Union[str, "BooleanExpression"] = None,
        selected_fields: Tuple[str, ...] = ("*",),
        snapshot_id: Optional[int] = None,
        scan_kwargs: Optional[dict[str, str]] = None,
        catalog_kwargs: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize an IcebergDatasource.

        Args:
            table_identifier: Fully qualified table identifier (i.e., "db_name.table_name")
            catalog_type: The type of catalog to use PyIceberg with (defaults to "glue")
            row_filter: A PyIceberg BooleanExpression to use to filter the data *prior* to reading
            selected_fields: Which columns from the data to read, passed directly to PyIceberg's load functions
            snapshot_id: Optional snapshot ID for the Iceberg table
            scan_kwargs: Optional arguments to pass to PyIceberg's Table.scan() function
            catalog_kwargs: Optional arguments to use when setting up the Iceberg catalog
        """
        from pyiceberg.expressions import AlwaysTrue

        self._scan_kwargs = scan_kwargs if scan_kwargs is not None else {}

        self.table_identifier = table_identifier

        self._catalog_type = catalog_type
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}

        self._row_filter = row_filter if row_filter is not None else AlwaysTrue()
        self._selected_fields = selected_fields
        self._snapshot_id = snapshot_id

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog
        if self._catalog_type == "glue":
            # Glue catalogs have no name, and no conf options are needed as long as AWS credentials are set up
            return catalog.load_glue(name="", conf=self._catalog_kwargs)
        else:
            raise NotImplementedError(f"Catalog type {self._catalog_type} not implemented")

    def _get_table_and_data_scan(self) -> Tuple["Table", "DataScan"]:
        catalog = self._get_catalog()
        table = catalog.load_table(self.table_identifier)

        data_scan = table.scan(
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=self._snapshot_id,
            **self._scan_kwargs,
        )

        return table, data_scan

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # Get the plan files in this query
        _, data_scan = self._get_table_and_data_scan()
        return sum(task.length for task in data_scan.plan_files())

    @staticmethod
    def _distribute_tasks_into_equal_chunks(
        plan_files: Iterable["FileScanTask"], n_chunks: int
    ) -> List[List["FileScanTask"]]:
        """
        Implement a greedy knapsack algorithm to distribute the files in the scan across tasks, based on their file
        size, as evenly as possible
        """
        chunks = [list() for _ in range(n_chunks)]
        chunk_sizes = {chunk: 0 for chunk in range(n_chunks)}

        # From largest to smallest, add the plan files ot the smallest chunk one at a time
        for plan_file in sorted(plan_files, key=lambda f: f.length, reverse=True):
            smallest_chunk = min(chunk_sizes, key=chunk_sizes.get)
            chunks[smallest_chunk].append(plan_file)
            # Update the size of the chunk after adding the file to it
            chunk_sizes[smallest_chunk] += plan_file.length

        return chunks

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        from pyiceberg.io import pyarrow as pyi_pa_io

        def _get_read_task(
            tasks: Iterable["FileScanTask"],
            table_identifier: str,
            schema: "Schema",
            catalog_type: str = "glue",
        ) -> Iterable[block.Block]:
            # Closure so we can pass this callable as an argument to ReadTask

            # Both the catalog and tbl attributes cannot be pickled, which means they must be instantiated within
            # this function (as opposed to being attributes of the IcebergDatasource class)
            catalog = self._get_catalog()
            tbl = catalog.load_table(table_identifier)

            for task in tasks:
                # Use the PyIceberg API to read only a single task (specifically, a FileScanTask) - note that this is
                # not as simple as reading a single parquet file, as there might be delete files, etc. associated, so we
                # must use the PyIceberg API for the projection.
                yield pyi_pa_io.project_table(
                    tasks=[task],
                    table=tbl,
                    row_filter=self._row_filter,
                    projected_schema=schema,
                    case_sensitive=self._scan_kwargs.get("case_sensitive", True),
                    limit=self._scan_kwargs.get("limit"),
                )

        # Get the PyIceberg scan
        _, data_scan = self._get_table_and_data_scan()

        # Get the plan files in this query
        plan_files = data_scan.plan_files()

        # Get the schema project for this scan, given all the row filters, snapshot ID, etc.
        projected_schema = data_scan.projection()
        # Get the arrow schema, to set in the metadata
        pya_schema = pyi_pa_io.schema_to_pyarrow(projected_schema)

        read_tasks = []
        # Chunk the plan files based on the requested parallelism
        for chunk_tasks in IcebergDatasource._distribute_tasks_into_equal_chunks(plan_files, parallelism):
            metadata = block.BlockMetadata(
                num_rows=None,
                size_bytes=sum(task.length for task in chunk_tasks),
                schema=pya_schema,
                input_files=[task.file.file_path for task in chunk_tasks],
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    read_fn=lambda tasks=chunk_tasks: _get_read_task(
                        tasks=tasks,
                        table_identifier=self.table_identifier,
                        schema=projected_schema,
                        catalog_type=self._catalog_type,
                    ),
                    metadata=metadata,
                )
            )

        return read_tasks



