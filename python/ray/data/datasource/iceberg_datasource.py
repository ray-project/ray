"""
Module to read an iceberg table into a Ray Dataset, by using the Ray Datasource API.
"""

# -----------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import logging
import numpy as np
from pyiceberg import (
    catalog as pyi_catalog,
    expressions as pyi_expr,
    table as pyi_table,
)
from pyiceberg.io import pyarrow as pyi_pa_io
from ray.data import Dataset, ReadTask, block, read_api
from ray.data.datasource import Datasource


# -----------------------------------------------------------------------------
# CLASSES
# -----------------------------------------------------------------------------
class IcebergDatasource(Datasource):
    """
    Iceberg datasource to read Iceberg tables into a Ray Dataset. This module heavily uses PyIceberg to read iceberg
    tables. All the routines in this class override `ray.data.Datasource`.
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_type: str = "glue",
        row_filter: Union[str, pyi_expr.BooleanExpression] = pyi_expr.AlwaysTrue,
        selected_fields: Tuple[str, ...] = ("*",),
        snapshot_id: Optional[int] = None,
        scan_kwargs: Optional[dict[str, str]] = None,
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
        """
        self.scan_kwargs = scan_kwargs if scan_kwargs is not None else {}

        if "limit" in self.scan_kwargs:
            logging.warning(
                "Row limit was specified for Iceberg Datasource. Please note that this limit is applied "
                "per-file, and not to the result as a whole"
            )

        self.table_identifier = table_identifier

        self._catalog_type = catalog_type

        self._row_filter = row_filter
        self._selected_fields = selected_fields
        self._snapshot_id = snapshot_id

    def _get_table_and_data_scan(self) -> Tuple[pyi_table.Table, pyi_table.DataScan]:
        # Glue catalogs have no name, and no conf options are needed as long as AWS credentials are set up
        catalog = pyi_catalog.load_glue(name="", conf={})
        table = catalog.load_table(self.table_identifier)

        data_scan = table.scan(
            row_filter=self._row_filter,
            selected_fields=self._selected_fields,
            snapshot_id=self._snapshot_id,
            **self.scan_kwargs,
        )

        return table, data_scan

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # Get the plan files in this query
        _, data_scan = self._get_table_and_data_scan()
        return sum(task.length for task in data_scan.plan_files())

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        def _get_read_task(
            tasks: Iterable[pyi_table.FileScanTask],
            table_identifier: str,
            schema: pyi_table.Schema,
            catalog_type: str = "glue",
        ) -> Iterable[block.Block]:
            # Closure so we can pass this callable as an argument to ReadTask

            # Both the catalog and tbl attributes cannot be pickled, which means they must be instantiated within
            # this function (as opposed to being attributes of the IcebergDatasource class)
            if catalog_type == "glue":
                catalog = pyi_catalog.load_glue(name="", conf={})
            else:
                raise NotImplementedError(f"Unidentified catalog {catalog_type}")
            tbl = catalog.load_table(table_identifier)

            for task in tasks:
                yield pyi_pa_io.project_table(
                    tasks=[task],
                    table=tbl,
                    row_filter=self._row_filter,
                    projected_schema=schema,
                    case_sensitive=self.scan_kwargs.get("case_sensitive", True),
                    limit=self.scan_kwargs.get("limit"),
                )

        # Get the PyIceberg scan
        _, data_scan = self._get_table_and_data_scan()

        # Get the plan files in this query
        plan_files = data_scan.plan_files()

        # Get the schema project for this scan, given all the row filters, snapshot, etc.
        projected_schema = data_scan.projection()
        # Get the arrow schema, to set in the metadata
        pya_schema = pyi_pa_io.schema_to_pyarrow(projected_schema)

        read_tasks = []
        # Chunk the plan files based on the requested parallelism
        for chunk_tasks in np.array_split(plan_files, parallelism):
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


# -----------------------------------------------------------------------------
# FUNCTIONS
# -----------------------------------------------------------------------------
def read_iceberg(
    *,
    table_identifier: str,
    row_filter: Union[str, pyi_expr.BooleanExpression] = pyi_expr.AlwaysTrue,
    catalog_type: str = "glue",
    parallelism: int = -1,
    selected_fields: Tuple[str, ...] = ("*",),
    snapshot_id: Optional[int] = None,
    scan_kwargs: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Dataset:
    """
    Read an Iceberg table into a `ray.data.Dataset` object. This function creates an IcebergDatasource object, which is
    then passed to Ray's read_api functionality.

    Args:
        table_identifier: Fully qualified table identifier (i.e., "db_name.table_name")
        catalog_type: The type of catalog to use PyIceberg with (defaults to "glue")
        parallelism: Degree of parallelism to use for the Dataset. Please consult ray.data.read_api for details
        row_filter: A PyIceberg BooleanExpression to use to filter the data *prior* to reading
        selected_fields: Which columns from the data to read, passed directly to PyIceberg's load functions
        snapshot_id: Optional snapshot ID for the Iceberg table
        scan_kwargs: Optional arguments to pass to PyIceberg's Table.scan() function (e.g., case_sensitive, limit, etc.)
        ray_remote_args: Optional arguments to pass to `ray.remote` in the read tasks

    Returns:
        dataset: A Ray dataset (of type `ray.data.Dataset`) read off the Iceberg table
    """
    # Setup the Datasource
    datasource = IcebergDatasource(
        table_identifier=table_identifier,
        catalog_type=catalog_type,
        row_filter=row_filter,
        selected_fields=selected_fields,
        snapshot_id=snapshot_id,
        scan_kwargs=scan_kwargs,
    )

    dataset = read_api.read_datasource(
        datasource=datasource,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
    )

    return dataset
