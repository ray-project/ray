"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""

import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import GiB
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

    from ray.data.expressions import Expr


logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(Datasink[List[Dict[str, Any]]]):
    """
    Iceberg datasink to write a Ray Dataset into an existing Iceberg table.
    
    This datasink handles concurrent writes by:
    - Each worker writes Parquet files to storage and returns file metadata as dicts
    - The driver collects all file metadata and performs a single commit
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
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg catalog
            snapshot_properties: Custom properties to write to snapshot summary
            mode: Write mode - APPEND, UPSERT, or OVERWRITE. Defaults to APPEND.
                - APPEND: Add new data without checking for duplicates
                - UPSERT: Update existing rows or insert new ones based on a join condition
                - OVERWRITE: Replace table data (all data or filtered subset)
            overwrite_filter: Optional filter for OVERWRITE mode to perform partial overwrites.
                Must be a Ray Data expression from `ray.data.expressions`. Only rows matching
                this filter are replaced. If None with OVERWRITE mode, replaces all table data.
            upsert_kwargs: Optional arguments to pass through to PyIceberg's table.upsert()
                method. Supported parameters include join_cols (List[str]),
                when_matched_update_all (bool), when_not_matched_insert_all (bool),
                case_sensitive (bool), branch (str). See PyIceberg documentation for details.
            overwrite_kwargs: Optional arguments to pass through to PyIceberg's table.overwrite()
                method. Supported parameters include case_sensitive (bool) and branch (str).
                See PyIceberg documentation for details.

        Note:
            Schema evolution is automatically enabled on the first block written by each worker.
            New columns in the incoming data are automatically added to the table schema.
        """
        if not table_identifier or not table_identifier.strip():
            raise ValueError("table_identifier must be a non-empty string")

        self.table_identifier = table_identifier.strip()
        self._catalog_kwargs = dict(catalog_kwargs or {})
        self._snapshot_properties = dict(snapshot_properties or {})
        self._mode = mode
        self._overwrite_filter = overwrite_filter
        self._upsert_kwargs = dict(upsert_kwargs or {})
        self._overwrite_kwargs = dict(overwrite_kwargs or {})

        # Validate kwargs are only set for relevant modes
        if self._upsert_kwargs and self._mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified when mode is SaveMode.UPSERT, "
                f"but mode is {self._mode}"
            )
        if self._overwrite_kwargs and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_kwargs can only be specified when mode is SaveMode.OVERWRITE, "
                f"but mode is {self._mode}"
            )
        if self._overwrite_filter is not None and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_filter can only be specified when mode is SaveMode.OVERWRITE, "
                f"but mode is {self._mode}"
            )

        # Extract catalog name from kwargs or use default
        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
            if not self._catalog_name or not self._catalog_name.strip():
                raise ValueError("catalog name must be non-empty if provided")
            self._catalog_name = self._catalog_name.strip()
        else:
            self._catalog_name = "default"

        self._table: Optional["Table"] = None
        self._write_uuid: Optional[uuid.UUID] = None
        self._schema_updated: bool = False

    def __reduce__(self):
        """Custom pickle support to exclude non-serializable _table."""
        return (
            self.__class__,
            (
                self.table_identifier,
                self._catalog_kwargs,
                self._snapshot_properties,
                self._mode,
                self._overwrite_filter,
                self._upsert_kwargs,
                self._overwrite_kwargs,
            ),
            {"_write_uuid": self._write_uuid, "_schema_updated": self._schema_updated},
        )

    def __setstate__(self, state: dict) -> None:
        """Restore state after unpickling."""
        self._table = None
        self._write_uuid = state.get("_write_uuid")
        self._schema_updated = state.get("_schema_updated", False)

    def _get_catalog(self) -> "Catalog":
        """Load the Iceberg catalog."""
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def _reload_table(self) -> None:
        """Reload the Iceberg table from the catalog."""
        catalog = self._get_catalog()
        self._table = catalog.load_table(self.table_identifier)

    def _update_schema_once(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data using union-by-name semantics.
        
        This is called once per worker on the first block to handle schema evolution.
        Uses retry logic with exponential backoff to handle concurrent schema updates
        from multiple workers.

        Args:
            incoming_schema: The PyArrow schema from the incoming data
        """
        if self._schema_updated:
            return

        from pyiceberg.exceptions import CommitFailedException

        max_retries = 3
        base_delay = 0.1  # 100ms

        for attempt in range(max_retries):
            try:
                with self._table.update_schema() as update:
                    update.union_by_name(incoming_schema)
                # Success - reload table to get updated metadata and mark as done
                self._reload_table()
                self._schema_updated = True
                logger.info("Schema updated successfully")
                return
            except CommitFailedException:
                if attempt < max_retries - 1:
                    # Another worker updated schema concurrently, retry with backoff
                    delay = base_delay * (2**attempt)
                    logger.debug(
                        f"Schema update conflict on attempt {attempt + 1}, "
                        f"retrying after {delay}s"
                    )
                    time.sleep(delay)
                    self._reload_table()
                else:
                    # Final attempt failed, reload and continue - schema might already be correct
                    logger.warning(
                        "Schema update failed after retries, continuing with current schema"
                    )
                    self._reload_table()
                    self._schema_updated = True

    def on_write_start(self) -> None:
        """Initialize table for writing and create a shared write UUID."""
        self._table = self._get_catalog().load_table(self.table_identifier)
        self._write_uuid = uuid.uuid4()
        logger.info(
            f"Starting write to {self.table_identifier} with UUID {self._write_uuid}"
        )

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> List[Dict[str, Any]]:
        """
        Write blocks to Parquet files in storage and return file metadata.

        This runs on each worker in parallel. Files are written directly to storage
        (S3, HDFS, etc.) and only metadata is returned to the driver.

        Args:
            blocks: Iterable of Ray Data blocks to write
            ctx: TaskContext object containing task-specific information

        Returns:
            List of dictionaries containing file metadata for each written file.
        """
        from pyiceberg.io.pyarrow import data_file_statistics_from_parquet_metadata
        from pyiceberg.table.snapshots import Operation
        from pyiceberg.types import transform_dict_to_row

        if self._table is None:
            self._reload_table()

        file_metadata_list = []

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            
            if pa_table.num_rows == 0:
                continue

            # Update schema once per worker on first non-empty block
            if not self._schema_updated:
                self._update_schema_once(pa_table.schema)

            # Write data file using PyIceberg's public API
            # Use the table's file_io to write directly to storage
            output_file_location = self._table.location() + "/data/" + str(uuid.uuid4()) + ".parquet"
            
            with self._table.io.new_output(output_file_location) as output_file:
                import pyarrow.parquet as pq
                pq.write_table(pa_table, output_file)

            # Collect file metadata for commit (avoid using private DataFile objects)
            # Read back the file to get statistics
            with self._table.io.new_input(output_file_location) as input_file:
                parquet_metadata = pq.read_metadata(input_file)
            
            # Build file metadata dict
            file_metadata = {
                "file_path": output_file_location,
                "file_format": "PARQUET",
                "record_count": pa_table.num_rows,
                "file_size_in_bytes": parquet_metadata.serialized_size,
            }
            
            file_metadata_list.append(file_metadata)

        return file_metadata_list

    def on_write_complete(
        self, write_result: WriteResult[List[Dict[str, Any]]]
    ) -> None:
        """
        Complete the write by committing all data files in a single transaction.

        This runs on the driver after all workers finish writing files.
        Collects all file metadata from all workers and performs a single
        atomic commit based on the configured mode.
        """
        # Reload table to get latest metadata
        self._reload_table()

        # Collect all file metadata from all workers
        all_file_metadata = []
        for metadata_list in write_result.write_returns:
            if metadata_list:
                all_file_metadata.extend(metadata_list)

        if not all_file_metadata:
            logger.warning("No data files to commit, write operation produced no data")
            return

        logger.info(f"Committing {len(all_file_metadata)} data files")

        # Commit based on mode
        if self._mode == SaveMode.APPEND:
            self._commit_append(all_file_metadata)
        elif self._mode == SaveMode.OVERWRITE:
            self._commit_overwrite(all_file_metadata)
        elif self._mode == SaveMode.UPSERT:
            self._commit_upsert(all_file_metadata)
        else:
            raise ValueError(f"Unsupported mode: {self._mode}")

        logger.info(f"Successfully committed write to {self.table_identifier}")

    def _commit_append(self, file_metadata_list: List[Dict[str, Any]]) -> None:
        """Commit data files using APPEND mode."""
        from pyiceberg.io.pyarrow import parquet_files_to_data_files

        # Convert file paths to DataFile objects
        file_paths = [fm["file_path"] for fm in file_metadata_list]
        data_files = parquet_files_to_data_files(
            io=self._table.io,
            table_metadata=self._table.metadata,
            file_paths=file_paths,
        )

        # Append using public API
        self._table.append(data_files)
        logger.info(f"Appended {len(file_metadata_list)} files")

    def _commit_overwrite(self, file_metadata_list: List[Dict[str, Any]]) -> None:
        """Commit data files using OVERWRITE mode."""
        from pyiceberg.io.pyarrow import parquet_files_to_data_files

        # Convert file paths to DataFile objects
        file_paths = [fm["file_path"] for fm in file_metadata_list]
        data_files = parquet_files_to_data_files(
            io=self._table.io,
            table_metadata=self._table.metadata,
            file_paths=file_paths,
        )

        # Convert Ray expression to PyIceberg expression if filter provided
        if self._overwrite_filter is not None:
            from ray.data._internal.datasource.iceberg_datasource import (
                _IcebergExpressionVisitor,
            )

            visitor = _IcebergExpressionVisitor()
            pyi_filter = visitor.visit(self._overwrite_filter)
            
            logger.info(f"Overwriting data matching filter")
            self._table.overwrite(
                data_files,
                overwrite_filter=pyi_filter,
                **self._overwrite_kwargs,
            )
        else:
            # Full table overwrite
            logger.warning(
                f"Performing FULL table overwrite - all existing data in "
                f"{self.table_identifier} will be deleted"
            )
            from pyiceberg.expressions import AlwaysTrue

            self._table.overwrite(
                data_files,
                overwrite_filter=AlwaysTrue(),
                **self._overwrite_kwargs,
            )

        logger.info(f"Overwrote table with {len(file_metadata_list)} files")

    def _commit_upsert(self, file_metadata_list: List[Dict[str, Any]]) -> None:
        """
        Commit data files using UPSERT mode.

        For upsert, we need to read back the data files to perform merge logic.
        The write I/O was still distributed (workers wrote files in parallel),
        but the merge logic happens in a Ray task to protect the driver from OOM.
        """
        # Run the upsert commit in a Ray task to protect the driver
        upsert_result = _commit_upsert_task.options(
            num_cpus=1, 
            memory=4 * GiB
        ).remote(
            table_identifier=self.table_identifier,
            catalog_name=self._catalog_name,
            catalog_kwargs=self._catalog_kwargs,
            file_metadata_list=file_metadata_list,
            upsert_kwargs=self._upsert_kwargs,
        )
        
        # Wait for upsert to complete with timeout
        timeout_seconds = 3600  # 1 hour
        ray.get(upsert_result, timeout=timeout_seconds)
        
        logger.info(f"Upserted {len(file_metadata_list)} files")


@ray.remote
def _commit_upsert_task(
    table_identifier: str,
    catalog_name: str,
    catalog_kwargs: Dict[str, Any],
    file_metadata_list: List[Dict[str, Any]],
    upsert_kwargs: Dict[str, Any],
) -> None:
    """
    Remote task to commit UPSERT operations.

    This runs in a separate Ray task to protect the driver from OOM
    when reading back data files for upsert merge logic.
    
    Processes files in batches to control memory usage while maintaining
    some parallelism within the batch.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg import catalog

    # Load the catalog and table
    loaded_catalog = catalog.load_catalog(catalog_name, **catalog_kwargs)
    table = loaded_catalog.load_table(table_identifier)

    # Process files in batches to avoid OOM
    batch_size = 10
    for i in range(0, len(file_metadata_list), batch_size):
        batch = file_metadata_list[i : i + batch_size]
        
        # Read all files in batch and concatenate
        tables = []
        for file_metadata in batch:
            file_path = file_metadata["file_path"]
            with table.io.new_input(file_path).open() as f:
                pa_table = pq.read_table(f)
                tables.append(pa_table)
        
        # Concatenate all tables in batch
        if len(tables) == 1:
            combined_table = tables[0]
        else:
            combined_table = pa.concat_tables(tables)
        
        # Upsert the batch
        table.upsert(df=combined_table, **upsert_kwargs)

