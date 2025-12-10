"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table

logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(
    Datasink[Tuple[List["DataFile"], List["pa.Schema"]]]
):  # Changed return type
    """
    Iceberg datasink to write a Ray Dataset into an existing Iceberg table.
    This datasink handles concurrent writes by:
    - Each worker writes Parquet files to storage and returns DataFile metadata
    - The driver collects all DataFile objects and performs a single commit

    Schema evolution is supported:
    - New columns in incoming data are automatically added to the table schema
    - Type promotion across blocks is handled via schema reconciliation on the driver
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg catalog
            snapshot_properties: Custom properties to write to snapshot summary

        Note:
            Schema evolution is automatically enabled. New columns in the incoming data
            are automatically added to the table schema. The schema is extracted from
            the first input bundle when on_write_start is called.
        """
        self.table_identifier = table_identifier
        self._catalog_kwargs = (catalog_kwargs or {}).copy()
        self._snapshot_properties = (snapshot_properties or {}).copy()

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self._table: "Table" = None

    def __getstate__(self) -> dict:
        """Exclude `_table` during pickling."""
        state = self.__dict__.copy()
        state.pop("_table", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._table = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def _reload_table(self) -> None:
        """Reload the Iceberg table from the catalog."""
        catalog = self._get_catalog()
        self._table = catalog.load_table(self.table_identifier)

    def _update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data using union-by-name semantics.

        .. warning::
            This method must only be called from the driver process.
            It performs schema evolution which requires exclusive table access.

        Args:
            incoming_schema: The PyArrow schema to merge with the table schema
        """
        with self._table.update_schema() as update:
            update.union_by_name(incoming_schema)
        # Succeeded, reload to get latest table version and exit.
        self._reload_table()

    def _append_and_commit(
        self, txn: "Table.transaction", data_files: List["DataFile"]
    ) -> None:
        """Append data files to a transaction and commit.

        Args:
            txn: PyIceberg transaction object
            data_files: List of DataFile objects to append
        """
        with txn._append_snapshot_producer(self._snapshot_properties) as append_files:
            for data_file in data_files:
                append_files.append_data_file(data_file)
        txn.commit_transaction()

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        """Initialize table for writing and create a shared write UUID.

        Args:
            schema: The PyArrow schema of the data being written. This is
                automatically extracted from the first input bundle by the
                Write operator. Used to evolve the table schema before writing
                to avoid PyIceberg name mapping errors.
        """
        self._reload_table()

        # Evolve schema BEFORE any files are written
        # This prevents PyIceberg name mapping errors when incoming data has new columns
        if schema is not None:
            self._update_schema(schema)

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> Tuple[List["DataFile"], List["pa.Schema"]]:
        """
        Write blocks to Parquet files in storage and return DataFile metadata with schemas.

        This runs on each worker in parallel. Files are written directly to storage
        (S3, HDFS, etc.) and only metadata is returned to the driver.
        Schema updates are NOT performed here - they happen on the driver.

        Args:
            blocks: Iterable of Ray Data blocks to write
            ctx: TaskContext object containing task-specific information

        Returns:
            Tuple of (List of DataFile objects, List of PyArrow schemas from all non-empty blocks).
        """
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        # Workers receive a pickled datasink with _table=None (excluded during
        # serialization), so we reload it on first use.
        if self._table is None:
            self._reload_table()

        all_data_files = []
        block_schemas = []

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                block_schemas.append(pa_table.schema)

                # Write data files to storage
                data_files = list(
                    _dataframe_to_data_files(
                        table_metadata=self._table.metadata,
                        df=pa_table,
                        io=self._table.io,
                    )
                )
                all_data_files.extend(data_files)

        return (all_data_files, block_schemas)

    def on_write_complete(self, write_result: WriteResult) -> None:
        """
        Complete the write by reconciling schemas and committing all data files.

        This runs on the driver after all workers finish writing files.
        Collects all DataFile objects and schemas from all workers, reconciles schemas
        (allowing type promotion), updates table schema if needed, then performs a single
        atomic commit.
        """
        # Reload table to get latest metadata
        self._reload_table()

        # Collect all data files and schemas from all workers
        all_data_files = []
        all_schemas = []

        for write_return in write_result.write_returns:
            if not write_return:
                continue

            data_files, schemas = write_return
            if data_files:  # Only add schema if we have data files
                all_data_files.extend(data_files)
                all_schemas.extend(schemas)

        if not all_data_files:
            return

        # Reconcile all schemas from all blocks across all workers
        # Get table schema and union with reconciled schema using unify_schemas with promotion
        from pyiceberg.io import pyarrow as pyi_pa_io

        from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

        table_schema = pyi_pa_io.schema_to_pyarrow(self._table.schema())
        final_reconciled_schema = unify_schemas(
            [table_schema] + all_schemas, promote_types=True
        )

        # Create transaction and commit schema update + data files atomically
        txn = self._table.transaction()

        # Update table schema within the transaction if it differs
        if not final_reconciled_schema.equals(table_schema):
            with txn.update_schema() as update:
                update.union_by_name(final_reconciled_schema)

        self._append_and_commit(txn, all_data_files)
