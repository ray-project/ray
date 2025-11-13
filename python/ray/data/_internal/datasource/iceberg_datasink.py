"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table

    from ray.data.expressions import Expr


logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(Datasink[List["DataFile"]]):
    """
    Iceberg datasink to write a Ray Dataset into an existing Iceberg table. This module
    heavily uses PyIceberg to write to iceberg table. All the routines in this class override
    `ray.data.Datasink`.

    This datasink respects streaming semantics:
    - Data files are written incrementally during `write()` as tasks complete
    - The snapshot commit happens in `on_write_complete()` after all files are written
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
            table_identifier: The identifier of the table to read such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
            snapshot_properties: Custom properties to write to snapshot summary, such as commit metadata
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
            Schema evolution is automatically enabled. New columns in the incoming data
            are automatically added to the table schema.
        """

        self.table_identifier = table_identifier
        self._catalog_kwargs = (catalog_kwargs or {}).copy()
        self._snapshot_properties = snapshot_properties or {}
        self._mode = mode
        self._overwrite_filter = overwrite_filter
        self._upsert_kwargs = (upsert_kwargs or {}).copy()
        self._overwrite_kwargs = (overwrite_kwargs or {}).copy()

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

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self._table: "Table" = None
        self._write_uuid: uuid.UUID = None

    # Since iceberg table is not pickle-able, we need to exclude it during serialization
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

        This automatically handles:
        - Adding new columns from the incoming schema
        - Type promotion (e.g., int32 -> int64) where compatible
        - Preserving existing columns not in the incoming schema

        Args:
            incoming_schema: The PyArrow schema from the incoming data
        """
        # Use PyIceberg's update_schema API
        with self._table.update_schema() as update:
            update.union_by_name(incoming_schema)

        # Reload table completely after schema evolution
        self._reload_table()

    def on_write_start(self) -> None:
        """Initialize table for writing and set up write UUID."""
        self._reload_table()
        # Generate a unique write UUID for this write operation
        self._write_uuid = uuid.uuid4()

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> List["DataFile"]:
        """
        Write blocks as parquet files to the table location.

        Returns DataFile objects with metadata about the written files.
        This respects streaming semantics by writing files incrementally
        as tasks complete.
        """
        import itertools

        import pyarrow as pa
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        # Lazy-load table if needed (happens in worker tasks after deserialization)
        if self._table is None:
            self._reload_table()

        # Convert blocks to PyArrow tables
        tables = []
        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                tables.append(pa_table)

        if not tables:
            return []

        # Combine all tables from this task
        combined_table = pa.concat_tables(tables)

        # Ensure table schema is evolved before generating DataFiles so that
        # PyIceberg name-mapping can resolve field IDs for any new columns.
        self._update_schema(combined_table.schema)

        # Use PyIceberg's _dataframe_to_data_files which properly handles:
        # - Schema conversion with name mapping
        # - Partitioned vs unpartitioned tables
        # - Proper DataFile formatting
        # Use task_idx as counter base to ensure unique file names across tasks
        counter = itertools.count(ctx.task_idx * 10000)
        data_files = list(
            _dataframe_to_data_files(
                table_metadata=self._table.metadata,
                df=combined_table,
                io=self._table.io,
                write_uuid=self._write_uuid,
                counter=counter,
            )
        )

        return data_files

    def _collect_data_files(
        self, write_result: WriteResult[List["DataFile"]]
    ) -> List["DataFile"]:
        """Collect all DataFile objects from write results."""
        all_data_files = []
        for data_files_batch in write_result.write_returns:
            all_data_files.extend(data_files_batch)

        if not all_data_files:
            logger.warning("No data files written")
            return []

        return all_data_files

    def _read_data_files_as_table(self, data_files: List["DataFile"]) -> "pa.Table":
        """
        Read back written data files and combine them into a single PyArrow table.

        This is used for UPSERT and OVERWRITE modes where PyIceberg's APIs
        require the data as a DataFrame/Table rather than DataFile objects.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        total_rows = sum(df.record_count for df in data_files)
        logger.info(
            f"Reading {len(data_files)} data file(s) with " f"{total_rows} rows"
        )

        tables = []
        for data_file in data_files:
            # Use PyIceberg's IO to read the file
            input_file = self._table.io.new_input(data_file.file_path)
            with input_file.open() as f:
                table = pq.read_table(f)
                tables.append(table)

        return pa.concat_tables(tables)

    def _complete_append(self, data_files: List["DataFile"]) -> None:
        """
        Complete APPEND mode write using PyIceberg's fast_append API.

        This commits all the data files that were written during the write phase.
        """
        # Create a transaction and use fast_append to commit all data files in one snapshot
        transaction = self._table.transaction()
        with transaction.update_snapshot(
            snapshot_properties=self._snapshot_properties
        ).fast_append() as update:
            for data_file in data_files:
                update.append_data_file(data_file)

        # Commit the transaction
        transaction.commit_transaction()

    def _complete_upsert(self, data_files: List["DataFile"]) -> None:
        """
        Complete UPSERT mode write using PyIceberg's upsert API.

        For upsert, we need to read back the written data files and pass the
        data to PyIceberg's upsert API, which handles the merge logic.
        """
        combined_table = self._read_data_files_as_table(data_files)

        self._table.upsert(df=combined_table, **self._upsert_kwargs)

        join_cols = self._upsert_kwargs.get("join_cols")
        if join_cols:
            logger.info(
                f"Upserted {combined_table.num_rows} rows to {self.table_identifier} "
                f"using join columns: {join_cols}"
            )
        else:
            logger.info(
                f"Upserted {combined_table.num_rows} rows to {self.table_identifier} "
                f"using table-defined identifier-field-ids"
            )

    def _complete_overwrite(self, data_files: List["DataFile"]) -> None:
        """
        Complete OVERWRITE mode write using PyIceberg's overwrite API.

        For overwrite, we need to read back the written data files and pass the
        data to PyIceberg's overwrite API, which handles the replacement logic.
        """
        combined_table = self._read_data_files_as_table(data_files)

        # Warn if user passed overwrite_filter via overwrite_kwargs
        if "overwrite_filter" in self._overwrite_kwargs:
            self._overwrite_kwargs.pop("overwrite_filter")
            logger.warning(
                "Use Ray Data's Expressions for overwrite filter instead of passing "
                "it via PyIceberg's overwrite_filter parameter"
            )

        if self._overwrite_filter:
            # Partial overwrite with filter
            from ray.data._internal.datasource.iceberg_datasource import (
                _IcebergExpressionVisitor,
            )

            iceberg_filter = _IcebergExpressionVisitor().visit(self._overwrite_filter)
            self._table.overwrite(
                df=combined_table,
                overwrite_filter=iceberg_filter,
                snapshot_properties=self._snapshot_properties,
                **self._overwrite_kwargs,
            )
            logger.info(
                f"Overwrote {combined_table.num_rows} rows in {self.table_identifier} "
                f"matching filter: {self._overwrite_filter}"
            )
        else:
            # Full table overwrite
            self._table.overwrite(
                df=combined_table,
                snapshot_properties=self._snapshot_properties,
                **self._overwrite_kwargs,
            )
            logger.info(
                f"Overwrote entire table {self.table_identifier} "
                f"with {combined_table.num_rows} rows"
            )

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]) -> None:
        """
        Complete the write operation based on the configured mode.

        This commits the snapshot with all the data files that were written
        during the write phase, respecting streaming semantics.
        """
        # Collect all DataFile objects
        data_files = self._collect_data_files(write_result)
        if not data_files:
            logger.warning("No data files to commit")
            return

        logger.info(
            f"Completing write with {len(data_files)} data file(s) "
            f"in {self._mode} mode"
        )

        # Always reload the table to get the latest schema before any post-write operations
        self._reload_table()

        # Before committing in any mode, ensure the table schema contains any new columns
        # present in the written data files.
        import pyarrow.parquet as pq

        from ray.data._internal.arrow_ops import transform_pyarrow

        schemas = []
        for df in data_files:
            input_file = self._table.io.new_input(df.file_path)
            with input_file.open() as f:
                md = pq.read_metadata(f)
                schemas.append(md.schema.to_arrow_schema())

        # Unify Arrow schemas then evolve once
        incoming_schema = transform_pyarrow.unify_schemas(schemas, promote_types=True)
        self._update_schema(incoming_schema)

        # Execute the appropriate write operation
        if self._mode == SaveMode.APPEND:
            self._complete_append(data_files)
        elif self._mode == SaveMode.UPSERT:
            self._complete_upsert(data_files)
        elif self._mode == SaveMode.OVERWRITE:
            self._complete_overwrite(data_files)
        else:
            raise ValueError(
                f"Unsupported write mode: {self._mode}. "
                f"Supported modes are: APPEND, UPSERT, OVERWRITE"
            )
