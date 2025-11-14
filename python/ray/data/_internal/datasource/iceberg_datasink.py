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
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table, Transaction

    from ray.data.expressions import Expr


logger = logging.getLogger(__name__)

# Multiplier for task_idx to ensure unique file names across tasks
_FILE_NAME_COUNTER_BASE = 10000


@DeveloperAPI
class IcebergDatasink(Datasink[tuple[list["DataFile"], "pa.Schema"]]):
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
        from pyiceberg.exceptions import CommitFailedException

        # Use PyIceberg's update_schema API
        try:
            with self._table.update_schema() as update:
                update.union_by_name(incoming_schema)
        except CommitFailedException:
            # Another worker updated the schema concurrently. Reload and continue -
            # union_by_name is idempotent so if they added our columns, we're good.
            # If not, _dataframe_to_data_files will fail with a clear error.
            logger.debug(
                "Schema update conflict - another worker modified schema, reloading"
            )
        finally:
            self._reload_table()

    def on_write_start(self) -> None:
        """Initialize table for writing and set up write UUID."""
        self._reload_table()
        # Generate a unique write UUID for this write operation
        self._write_uuid = uuid.uuid4()

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> tuple[list["DataFile"], "pa.Schema"]:
        """
        Write blocks as parquet files to the table location.

        Returns a tuple of (DataFile objects, schema of written data).
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
            # Return empty schema for empty blocks
            return ([], pa.schema([]))

        # Combine all tables from this task
        combined_table = pa.concat_tables(tables)

        # Ensure table schema is evolved before generating DataFiles. PyIceberg's
        # name-mapping requires field IDs for all columns before writing.
        # We handle concurrent schema updates with retry logic.
        self._update_schema(combined_table.schema)

        # Use PyIceberg's _dataframe_to_data_files which properly handles:
        # - Schema conversion with name mapping
        # - Partitioned vs unpartitioned tables
        # - Proper DataFile formatting
        # Use task_idx as counter base to ensure unique file names across tasks
        counter = itertools.count(ctx.task_idx * _FILE_NAME_COUNTER_BASE)
        data_files = list(
            _dataframe_to_data_files(
                table_metadata=self._table.metadata,
                df=combined_table,
                io=self._table.io,
                write_uuid=self._write_uuid,
                counter=counter,
            )
        )

        return (data_files, combined_table.schema)

    def _collect_data_files_and_schemas(
        self, write_result: WriteResult[tuple[list["DataFile"], "pa.Schema"]]
    ) -> tuple[list["DataFile"], list["pa.Schema"]]:
        """Collect all DataFile objects and schemas from write results."""
        all_data_files = []
        all_schemas = []

        for data_files_batch, schema in write_result.write_returns:
            all_data_files.extend(data_files_batch)
            # Only collect non-empty schemas
            if len(schema) > 0:
                all_schemas.append(schema)

        if not all_data_files:
            return ([], [])

        return (all_data_files, all_schemas)

    def _read_data_files_as_table(self, data_files: List["DataFile"]) -> "pa.Table":
        """
        Read back written data files and combine them into a single PyArrow table.

        This is used for UPSERT mode where PyIceberg's API requires the data as a
        DataFrame/Table rather than DataFile objects for row-level comparisons.

        Note: OVERWRITE mode was optimized to work directly with DataFiles.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        tables = []
        for data_file in data_files:
            # Use PyIceberg's IO to read the file
            input_file = self._table.io.new_input(data_file.file_path)
            with input_file.open() as f:
                table = pq.read_table(f)
                tables.append(table)

        return pa.concat_tables(tables)

    def _append_data_files(
        self,
        data_files: List["DataFile"],
        transaction: "Transaction",
        branch: Optional[str] = None,
    ) -> None:
        """
        Append pre-written DataFiles to a transaction using fast_append.

        This is a helper method used by both APPEND and OVERWRITE modes.

        Args:
            data_files: List of DataFile objects to append
            transaction: PyIceberg transaction to append to
            branch: Optional branch name for the snapshot
        """
        from pyiceberg.table.refs import MAIN_BRANCH

        branch = branch or MAIN_BRANCH
        with transaction.update_snapshot(
            snapshot_properties=self._snapshot_properties, branch=branch
        ).fast_append() as update:
            for data_file in data_files:
                update.append_data_file(data_file)

    def _complete_append(self, data_files: List["DataFile"]) -> None:
        """
        Complete APPEND mode write using PyIceberg's fast_append API.

        This commits all the data files that were written during the write phase.
        """
        self._commit_transaction_with_append(data_files)

    def _complete_upsert(self, data_files: List["DataFile"]) -> None:
        """
        Complete UPSERT mode write using PyIceberg's upsert API.

        For upsert, we must read back the written data files because PyIceberg's
        upsert operation inherently requires the actual data content to:
        1. Compare against existing table data to identify matching rows
        2. Determine which values have actually changed (to avoid unnecessary writes)
        3. Separate rows to update from rows to insert

        This data reload is unavoidable - it's not a limitation of our implementation,
        but a fundamental requirement of the UPSERT operation itself. PyIceberg's
        upsert() API only accepts DataFrame input, not DataFile objects, because
        it needs to perform row-level comparisons.

        Note: APPEND and OVERWRITE modes don't have this limitation and work
        directly with DataFile objects without reloading data.
        """
        combined_table = self._read_data_files_as_table(data_files)

        self._table.upsert(df=combined_table, **self._upsert_kwargs)

    def _validate_overwrite_kwargs(self) -> None:
        """Warn if user passed overwrite_filter via overwrite_kwargs."""
        if "overwrite_filter" in self._overwrite_kwargs:
            self._overwrite_kwargs.pop("overwrite_filter")
            logger.warning(
                "Use Ray Data's Expressions for overwrite filter instead of passing "
                "it via PyIceberg's overwrite_filter parameter"
            )

    def _commit_transaction_with_append(
        self,
        data_files: List["DataFile"],
        branch: Optional[str] = None,
        delete_predicate: Optional["BooleanExpression"] = None,
        case_sensitive: bool = True,
    ) -> None:
        """
        Create a transaction, optionally delete rows, append data files, and commit.

        Args:
            data_files: DataFiles to append
            branch: Optional branch name
            delete_predicate: Optional delete predicate (for overwrite)
            case_sensitive: Whether delete predicate is case sensitive
        """
        from pyiceberg.expressions import AlwaysFalse
        from pyiceberg.table.refs import MAIN_BRANCH

        transaction = self._table.transaction()

        # Optional delete step (for OVERWRITE)
        if delete_predicate is not None and delete_predicate != AlwaysFalse():
            with transaction.update_snapshot(
                snapshot_properties=self._snapshot_properties,
                branch=branch or MAIN_BRANCH,
            ).delete() as delete_snapshot:
                delete_snapshot.delete_by_predicate(
                    predicate=delete_predicate, case_sensitive=case_sensitive
                )

        # Append data files
        self._append_data_files(data_files, transaction, branch)

        # Commit
        transaction.commit_transaction()

    def _complete_overwrite(self, data_files: List["DataFile"]) -> None:
        """
        Complete OVERWRITE mode write using PyIceberg's lower-level transaction API.

        This optimized implementation avoids reloading data by using PyIceberg's
        transaction API directly: delete by predicate (metadata operation) + append
        pre-written DataFiles.
        """
        from pyiceberg.table.refs import MAIN_BRANCH

        # Validate and extract parameters
        self._validate_overwrite_kwargs()
        from pyiceberg.expressions import AlwaysTrue

        iceberg_filter: "BooleanExpression" = AlwaysTrue()
        if self._overwrite_filter:
            from ray.data._internal.datasource.iceberg_datasource import (
                _IcebergExpressionVisitor,
            )

            iceberg_filter = _IcebergExpressionVisitor().visit(self._overwrite_filter)
        branch = self._overwrite_kwargs.get("branch", MAIN_BRANCH)
        case_sensitive = self._overwrite_kwargs.get("case_sensitive", True)

        # Execute transaction
        self._commit_transaction_with_append(
            data_files,
            branch=branch,
            delete_predicate=iceberg_filter,
            case_sensitive=case_sensitive,
        )

    def on_write_complete(
        self, write_result: WriteResult[tuple[list["DataFile"], "pa.Schema"]]
    ) -> None:
        """
        Complete the write operation based on the configured mode.

        This commits the snapshot with all the data files that were written
        during the write phase, respecting streaming semantics.
        """
        # Collect all DataFile objects and schemas from write tasks
        data_files, schemas = self._collect_data_files_and_schemas(write_result)
        if not data_files:
            return

        # Always reload the table to get the latest schema before any post-write operations
        self._reload_table()

        # Before committing in any mode, ensure the table schema contains any new columns
        # present in the written data. Unify schemas from all write tasks.
        if schemas:
            from ray.data._internal.arrow_ops import transform_pyarrow

            incoming_schema = transform_pyarrow.unify_schemas(
                schemas, promote_types=True
            )
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
