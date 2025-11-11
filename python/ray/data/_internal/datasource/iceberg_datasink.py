"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from packaging import version
from pyiceberg.io import FileIO
from pyiceberg.table import Transaction
from pyiceberg.table.metadata import TableMetadata

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
            snapshot_properties: Custom properties to write to snapshot when committing
                to an iceberg table, such as {"commit_time": "2021-01-01T00:00:00Z"}
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
        self._catalog_kwargs = catalog_kwargs or {}
        self._snapshot_properties = snapshot_properties or {}
        self._mode = mode
        self._overwrite_filter = overwrite_filter
        self._upsert_kwargs = upsert_kwargs or {}
        self._overwrite_kwargs = overwrite_kwargs or {}

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self._uuid: str = None
        self._io: FileIO = None
        self._txn: Transaction = None
        self._table: "Table" = None
        self._table_metadata: TableMetadata = None

    # Since iceberg transaction is not pickle-able, because of the table and catalog properties
    # we need to exclude the transaction object during serialization and deserialization during pickle
    def __getstate__(self) -> dict:
        """Exclude `_txn` and `_table` during pickling."""
        state = self.__dict__.copy()
        state.pop("_txn", None)
        state.pop("_table", None)
        state.pop("_table_metadata", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._txn = None
        self._table = None
        self._table_metadata = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def _ensure_table_loaded_for_append(self) -> None:
        """Ensure table metadata and IO are loaded for APPEND mode (handles unpickling)."""
        if self._table_metadata is None or self._io is None:
            catalog = self._get_catalog()
            table = catalog.load_table(self.table_identifier)
            self._txn = table.transaction()
            self._io = self._txn._table.io
            self._table_metadata = self._txn.table_metadata

    def _pyarrow_type_to_iceberg(self, pa_type: "pa.DataType"):
        """Convert a PyArrow type to an Iceberg type using PyIceberg's native visitor."""
        from pyiceberg.io.pyarrow import _ConvertToIceberg, visit_pyarrow

        visitor = _ConvertToIceberg(downcast_ns_timestamp_to_us=False, format_version=2)
        return visit_pyarrow(pa_type, visitor)

    def _update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data.

        Args:
            incoming_schema: The PyArrow schema from the incoming data
        """
        current_schema = self._table.schema().as_arrow()
        current_field_names = {field.name for field in current_schema}

        # Find new fields
        new_fields = [
            field for field in incoming_schema if field.name not in current_field_names
        ]

        if new_fields:
            logger.info(
                f"Updating schema to add {len(new_fields)} new fields: "
                f"{[f.name for f in new_fields]}"
            )

            # Use PyIceberg's update_schema API
            with self._table.update_schema() as update:
                for field in new_fields:
                    try:
                        # Convert PyArrow type to Iceberg type
                        iceberg_type = self._pyarrow_type_to_iceberg(field.type)
                        update.add_column(
                            path=field.name,
                            field_type=iceberg_type,
                            doc=None,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to add field {field.name}: {e}. Skipping."
                        )

            # Reload table completely after schema evolution
            catalog = self._get_catalog()
            self._table = catalog.load_table(self.table_identifier)

    def _validate_partitions_for_append(self) -> None:
        """Validate that all partitions support PyArrow transforms for APPEND mode."""
        unsupported_partitions = [
            field
            for field in self._table_metadata.spec().fields
            if not getattr(field.transform, "supports_pyarrow_transform", True)
        ]

        if unsupported_partitions:
            raise ValueError(
                f"Not all partition types are supported for writes. "
                f"Following partitions cannot be written using pyarrow: {unsupported_partitions}"
            )

    def _setup_manifest_merge(self) -> None:
        """Configure manifest merge settings for APPEND mode."""
        import pyiceberg
        from pyiceberg.table import TableProperties

        if version.parse(pyiceberg.__version__) >= version.parse("0.9.0"):
            from pyiceberg.utils.properties import property_as_bool
        else:
            from pyiceberg.table import PropertyUtil

            property_as_bool = PropertyUtil.property_as_bool

        self._manifest_merge_enabled = property_as_bool(
            self._table_metadata.properties,
            TableProperties.MANIFEST_MERGE_ENABLED,
            TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
        )

    def on_write_start(self) -> None:
        """Initialize table and transaction for writing."""
        catalog = self._get_catalog()
        table = catalog.load_table(self.table_identifier)

        if self._mode == SaveMode.APPEND:
            # Use transaction-based API for optimal concurrency
            self._txn = table.transaction()
            self._io = self._txn._table.io
            self._table_metadata = self._txn.table_metadata
            self._table = table
            self._validate_partitions_for_append()
            self._setup_manifest_merge()
        else:
            # Use high-level APIs for UPSERT/OVERWRITE
            self._table = table
            self._table_metadata = table.metadata

        self._uuid = uuid.uuid4()

    def _write_append_mode(self, blocks: Iterable[Block]) -> List["DataFile"]:
        """Write blocks using transaction-based API for APPEND mode."""
        from pyiceberg.io.pyarrow import (
            _check_pyarrow_schema_compatible,
            _dataframe_to_data_files,
        )
        from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
        from pyiceberg.utils.config import Config

        self._ensure_table_loaded_for_append()

        data_files_list = []
        downcast_ns_timestamp_to_us = (
            Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
        )

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()

            if pa_table.shape[0] <= 0:
                continue

            _check_pyarrow_schema_compatible(
                self._table_metadata.schema(),
                provided_schema=pa_table.schema,
                downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
            )

            task_uuid = uuid.uuid4()
            data_files = _dataframe_to_data_files(
                self._table_metadata, pa_table, self._io, task_uuid
            )
            data_files_list.extend(data_files)

        return data_files_list

    def _write_upsert_overwrite_mode(self, blocks: Iterable[Block]) -> List["pa.Table"]:
        """Collect PyArrow tables for UPSERT/OVERWRITE modes."""
        collected_tables = []

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()

            if pa_table.num_rows > 0:
                collected_tables.append(pa_table)

        return collected_tables

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> WriteResult[List["DataFile"]]:
        """Write blocks to data files based on the configured mode."""
        if self._mode == SaveMode.APPEND:
            return self._write_append_mode(blocks)
        return self._write_upsert_overwrite_mode(blocks)

    def _collect_and_concat_tables(
        self, write_result: WriteResult[List["pa.Table"]]
    ) -> Optional["pa.Table"]:
        """Collect and concatenate all PyArrow tables from write results."""
        import pyarrow as pa

        all_tables = []
        for tables_batch in write_result.write_returns:
            all_tables.extend(tables_batch)

        if not all_tables:
            logger.warning("No data to write")
            return None

        return pa.concat_tables(all_tables)

    def _complete_append(self, write_result: WriteResult[List["DataFile"]]) -> None:
        """Complete APPEND mode write using transaction commit."""
        update_snapshot = self._txn.update_snapshot(
            snapshot_properties=self._snapshot_properties
        )
        append_method = (
            update_snapshot.merge_append
            if self._manifest_merge_enabled
            else update_snapshot.fast_append
        )

        with append_method() as append_files:
            append_files.commit_uuid = self._uuid
            for data_files in write_result.write_returns:
                for data_file in data_files:
                    append_files.append_data_file(data_file)

        self._txn.commit_transaction()

    def _complete_upsert(self, combined_table: "pa.Table") -> None:
        """Complete UPSERT mode write using PyIceberg's upsert API."""
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

    def _complete_overwrite(self, combined_table: "pa.Table") -> None:
        """Complete OVERWRITE mode write using PyIceberg's overwrite API."""
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
        """Complete the write operation based on the configured mode."""
        if self._mode == SaveMode.APPEND:
            self._complete_append(write_result)
            return

        # For UPSERT/OVERWRITE: collect tables and apply schema evolution
        combined_table = self._collect_and_concat_tables(write_result)
        if combined_table is None:
            return

        self._update_schema(combined_table.schema)

        if self._mode == SaveMode.UPSERT:
            self._complete_upsert(combined_table)
        elif self._mode == SaveMode.OVERWRITE:
            self._complete_overwrite(combined_table)
        else:
            raise ValueError(
                f"Unsupported write mode: {self._mode}. "
                f"Supported modes are: APPEND, UPSERT, OVERWRITE"
            )
