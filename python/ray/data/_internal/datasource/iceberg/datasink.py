"""
Module to write Ray Datasets to Apache Iceberg tables using PyIceberg.

Supports append and overwrite operations with ACID transaction guarantees.
See: https://py.iceberg.apache.org/
"""

import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional

from packaging import version

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.manifest import DataFile


logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(Datasink[List["DataFile"]]):
    """Datasink for writing Ray Datasets to Apache Iceberg tables.

    Supports append and overwrite modes with optional filtering for dynamic partition overwrites.
    See: https://py.iceberg.apache.org/
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        mode: Literal["append", "overwrite"] = "append",
        overwrite_filter: Optional["BooleanExpression"] = None,
    ):
        """
        Initialize the IcebergDatasink.

        Args:
            table_identifier: Fully qualified table identifier (e.g., "db_name.table_name")
            catalog_kwargs: Arguments for PyIceberg catalog.load_catalog()
            snapshot_properties: Custom properties to attach to snapshot
            mode: Write mode - "append" or "overwrite"
            overwrite_filter: Optional filter for dynamic partition overwrite
        """
        self.table_identifier = table_identifier
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}
        self._snapshot_properties = (
            snapshot_properties if snapshot_properties is not None else {}
        )
        self._mode = mode
        self._overwrite_filter = overwrite_filter

        if mode not in ("append", "overwrite"):
            raise ValueError(f"Invalid mode '{mode}'. Must be 'append' or 'overwrite'.")

        if overwrite_filter is not None and mode != "overwrite":
            logger.warning(
                "overwrite_filter is specified but mode is not 'overwrite'. "
                "The filter will be ignored."
            )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        # Will be initialized in on_write_start()
        from pyiceberg.io import FileIO
        from pyiceberg.table import Transaction
        from pyiceberg.table.metadata import TableMetadata

        self._uuid: str = None
        self._io: FileIO = None
        self._txn: Transaction = None
        self._table_metadata: TableMetadata = None
        self._manifest_merge_enabled: bool = False

    def __getstate__(self) -> dict:
        """Exclude `_txn` during pickling (transaction is driver-only)."""
        state = self.__dict__.copy()
        del state["_txn"]
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state after unpickling."""
        self.__dict__.update(state)
        self._txn = None

    def _get_catalog(self) -> "Catalog":
        """Load the Iceberg catalog."""
        from pyiceberg import catalog
        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def on_write_start(self) -> None:
        """Prepare for write transaction: load table, start transaction, validate partitions."""
        import pyiceberg
        from pyiceberg.table import TableProperties

        if version.parse(pyiceberg.__version__) >= version.parse("0.9.0"):
            from pyiceberg.utils.properties import property_as_bool
        else:
            from pyiceberg.table import PropertyUtil
            property_as_bool = PropertyUtil.property_as_bool

        catalog = self._get_catalog()
        table = catalog.load_table(self.table_identifier)
        self._txn = table.transaction()
        self._io = self._txn._table.io
        self._table_metadata = self._txn.table_metadata
        self._uuid = uuid.uuid4()

        # Validate partition transforms support PyArrow
        unsupported = []
        for field in self._table_metadata.spec().fields:
            if not getattr(field.transform, "supports_pyarrow_transform", True):
                unsupported.append(field)
        if unsupported:
            raise ValueError(
                f"Unsupported partition transforms: {unsupported}. "
                "Only transforms supporting PyArrow are supported."
            )

        self._manifest_merge_enabled = property_as_bool(
            self._table_metadata.properties,
            TableProperties.MANIFEST_MERGE_ENABLED,
            TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
        )

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> List["DataFile"]:
        """Write Ray Data blocks to Parquet files and return DataFile metadata."""
        from pyiceberg.io.pyarrow import (
            _check_pyarrow_schema_compatible,
            _dataframe_to_data_files,
        )
        from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
        from pyiceberg.utils.config import Config

        data_files_list: List["DataFile"] = []
        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            downcast_ns_timestamp_to_us = (
                Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
            )
            _check_pyarrow_schema_compatible(
                self._table_metadata.schema(),
                provided_schema=pa_table.schema,
                downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
            )
            if pa_table.shape[0] <= 0:
                continue
            data_files = _dataframe_to_data_files(
                self._table_metadata, pa_table, self._io, uuid.uuid4()
            )
            data_files_list.extend(data_files)
        return data_files_list

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]):
        """Commit all written data files to the Iceberg table in a single ACID transaction."""
        all_data_files = []
        for task_data_files in write_result.write_returns:
            all_data_files.extend(task_data_files)

        if not all_data_files:
            logger.warning(f"No data files written for table {self.table_identifier}. Skipping commit.")
            return

        update_snapshot = self._txn.update_snapshot(snapshot_properties=self._snapshot_properties)

        if self._mode == "append":
            append_method = (
                update_snapshot.merge_append if self._manifest_merge_enabled else update_snapshot.fast_append
            )
            with append_method() as append_files:
                append_files.commit_uuid = self._uuid
                for data_file in all_data_files:
                    append_files.append_data_file(data_file)
        elif self._mode == "overwrite":
            if hasattr(update_snapshot, "overwrite_files"):
                with update_snapshot.overwrite_files() as overwrite_files:
                    overwrite_files.commit_uuid = self._uuid
                    from pyiceberg.expressions import AlwaysTrue
                    overwrite_files.delete_by_predicate(
                        self._overwrite_filter if self._overwrite_filter is not None else AlwaysTrue()
                    )
                    for data_file in all_data_files:
                        overwrite_files.append_data_file(data_file)
            else:
                # Fallback for PyIceberg < 0.10.0
                import pyiceberg
                logger.warning(
                    f"PyIceberg {pyiceberg.__version__} detected. Using fallback overwrite. "
                    "Upgrade to >= 0.10.0 for optimal performance."
                )
                with update_snapshot.delete() as delete_snapshot:
                    delete_snapshot.commit_uuid = self._uuid
                    from pyiceberg.expressions import AlwaysTrue
                    delete_snapshot.delete_by_predicate(
                        self._overwrite_filter if self._overwrite_filter is not None else AlwaysTrue()
                    )
                append_method = (
                    update_snapshot.merge_append
                    if self._manifest_merge_enabled
                    else update_snapshot.fast_append
                )
                with append_method() as append_files:
                    append_files.commit_uuid = self._uuid
                    for data_file in all_data_files:
                        append_files.append_data_file(data_file)

        try:
            self._txn.commit_transaction()
        except Exception as e:
            logger.error(f"Failed to commit transaction for table {self.table_identifier}: {e}")
            raise

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failures by logging error (transaction auto-rolls back)."""
        logger.error(f"Write operation failed for table {self.table_identifier}: {error}")
