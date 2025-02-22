"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging

from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray.data.datasource.datasink import Datasink
from ray.util.annotations import DeveloperAPI
from ray.data.block import BlockAccessor, Block
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.datasource.datasink import WriteResult
import uuid

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile


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
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table to read e.g. `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
            snapshot_properties: custom properties write to snapshot when committing
            to an iceberg table, e.g. {"commit_time": "2021-01-01T00:00:00Z"}
        """

        from pyiceberg.io import FileIO
        from pyiceberg.table import Transaction
        from pyiceberg.table.metadata import TableMetadata

        self.table_identifier = table_identifier
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}
        self._snapshot_properties = (
            snapshot_properties if snapshot_properties is not None else {}
        )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self._uuid: str = None
        self._io: FileIO = None
        self._txn: Transaction = None
        self._table_metadata: TableMetadata = None

    # Since iceberg transaction is not pickle-able, because of the table and catalog properties
    # we need to exclude the transaction object during serialization and deserialization during pickle
    def __getstate__(self) -> dict:
        """Exclude `_txn` during pickling."""
        state = self.__dict__.copy()
        del state["_txn"]
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._txn = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def on_write_start(self) -> None:
        """Prepare for the transaction"""
        from pyiceberg.table import PropertyUtil, TableProperties

        catalog = self._get_catalog()
        table = catalog.load_table(self.table_identifier)
        self._txn = table.transaction()
        self._io = self._txn._table.io
        self._table_metadata = self._txn.table_metadata
        self._uuid = uuid.uuid4()

        if unsupported_partitions := [
            field
            for field in self._table_metadata.spec().fields
            if not field.transform.supports_pyarrow_transform
        ]:
            raise ValueError(
                f"Not all partition types are supported for writes. Following partitions cannot be written using pyarrow: {unsupported_partitions}."
            )

        self._manifest_merge_enabled = PropertyUtil.property_as_bool(
            self._table_metadata.properties,
            TableProperties.MANIFEST_MERGE_ENABLED,
            TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
        )

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> WriteResult[List["DataFile"]]:
        from pyiceberg.io.pyarrow import (
            _check_pyarrow_schema_compatible,
            _dataframe_to_data_files,
        )
        from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
        from pyiceberg.utils.config import Config

        data_files_list: WriteResult[List["DataFile"]] = []
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
                self._table_metadata, pa_table, self._io, self._uuid
            )
            data_files_list.extend(data_files)

        return data_files_list

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]):
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
