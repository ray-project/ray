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
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table


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
            table_identifier: The identifier of the table to read
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
            snapshot_properties: Optional snapshot properties to
                use when reading the table
        """

        self.table_identifier = table_identifier
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}
        self._snapshot_properties = snapshot_properties

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"
        self._uuid = None

        self._io = None
        self._table_metadata = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def on_write_start(self) -> None:
        """Prepare for the transaction"""
        catalog = self._get_catalog()
        table = catalog.load_table(self.table_identifier)
        self._txn = table.transaction()
        self._io = table.io
        self._table_metadata = self._txn.table_metadata
        self._uuid = uuid.uuid4()

        if unsupported_partitions := [
            field
            for field in self._txn.table_metadata.spec().fields
            if not field.transform.supports_pyarrow_transform
        ]:
            raise ValueError(
                f"Not all partition types are supported for writes. Following partitions cannot be written using pyarrow: {unsupported_partitions}."
            )

    def write(self, blocks: Iterable[Block], ctx: TaskContext):
        from pyiceberg.io.pyarrow import (
            _check_pyarrow_schema_compatible,
            _dataframe_to_data_files,
        )
        from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
        from pyiceberg.utils.config import Config

        data_files_list = []
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

            data_files = _dataframe_to_data_files(
                self._table_metadata, pa_table, self._io, self._uuid
            )
            data_files_list.extend(data_files)

        return data_files_list

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]):
        from pyiceberg.table.update.snapshot import _FastAppendFiles
        from pyiceberg.table.snapshots import Operation

        fast_append = _FastAppendFiles(
            operation=Operation.APPEND,
            transaction=self._txn,
            io=self._io,
            snapshot_properties=self._snapshot_properties,
        )

        with fast_append(self._uuid) as append_files:
            for write_return in write_result.write_returns:
                for data_file in write_return:
                    append_files.append_data_file(data_file)
