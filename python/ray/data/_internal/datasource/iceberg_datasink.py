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
        self._table = None
        self._datasink = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    @property
    def table(self) -> "Table":
        """
        Return the table reference from the catalog
        """
        if self._table is None:
            catalog = self._get_catalog()
            self._table = catalog.load_table(self.table_identifier)
        return self._table

    def on_write_start(self) -> None:
        """Prepare for the transaction"""
        self._uuid = uuid.uuid4()

    def write(self, blocks: Iterable[Block], ctx: TaskContext):
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        data_files_list = []
        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            data_files = _dataframe_to_data_files(
                self.table.metadata, pa_table, self.table.io, self._uuid
            )
            data_files_list.extend(data_files)

        return data_files_list

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]):
        from pyiceberg.io.pyarrow import _FastAppendFiles, Operation

        fast_append = _FastAppendFiles(
            operation=Operation.APPEND,
            transaction=self._table._transaction,
            io=self._table.io,
            snapshot_properties=self._snapshot_properties,
        )

        with fast_append(self._uuid) as append_files:
            for write_return in write_result.write_returns:
                for data_file in write_return:
                    append_files.append_data_file(data_file)
