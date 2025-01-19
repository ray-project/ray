import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.datasource.datasink import Datasink
from ray.util.annotations import DeveloperAPI
from ray.data.block import Block

from python.ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from python.ray.data.datasource.datasink import WriteResult
from python.ray.data.datasource.filename_provider import _DefaultFilenameProvider


if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table


logger = logging.getLogger(__name__)

_SUPPORTED_COMPRESSION_CODECS = {"snappy", "zstd"}


class _LoggingFilenameProvider(_DefaultFilenameProvider):
    def __init__(
        self, dataset_uuid: Optional[str] = None, file_format: Optional[str] = None
    ):
        self.super().__init__(dataset_uuid, file_format)
        self.written_paths: List[str] = []

    def get_filename_for_block(
        self, block: Block, task_index: int, block_index: int
    ) -> str:
        file_id = f"{task_index:06}_{block_index:06}"
        file_name = self._generate_filename(file_id)
        self.written_paths.append(file_name)
        return file_name

    def get_filename_for_row(
        self, row: Dict[str, Any], task_index: int, block_index: int, row_index: int
    ) -> str:
        raise NotImplementedError("IcebergDatasink does not support writing rows")


@DeveloperAPI
class IcebergDatasink(Datasink):
    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
    ):
        """
        Iceberg datasink writes Ray Dataset to an existing Iceberg tables. This module heavily uses `write_parquet` and
        PyIceberg to write parquet files to iceberg data folder and add_files to the iceberg table in a transaction

        Args:
            table_identifier: Fully qualified table identifier (i.e., "db_name.table_name")
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
            snapshot_properties: Optional properties to add to the iceberg snapshot when adding files
        """
        _check_import(self, module="pyiceberg", package="pyiceberg")

        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"
        self.table_identifier = table_identifier
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

    def _create_datasink(self) -> Datasink:
        props = self._table.properties
        datafile_format = props.get("write.format.default", "parquet")
        compression_codec = props.get("write.parquet.compression-codec")

        if datafile_format and datafile_format != "parquet":
            raise NotImplementedError(
                f"IcebergDatasink currently does not support {datafile_format}, and only supports writing to parquet tables"
            )
        if compression_codec and compression_codec not in _SUPPORTED_COMPRESSION_CODECS:
            raise NotImplementedError(
                f"IcebergDatasink currently does not support {compression_codec}, and only supports {_SUPPORTED_COMPRESSION_CODECS}"
            )

        filename_provider = _LoggingFilenameProvider(
            dataset_uuid=str(uuid.uuid4()), file_format=datafile_format
        )

        data_path = f"{self._table.location}/data/"
        return ParquetDatasink(
            path=data_path,
            try_create_dir=False,
            filename_provider=filename_provider,
            arrow_parquet_args={"compression": compression_codec},
        )

    def on_write_start(self) -> None:
        _check_not_partitioned(self._table)
        self._datasink = self._create_datasink()
        self._datasink.on_write_start()

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
        self._datasink.write(blocks, ctx)

    def on_write_complete(self, write_result_blocks: List[Block]) -> WriteResult:
        write_result = self._datasink.on_write_complete(write_result_blocks)
        written_paths = self._datasink.filename_provider.written_paths
        self._table.add_files(
            files=written_paths, snapshot_properties=self._snapshot_properties
        )

        return write_result


def _check_not_partitioned(table: "Table"):
    current_spec = table.specs[table.current_spec_id]
    if len(current_spec.fields) > 0:
        raise NotImplementedError(
            "IcebergDatasink currently does not support writing to partitioned iceberg table"
        )
