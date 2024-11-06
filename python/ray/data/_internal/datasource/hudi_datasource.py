import logging
from pathlib import PurePosixPath
from typing import Dict, Iterator, List, Optional
from urllib.parse import urljoin

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


class HudiDatasource(Datasource):
    """Hudi datasource, for reading Apache Hudi table."""

    def __init__(
        self,
        table_uri: str,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        _check_import(self, module="hudi", package="hudi-python")

        self._table_uri = table_uri
        self._storage_options = storage_options

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        import pyarrow
        from hudi import HudiTable

        def _perform_read(
            table_uri: str,
            base_file_paths: List[str],
            options: Dict[str, str],
        ) -> Iterator["pyarrow.Table"]:
            from hudi import HudiFileGroupReader

            for p in base_file_paths:
                fg_reader = HudiFileGroupReader(table_uri, options)
                batch = fg_reader.read_file_slice_by_base_file_path(p)
                yield pyarrow.Table.from_batches([batch])

        hudi_table = HudiTable(self._table_uri, self._storage_options)

        reader_options = {}
        reader_options.update(hudi_table.storage_options())
        reader_options.update(hudi_table.hudi_options())

        schema = hudi_table.get_schema()
        read_tasks = []
        for file_slices in hudi_table.split_file_slices(parallelism):
            if len(file_slices) <= 0:
                continue

            num_rows = 0
            relative_paths = []
            input_files = []
            size_bytes = 0
            for f in file_slices:
                num_rows += f.num_records
                relative_path = f.base_file_relative_path()
                relative_paths.append(relative_path)
                full_path = urljoin(
                    self._table_uri, PurePosixPath(relative_path).as_posix()
                )
                input_files.append(full_path)
                size_bytes += f.base_file_size

            metadata = BlockMetadata(
                num_rows=num_rows,
                schema=schema,
                input_files=input_files,
                size_bytes=size_bytes,
                exec_stats=None,
            )

            read_task = ReadTask(
                read_fn=lambda paths=relative_paths: _perform_read(
                    self._table_uri, paths, reader_options
                ),
                metadata=metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(xushiyan) add APIs to provide estimated in-memory size
        return None
