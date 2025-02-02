import logging
import os
from typing import Dict, Iterator, List, Optional

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
                file_group_reader = HudiFileGroupReader(table_uri, options)
                batch = file_group_reader.read_file_slice_by_base_file_path(p)
                yield pyarrow.Table.from_batches([batch])

        hudi_table = HudiTable(self._table_uri, self._storage_options)

        reader_options = {
            **hudi_table.storage_options(),
            **hudi_table.hudi_options(),
        }

        schema = hudi_table.get_schema()
        read_tasks = []
        for file_slices_split in hudi_table.get_file_slices_splits(parallelism):
            num_rows = 0
            relative_paths = []
            input_files = []
            size_bytes = 0
            for file_slice in file_slices_split:
                # A file slice in a Hudi table is a logical group of data files
                # within a physical partition. Records stored in a file slice
                # are associated with a commit on the Hudi table's timeline.
                # For more info, see https://hudi.apache.org/docs/file_layouts
                num_rows += file_slice.num_records
                relative_path = file_slice.base_file_relative_path()
                relative_paths.append(relative_path)
                full_path = os.path.join(self._table_uri, relative_path)
                input_files.append(full_path)
                size_bytes += file_slice.base_file_size

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
