import logging
from functools import partial
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


@DeveloperAPI
class HudiDatasource(Datasource):
    """Hudi datasource, for reading Apache Hudi table."""

    def __init__(
        self,
        table_uri: str,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        _check_import(self, module="hudi", package="hudi-python")

        from hudi import HudiTable

        self.hudi_table = HudiTable(table_uri, storage_options)

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        read_tasks = []
        schema = self.hudi_table.schema()
        for file_slices in self.hudi_table.split_latest_file_slices(parallelism):
            if len(file_slices) <= 0:
                continue

            num_rows = 0
            input_files = []
            size_bytes = 0
            for f in file_slices:
                num_rows += f.num_records
                input_files.append(f.base_file_path)
                size_bytes += f.base_file_size

            metadata = BlockMetadata(
                num_rows=num_rows,
                schema=schema,
                input_files=input_files,
                size_bytes=size_bytes,
                exec_stats=None,
            )

            read_task = ReadTask(
                partial(_read_file_slices, self.hudi_table, input_files),
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(xushiyan) add APIs to provide estimated in-memory size
        return None


def _read_file_slices(
    hudi_table, file_slice_paths: List[str]
) -> Iterator["pyarrow.Table"]:
    import pyarrow

    for p in file_slice_paths:
        yield pyarrow.Table.from_batches(hudi_table.read_file_slice(p))
