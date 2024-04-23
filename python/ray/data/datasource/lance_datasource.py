import logging
from typing import List, Optional

import lance
import pyarrow as pa
from lance import LanceFragment

from ray.data import ReadTask
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class LanceDatasource(Datasource):
    """Lance Datasource
    Read a Lance table as a Ray Dataset

    Parameters
    ----------
    uri : str
        The base URI of the Lance dataset.
    columns: list
        A list of columns to return from the dataset.
    filter: str
        A standard SQL expressions as predicates for dataset filtering.
    """

    def __init__(
        self,
        uri: str,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
    ):
        self.uri = uri
        self.columns = columns
        self.filter = filter

        self.lance_ds = lance.dataset(uri)
        self.fragments = self.lance_ds.get_fragments()

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # To begin with, read one Fragment at a time
        # Each Ray Data Block contains a Pandas RecordBatch
        def _read_single_fragment(fragment: LanceFragment) -> Block:
            # Fetch batches from the fragment
            batches = fragment.to_batches(columns=self.columns, filter=self.filter)

            # Convert the generator of RecordBatch objects to a list
            batches_list = list(batches)

            # Convert the list of RecordBatch objects to a Table
            table = pa.Table.from_batches(batches_list)

            return table

        read_tasks = []
        for fragment in self.fragments:
            data_files = ", ".join(
                [data_file.path() for data_file in fragment.data_files()]
            )

            metadata = BlockMetadata(
                num_rows=fragment.count_rows(),
                size_bytes=None,
                schema=fragment.schema,
                input_files=[data_files],
                exec_stats=None,
            )

            read_task = ReadTask(
                lambda fragment=fragment: [_read_single_fragment(fragment)],
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO: Add memory size estimation to improve auto-tune of parallelism.
        return None
