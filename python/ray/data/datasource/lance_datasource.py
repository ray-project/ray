import logging
from typing import TYPE_CHECKING, Iterator, List, Dict, Optional

import lance
import numpy as np
import pyarrow as pa
from lance import LanceFragment

from ray.data import ReadTask
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


@DeveloperAPI
class LanceDatasource(Datasource):
    """Lance Datasource
    Read a Lance Dataset as a Ray Dataset

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
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self.uri = uri
        self.columns = columns
        self.filter = filter
        self.storage_options = storage_options

        self.lance_ds = lance.dataset(uri=uri, storage_options=storage_options)
        self.fragments = self.lance_ds.get_fragments()

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # Read multiple fragments in parallel
        # Each Ray Data Block contains a Pandas RecordBatch
        def _read_fragments(
            fragments: List[LanceFragment],
        ) -> Iterator["pyarrow.Table"]:
            for fragment in fragments:
                batches = fragment.to_batches(columns=self.columns, filter=self.filter)
                for batch in batches:
                    yield pa.Table.from_batches([batch])

        # Set the parallelism to the min of the number of fragments
        if parallelism > len(self.fragments):
            parallelism = len(self.fragments)
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the "
                "number of files"
            )

        read_tasks = []
        for fragments in np.array_split(self.fragments, parallelism):
            if len(fragments) <= 0:
                continue

            metadata = BlockMetadata(
                num_rows=None,
                schema=None,
                input_files=None,
                size_bytes=None,
                exec_stats=None,
            )

            read_task = ReadTask(
                lambda fragments=fragments: _read_fragments(fragments),
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO: Add memory size estimation to improve auto-tune of parallelism.
        return None
