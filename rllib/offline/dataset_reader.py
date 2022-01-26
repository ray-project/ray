import logging

import ray.data
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.json_reader import from_json_data
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


@PublicAPI
class DatasetReader(InputReader):
    """Reader object that loads data from Ray Dataset.

    Examples:
        config = {
            "input"="dataset",
            "input_config"={
                "format": "json",
                "path": "/tmp/sample_batches/",
            }
        }

    `path` may be a single data file, a directory, or anything
        that ray.data.dataset recognizes.
    """

    @PublicAPI
    def __init__(self, ioctx: IOContext, ds: ray.data.Dataset):
        """Initializes a DatasetReader instance.

        Args:
            ds: Ray dataset to sample from.
        """
        self._ioctx = ioctx
        self._dataset = ds
        # We allow the creation of a non-functioning None DatasetReader.
        # It's useful for example for a non-rollout local worker.
        if ds:
            print("DatasetReader ", ioctx.worker_index, " has ", ds.count(),
                  " samples.")
            self._iter = self._dataset.repeat().iter_rows()
        else:
            self._iter = None

    @override(InputReader)
    def next(self) -> SampleBatchType:
        # next() should not get called on None DatasetReader.
        assert self._iter is not None

        d = next(self._iter).as_pydict()
        # Columns like obs are compressed when written by DatasetWriter.
        d = from_json_data(d, self._ioctx.worker)

        return d
