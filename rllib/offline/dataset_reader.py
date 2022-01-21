import logging

from ray import data
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
                "type": "json",
                "path": "/tmp/sample_batches.json",
            }
        }
    """

    @PublicAPI
    def __init__(self, ioctx: IOContext = None):
        """Initializes a DatasetReader instance.

        Args:
            ioctx: Current IO context object.
        """
        self._ioctx = ioctx

        input_config = ioctx.input_config
        if (not input_config.get("type", None) or
            not input_config.get("path", None)):
            raise ValueError(
                "Must specify type and path via input_config key"
                " when using Ray dataset input.")

        self.dataset = None
        type = input_config["type"]
        path = input_config["path"]
        if type == "json":
            self.dataset = data.read_json(path)
        else:
            raise ValueError("Un-supported Ray dataset type: ", type)

    @override(InputReader)
    def next(self) -> SampleBatchType:
        return from_json_data(self.dataset.take(1), self._ioctx.worker)
