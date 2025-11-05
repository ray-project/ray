import io
import logging
from typing import Any, Dict

import numpy as np
from PIL import Image

from ray import data
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.offline.offline_data import OfflineData
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class ImageOfflineData(OfflineData):
    """This class overrides `OfflineData` to read in raw image data.

    The image data is from Ray Data`s S3 example bucket, namely
    `ray-example-data/batoidea/JPEGImages/`.
    To read in this data the raw bytes have to be decoded and then
    converted to `numpy` arrays. Each image array has a dimension
    (32, 32, 3).

    To just read in the raw image data and convert it to arrays it
    suffices to override the `OfflineData.__init__` method only.
    Note, that further transformations of the data - specifically
    into `SingleAgentEpisode` data - will be performed in a custom
    `OfflinePreLearner` defined in the `image_offline_prelearner`
    file. You could hard-code the usage of this prelearner here,
    but you will use the `prelearner_class` attribute in the
    `AlgorithmConfig` instead.
    """

    @override(OfflineData)
    def __init__(self, config: AlgorithmConfig):

        # Set class attributes.
        self.config = config
        self.is_multi_agent = self.config.is_multi_agent
        self.materialize_mapped_data = False
        self.path = self.config.input_

        self.data_read_batch_size = self.config.input_read_batch_size
        self.data_is_mapped = False

        # Define your function to map images to numpy arrays.
        def map_to_numpy(row: Dict[str, Any]) -> Dict[str, Any]:
            # Convert to byte stream.
            bytes_stream = io.BytesIO(row["bytes"])
            # Convert to image.
            image = Image.open(bytes_stream)
            # Return an array of the image.
            return {"array": np.array(image)}

        try:
            # Load the dataset and transform to arrays on-the-fly.
            self.data = data.read_binary_files(self.path).map(map_to_numpy)
        except Exception as e:
            logger.error(e)

        # Define further attributes needed in the `sample` method.
        self.batch_iterator = None
        self.map_batches_kwargs = self.config.map_batches_kwargs
        self.iter_batches_kwargs = self.config.iter_batches_kwargs
        # Use a custom OfflinePreLearner if needed.
        self.prelearner_class = self.config.prelearner_class or OfflinePreLearner

        # For remote learner setups.
        self.locality_hints = None
        self.learner_handles = None
        self.module_spec = None
