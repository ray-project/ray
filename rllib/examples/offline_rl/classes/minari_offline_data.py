import logging
import time
import ray

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.offline.offline_data import OfflineData
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

from minari_datasource import MinariDatasource

logger = logging.getLogger(__name__)


class MinariOfflineData(OfflineData):
    @override(OfflineData)
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: "AlgorithmConfig"):

        # TODO (simon): Define self.spaces here.
        self.config = config
        self.is_multi_agent = self.config.is_multi_agent
        self.path = (
            self.config.input_
            if not isinstance(self.config.input_, list)
            else self.config.input_[0]
        )
        # Override default arguments for the data read method.
        self.data_read_method_kwargs = self.config.input_read_method_kwargs
        # In case `EpisodeType` or `BatchType` batches are read the size
        # could differ from the final `train_batch_size_per_learner`.
        self.data_read_batch_size = self.config.input_read_batch_size

        # If data should be materialized.
        self.materialize_data = config.materialize_data
        # If mapped data should be materialized.
        self.materialize_mapped_data = config.materialize_mapped_data
        # Flag to identify, if data has already been mapped with the
        # `OfflinePreLearner`.
        self.data_is_mapped = False

        try:
            # Load the dataset.
            start_time = time.perf_counter()
            # Read here from datasource.
            self.data = ray.data.read_datasource(
                MinariDatasource(self.path, **self.data_read_method_kwargs)
            )
            if self.materialize_data:
                self.data = self.data.materialize()
            stop_time = time.perf_counter()
            logger.debug(
                "===> [OfflineData] - Time for loading dataset: "
                f"{stop_time - start_time}s."
            )
            logger.info("Reading data from {}".format(self.path))
        except Exception as e:
            logger.error(e)
        # Avoids reinstantiating the batch iterator each time we sample.
        self.batch_iterators = None
        self.map_batches_kwargs = (
            self.default_map_batches_kwargs | self.config.map_batches_kwargs
        )
        self.iter_batches_kwargs = (
            self.default_iter_batches_kwargs | self.config.iter_batches_kwargs
        )
        self.returned_streaming_split = False
        # Defines the prelearner class. Note, this could be user-defined.
        self.prelearner_class = self.config.prelearner_class or OfflinePreLearner
        # For remote learner setups.
        self.locality_hints = None
        self.learner_handles = None
        self.module_spec = None
