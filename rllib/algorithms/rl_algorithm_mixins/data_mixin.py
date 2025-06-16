import abc

from typing import Any, Dict, List, Union

from ray.data import DataIterator
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.offline.offline_data import OfflineData
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.typing import EpisodeType


class DataSourceMixin(abs.ABC):
    def __init__(self, config: AlgorithmConfig):

        self._data_source = None
        self.setup(self, config)

    @abc.abstractmethod
    def setup(self, config: AlgorithmConfig):
        """Abstract method to setup the data source."""
        pass

    @abc.abstractmethod
    def sample(self, batch_size, *args, **kwargs) -> Dict[str, Any]:
        """Abstract method to sample from the data source."""
        pass

    @abc.abstractmethod
    def cleanup(self):
        """Abstract method to release all resources."""
        self._data_source = None


class ReplayBufferConcreteMixin(DataSourceMixin):
    @override(DataSourceMixin)
    def setup(self, config: AlgorithmConfig):
        self._data_source = EpisodeReplayBuffer(**config.replay_buffer_config)

    @override(DataSourceMixin)
    def sample(self, batch_size: int) -> Dict[str, Any]:
        # Sample from the buffer `batch_size` many items.
        return self._data_source.sample(batch_size)

    def add(self, episodes: List[EpisodeType]):
        """Add new episodes to the buffer"""
        return self._data_source.add(episodes)


class OfflineDataConcreteMixin(DataSourceMixin):
    @override(DataSourceMixin)
    def setup(self, config: AlgorithmConfig):
        # Set up the `OfflineData` as the data source.
        self._data_source = OfflineData(config)

    @override(DataSourceMixin)
    def sample(
        self, batch_size, *args, **kwargs
    ) -> Union[Dict[str, Any], List[DataIterator]]:
        """Sample from the Ray Dataset."""
        return self._data_source.sample(batch_size, *args, **kwargs)
