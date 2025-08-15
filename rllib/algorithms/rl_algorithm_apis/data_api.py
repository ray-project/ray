import abc

from typing import Any, Dict, List, Union

from ray.data import DataIterator
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.offline.offline_data import OfflineData
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.typing import DataSourceType, EpisodeType


class DataSourceAPI(abc.ABC):

    # The data source.
    _data_source: DataSourceType = None

    def __init__(self, config: AlgorithmConfig, **kwargs: Dict[str, Any]):
        """Initializes a DataSourceAPI instance."""
        # Initialize the `super`.
        super().__init__(config=config, **kwargs)

    @abc.abstractmethod
    def _setup(self, config: AlgorithmConfig):
        """Abstract method to setup the data source."""
        # Note, it is important to call `super` here, to not break the MRO.
        super()._setup(config=config)

    # TODO (simon, sven): This naming is unconventional and not very clean.
    #   (1) Either we `sample` by calling `super`'s `sample` and fill a `dict`.
    #   (2) Or we have to create a "mixin of mixins" (online and offline sampling)
    #       which then has a single `sample` method to create a batch of both.
    @abc.abstractmethod
    def sample_from_data(
        self, batch_size, **kwargs: Dict[str, Any]
    ) -> Union[Dict[str, Any], List[Any]]:
        """Abstract method to sample from the data source."""
        pass

    @abc.abstractmethod
    def cleanup(self):
        """Abstract method to release all resources."""
        self._data_source = None
        # Clean up `super`.
        super().cleanup()


class ReplayBufferAPI(DataSourceAPI):
    @override(DataSourceAPI)
    def _setup(self, config: AlgorithmConfig):
        # Build the replay buffer.
        self._data_source = EpisodeReplayBuffer(**config.replay_buffer_config)
        # Set up the `super` class.
        super()._setup(config=config)

    @override(DataSourceAPI)
    def sample_from_data(self, batch_size: int) -> Dict[str, Any]:
        # Sample from the buffer `batch_size` many items.
        return self._data_source.sample(batch_size)

    def add(self, episodes: List[EpisodeType]) -> None:
        """Add new episodes to the buffer"""
        return self._data_source.add(episodes)


class OfflineDataAPI(DataSourceAPI):
    @override(DataSourceAPI)
    def _setup(self, config: AlgorithmConfig):
        # Set up the `OfflineData` as the data source.
        self._data_source = OfflineData(config)
        # Set up the `super` class.
        super()._setup(config=config)

    @override(DataSourceAPI)
    def sample_from_data(
        self,
        batch_size,
        **kwargs: Dict[str, Any],
    ) -> Union[Dict[str, Any], List[DataIterator]]:
        """Sample from the Ray Dataset."""
        return self._data_source.sample(batch_size, **kwargs)

    def cleanup(self):
        """Release all resources."""
        self._data_source = None
        # Clean up `super`.
        super().cleanup()
