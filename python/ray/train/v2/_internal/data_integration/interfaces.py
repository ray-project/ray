from dataclasses import dataclass
from typing import Callable, Protocol, Union

from ray.data import DataIterator, Dataset

# A type representing either a ray.data.Dataset or a function that returns a
# ray.data.Dataset and accepts no arguments.
GenDataset = Union[Dataset, Callable[[], Dataset]]


@dataclass
class DatasetShardMetadata:
    """Metadata about a dataset shard used for lookup and configuration."""

    dataset_name: str


class DatasetShardProvider(Protocol):
    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> DataIterator:
        """Get the dataset shard for the given dataset info.
        Args:
            dataset_info: The metadata of the shard to retrieve,
                including the dataset name.
        Returns:
            The :class:`~ray.data.DataIterator` shard for the given dataset info.
        Raises:
            KeyError: If the dataset shard for the given dataset info is not found.
        """
        ...
