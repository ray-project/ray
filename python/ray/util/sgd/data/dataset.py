from abc import ABC, abstractmethod

import random
import ray
import torch

from ray.util.sgd.data.utils import get_arr_partition


@ray.remote
def _load_remote(dataset, path):
    return dataset.load(path)


class Dataset(ABC, torch.utils.data.IterableDataset):
    """
    This class defines the Ray dataset abstration (primarily for SGD).
    It's primarily meant to be treated as an abstract class.
    """

    def __init__(self, loc, remote=False, max_paths=None, transform=None):
        """Needs to be initialized with a dataset location

        Args:
            loc: Location of training data (i.e. root of training directory)
                This is passed into get_paths
            remote: Whether dataset loading
                should be parallelized on multiple workers/nodes. For example,
                if only the driver has the necessary permissions to access
                the data, remote should be False
            max_paths: Maximum number of paths to load (passed to `get_paths`)

        """
        self._paths = self.get_paths(loc, max_paths=max_paths)
        self.transform = transform

        if remote:
            data = [_load_remote.remote(self, path) for path in self._paths]
        else:
            data = [ray.put(self.load(path)) for path in self._paths]

        random.seed(0)
        random.shuffle(data)
        self._data = data

    @abstractmethod
    def get_paths(self, loc, max_paths=None):
        """Should given a directory, this should return a list of paths to data points
        Note: This method should be overwritten

        Args:
            loc: Location of the data, passed in via initalizer
            max_files: Maximum number of paths to return (useful for debugging)

        Returns:
            paths: A list of the paths to each data point
        """
        assert False, "get_paths must be defined."

    @abstractmethod
    def load(self, path):
        """Gets the data located at a single path
        Note: This method should be overwritten

        Args:
            path: The path to a single piece of data

        Returns:
            input, label: A tuple of training data, label
        """
        assert False, "download must be defined"

    @abstractmethod
    def convert_data(self, data):
        """Preprocesses the input for a model. This will be run after shuffling.
        Note: This method should be overwritten

        Args:
            data: A single data point to be preprocessed
        """
        assert False, "This method should be overridden. if you really " \
            "don't want to do any preprocessing, you can replace this " \
            "with the identity function."

    @abstractmethod
    def convert_label(self, label):
        """Preprocesses the output of a model. This will be run after shuffling.
        Note: This method should be overwritten

        Args:
            label: A single label point to be preprocessed
        """
        assert False, "This method should be overridden. if you really " \
            "don't want to do any preprocessing, you can replace this " \
            "with the identity function."

    def _convert(self, data, label):
        data, label = self.convert_data(data), self.convert_label(label)
        if self.transform:
            data = self.transform(data)
        return data, label

    def get_data_loader(self, batch_size=32):
        materialized = list(self)
        loader = torch.utils.data.DataLoader(materialized, batch_size)
        return loader

    def get_sharded_loader(self, n, i, batch_size=32):
        """Get the ith shard of the dataset assuming it is sharded into
        n pieces"""

        def converter(x):
            data, label = ray.get(x)
            return self._convert(data, label)

        data = list(map(converter, get_arr_partition(self._data, n, i)))
        return torch.utils.data.DataLoader(data, batch_size)

    def __getitem__(self, idx):
        return self._convert(*ray.get(self._data[idx]))

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        for id in self._data:
            yield self._convert(*ray.get(id))
