import logging
from collections.abc import Iterable
from typing import Any, Callable, List, Optional

import numpy as np
import torch
from pandas import DataFrame
from torch.utils.data import IterableDataset

from ray.util.iter import LocalIterator
from ray.util.sgd.data.pandas_dataset import PandasDataset

from collections import Iterator

import functools


def convert_to_tensor(df, feature_columns: List[str],
                      feature_shapes: List[Any],
                      feature_types: List[torch.dtype], label_column: str,
                      label_shape: Optional[int], label_type: torch.dtype):
    feature_tensor = []
    for col, shape, dtype in zip(feature_columns, feature_shapes,
                                 feature_types):
        column = df[col].values
        if column.dtype == np.object:
            if isinstance(column[0], np.ndarray):
                column = np.stack(column)
            elif isinstance(column[0], (list, tuple)):
                column = list(column)
            else:
                raise Exception(
                    f"Column {col}'s type: {type(column[0])} is not supported. It must "
                    "be numpy built in type or numpy object of (ndarray, list, tuple)"
                )

        t = torch.as_tensor(column, dtype=dtype)
        if shape is not None:
            t = t.view(*(-1, *shape))
        feature_tensor.append(t)

    label_df = df[label_column].values
    label_tensor = torch.as_tensor(label_df, dtype=label_type)
    if label_shape:
        label_tensor = label_tensor.view(-1, label_shape)
    return feature_tensor, label_tensor


class TorchDataset:
    def __init__(self,
                 pandas_ds: PandasDataset = None,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_shape: Optional[int] = None,
                 label_type: Optional[torch.dtype] = None):

        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_shape = label_shape
        self._label_type = label_type

        self._check_and_convert()

        self._ds = pandas_ds

    def _check_and_convert(self):
        # convert to list for convenience
        if not isinstance(self._feature_columns, list):
            self._feature_columns = [self._feature_columns]

        if self._feature_shapes:
            if not isinstance(self._feature_shapes, list):
                self._feature_shapes = [self._feature_shapes]

            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"
            for i in range(len(self._feature_shapes)):
                if not isinstance(self._feature_shapes[i], Iterable):
                    self._feature_shapes[i] = [self._feature_shapes[i]]
        else:
            self._feature_shapes = [None] * len(self._feature_columns)

        if self._feature_types:
            if not isinstance(self._feature_types, list):
                self._feature_types = [self._feature_types]

            assert len(self._feature_columns) == len(self._feature_types), \
                "The feature_types size must match the feature_columns"
            for i in range(len(self._feature_types)):
                assert all(isinstance(dtype, torch.dtype) for dtype in self._feature_types), \
                    "All value in feature_types should be torch.dtype instance"
        else:
            self._feature_types = [torch.float] * len(self._feature_columns)

        if not self._label_type:
            self._label_type = torch.float

    def set_num_shards(self, num_shards):
        """
        Reshards the iterator if necessary.
        """
        if num_shards != self._ds.num_shards():
            logging.info("Setting num shards", num_shards)
            self._ds = self._ds.repartition(num_shards)

    def get_shard(self,
                  shard_index: int,
                  batch_ms: int = 0,
                  num_async: int = 1,
                  shuffle: bool = False,
                  shuffle_buffer_size: int = 1,
                  seed: int = None) -> torch.utils.data.IterableDataset:

        it = self._ds.get_shard(shard_index, batch_ms, num_async, shuffle,
                                shuffle_buffer_size, seed)
        convert_fn = functools.partial(
            convert_to_tensor,
            feature_columns=self._feature_columns,
            feature_shapes=self._feature_shapes,
            feature_types=self._feature_types,
            label_column=self._label_column,
            label_shape=self._label_shape,
            label_type=self._label_type)
        return TorchIterableDataset(it, convert_fn)


class TorchIterableDataset(IterableDataset):
    def __init__(self, it: Iterator[DataFrame],
                 convert_fn: Callable[[DataFrame], Any]):
        super().__init__()
        self._it = it
        self._convert_fn = convert_fn

    def __iter__(self):
        for df in self._it:
            yield self._convert_fn(df)
