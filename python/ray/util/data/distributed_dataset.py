from typing import Any, Callable, List, Optional, Iterable, Iterator

from .dataset import Dataset
from ..iter import T, U
from ray.util.iter import _NextValueNotReady, LocalIterator, ParallelIterator
from dataclasses import dataclass, is_dataclass
from functools import wraps

import pandas as pd
from pandas import DataFrame

from collections import defaultdict

import random


def item_check(item) -> bool:
    if (hasattr(item, "__getitem__") or
       hasattr(item, "__iter__") or is_dataclass(item)):
        return True
    else:
        return False


def type_check(func, is_batch: bool, check_fn: Callable[[T], bool]):
    @wraps(func)
    def wrapper(*args, **kwargs):
        it = func(*args, **kwargs)
        if is_batch:
            for item in it:
                if all([check_fn(inner) for inner in item]):
                    yield item
                else:
                    raise ValueError("DistributedDataset only support list like "
                                     "item or dataclass instance")
        else:
            for item in it:
                if check_fn(item):
                    yield item
                else:
                    raise ValueError("DistributedDataset only support list like "
                                     "item or dataclass instance")

    return wrapper


class DistributedDataset(Dataset[T]):
    """ A distributed dataset implemented based on ParallelIterator

    All item should be a dataclass
    """

    def __init__(self,
                 para_it: ParallelIterator[T],
                 batch_size: int = 0,
                 add_type_check: bool = False,
                 item_check_fn: Callable[[T], bool] = item_check):
        self._para_it = para_it
        self._batch_size = batch_size
        self._add_type_check = add_type_check
        self._item_check_fn = item_check_fn

    def transform(self,
                  fn: Callable[[Iterable[T]], Iterable[U]],
                  fn_name=".transform()") -> "DistributedDataset[U]":
        if self._add_type_check:
            fn = type_check(fn, self._batch_size > 0, self._item_check_fn)
            fn_name = fn_name + ".type_check()"
        para_it = self._para_it._with_transform(
            lambda local_it: local_it.transform(fn), fn_name)
        return DistributedDataset(
            para_it, self._batch_size, self._add_type_check)

    def batch(self, batch_size: int) -> "DistributedDataset[U]":
        if batch_size == self._batch_size:
            return self

        if self._batch_size == 0:
            para_it = self._para_it.batch(batch_size)
        else:
            para_it = self._para_it.flatten().batch(batch_size)
        return DistributedDataset(para_it, self._batch_size,
                                  self._add_type_check, self._item_check_fn)

    def shuffle(self, shuffle_batch_size: int, seed: int = None) -> "DistributedDataset[T]":
        para_it = self._para_it.local_shuffle(shuffle_batch_size, seed)
        return DistributedDataset(para_it, self._batch_size,
                                  self._add_type_check, self._item_check_fn)

    def get_shard(self,
                  index: int,
                  batch_ms: int = 0,
                  num_async: int = 1,
                  shuffle: bool = False,
                  shuffle_buffer_size: int = 1,
                  seed: int = None,
                  inner_shuffle_fn: Callable[[T], T] = None) -> Iterator[T]:
        if shuffle and self._batch_size > 0:
            shuffle_random = random.Random(seed)
            inner_shuffle_fn = lambda x: shuffle_random.shuffle(x)
        return _RepeatableIterator(
            self._para_it, index, batch_ms, num_async, shuffle,
            shuffle_buffer_size, seed, inner_shuffle_fn)

    def num_shards(self) -> int:
        return self._para_it.num_shards()

    def repartition(self, num_partitions: int, batch_ms: int = 0) -> "DistributedDataset[T]":
        para_it = self._para_it.repartition(num_partitions, batch_ms)
        return DistributedDataset(para_it, self._batch_size,
                                  self._add_type_check, self._item_check_fn)

    def to_pandas(self,
                  column_names: List[Optional[int, str]],
                  batch_size: int = 32) -> "PandasDistributedDataset":
        typ = type(column_names[0])
        all_equals = all([isinstance(col, typ) for col in column_names])
        assert all_equals
        if isinstance(column_names[0], str):
            def get_column_fn(item, col):
                return get_column_fn(item, col)
        elif isinstance(column_names[0], int):
            def get_column_fn(item, col):
                return item[col]
        else:
            raise ValueError("The column name only support str or int type")
        ds = self.batch(batch_size)

        def convert_fn(it: Iterable[T]) -> Iterable[DataFrame]:
            for batch in it:
                assert isinstance(batch, list)
                if hasattr(batch[0], "__getitem__") or is_dataclass(batch[0]):
                    batch = batch
                elif hasattr(batch[0], "__iter__"):
                    batch = [batch]
                else:
                    raise ValueError("DistributedDataset only support list like "
                                     "item or dataclass instance")

                values = defaultdict(lambda x: [])
                for item in batch:
                    for col in column_names:
                        values[col].append(get_column_fn(item, col))
                yield DataFrame(values)
        ds = ds.transform(convert_fn, ".to_pandas()")
        return PandasDistributedDataset.from_distributed_ds(ds)

    def to_torch(self,
                 feature_columns: List[Optional[int, str]] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List["torch.dtype"]] = None,
                 label_column: Optional[int, str] = None,
                 label_shape: Optional[int] = None,
                 label_type: Optional["torch.dtype"] = None):
        column_names = feature_columns.copy()
        column_names.append(label_column)
        pandas_ds = self.to_pandas(column_names)
        return pandas_ds.to_torch(feature_columns, feature_shapes, feature_types,
                                  label_column, label_shape, label_type)

    def to_tf(self,
              feature_columns: List[str],
              feature_shapes: List["tensorflow.TensorShape"],
              feature_types: List["tensorflow.DType"],
              label_column: str,
              label_shape: "tensorflow.TensorShape",
              label_type: "tensorflow.DType"):
        column_names = feature_columns.copy()
        column_names.append(label_column)
        pandas_ds = self.to_pandas(column_names)
        return pandas_ds.to_tf(feature_columns, feature_shapes, feature_types,
                               label_column, label_shape, label_type)


class PandasDistributedDataset(Dataset[DataFrame]):
    def __init__(self,
                 para_it: ParallelIterator[T],
                 batch_size: int = 0,
                 add_type_check: bool = False,
                 item_check_fn=lambda item: isinstance(item, DataFrame)):
        super(Dataset, self).__init__()
        self._para_it = para_it
        self._batch_size = batch_size
        self._add_type_check = add_type_check
        self._item_check_fn = item_check_fn

    @staticmethod
    def from_distributed_ds(
        ds: DistributedDataset[DataFrame]
    ) -> "PandasDistributedDataset":
        return PandasDistributedDataset(
            ds._para_it, ds._batch_size, ds._add_type_check)

    def transform(
        self, fn: Callable[[T], U], fn_name: str
    ) -> "PandasDistributedDataset":
        if self._add_type_check:
            fn = type_check(fn, self._batch_size > 0, self._item_check_fn)
            fn_name = fn_name + ".type_check()"
        para_it = self._para_it._with_transform(
            lambda local_it: local_it.transform(fn), fn_name)
        return PandasDistributedDataset(
            para_it, self._batch_size, self._add_type_check)

    def num_shards(self) -> int:
        return self._para_it.num_shards()

    def repartition(
        self, num_partitions: int, batch_ms: int = 0
    ) -> "PandasDistributedDataset":
        para_it = self._para_it.repartition(num_partitions, batch_ms)
        return PandasDistributedDataset(
            para_it, self._batch_size, self._add_type_check)

    def batch(self, batch_size: int) -> "PandasDistributedDataset":
        """
        Unlike the ParallelIterator.batch. This method rebatch the underlying
        the pandas DataFrame, and each pandas DataFrame will have batch_size
        rows.
        """
        if batch_size == self._batch_size:
            return self

        def batch_fn(it: Iterable[DataFrame]) -> Iterable[DataFrame]:
            it = iter(it)
            cur_df = None
            cur_index = 0
            cur_size = 0
            return_df = None
            while True:
                try:
                    cur_df = next(it)
                    while cur_df or (cur_index + batch_size) < cur_size:
                        if not cur_df or cur_index == cur_size:
                            cur_df = next(it)
                            cur_index = 0
                            cur_size = cur_df.shape[0]
                        if return_df:
                            ri = cur_index + batch_size - return_df.shape[0]
                            ri = min(ri, cur_size)
                            tmp = cur_df.iloc[cur_index, ri]
                            return_df = pd.concat([return_df, tmp])
                            cur_index = ri
                        else:
                            ri = cur_index + batch_size
                            ri = min(ri, cur_size)
                            return_df = cur_df.iloc[cur_index:ri]
                            cur_index = ri
                        if return_df.shape[0] == batch_size:
                            yield return_df
                            return_df = None
                except StopIteration:
                    break

            if return_df:
                return_df.index = range(return_df.shape[0])
                yield return_df

        return self.transform(batch_fn, f".batch({batch_size})")

    def shuffle(self, shuffle_buffer_size: int, seed: int = None) -> "PandasDistributedDataset":
        para_it = self._para_it.local_shuffle(shuffle_buffer_size, seed)

        def shuffle_fn(it: Iterable[DataFrame]) -> Iterable[DataFrame]:
            for df in it:
                df = df.sample(frac=1, random_state=seed)
                yield df
        para_it = para_it._with_transform(
            lambda local_it: local_it.transform(shuffle_fn), ".inner_pandas_shuffle()")
        return PandasDistributedDataset(
            para_it, self._batch_size, self._add_type_check, self._item_check_fn)

    def get_shard(self,
                  shard_index: int,
                  batch_ms: int = 0,
                  num_async: int = 1,
                  shuffle: bool = False,
                  shuffle_buffer_size: int = 1,
                  seed: int = None,
                  inner_shuffle_fn: Callable[[T], T] = None) -> Iterator[DataFrame]:
        if shuffle and inner_shuffle_fn is None:
            inner_shuffle_fn = lambda df: df.sample(frac=1, random_state=seed)
        return _RepeatableIterator(
            self._para_it, shard_index, batch_ms, num_async,
            shuffle, shuffle_buffer_size, seed, inner_shuffle_fn)

    def to_pandas(self,
                  column_names: List[Optional[int, str]],
                  batch_size: int = 32) -> "PandasDistributedDataset":
        return self.batch(batch_size)

    def to_torch(self,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List["torch.dtype"]] = None,
                 label_column: str = None,
                 label_shape: Optional[int] = None,
                 label_type: Optional["torch.dtype"] = None) -> "TorchDataset":
        from ray.util.sgd.torch.torch_dataset import TorchDataset
        return TorchDataset(self, feature_columns, feature_shapes,
                            feature_types, label_column, label_shape,
                            label_type)

    def to_tf(self,
              feature_columns: List[str],
              feature_shapes: List["tensorflow.TensorShape"],
              feature_types: List["tensorflow.DType"], label_column: str,
              label_shape: "tensorflow.TensorShape",
              label_type: "tensorflow.DType"):
        from ray.util.sgd.tf.tf_dataset import TFDataset
        return TFDataset(self, feature_columns, feature_shapes, feature_types,
                         label_column, label_shape, label_type)


class _RepeatableIterator(Iterator[T]):
    def __init__(self,
                 it: ParallelIterator[T],
                 shard_index: int,
                 batch_ms: int = 0,
                 num_async: int = 1,
                 shuffle: bool = False,
                 shuffle_buffer_size: int = 1,
                 seed: int = None,
                 inner_shuffle_fn: Callable[[T], T] = None):
        super(_RepeatableIterator, self).__init__()
        self._it = it
        self._shard_index = shard_index
        self._batch_ms = batch_ms
        self._num_async = num_async
        self._shuffle = shuffle
        self._shuffle_buffer_size = shuffle_buffer_size
        self._seed = seed
        self._inner_shuffle_fn = inner_shuffle_fn
        if shuffle and inner_shuffle_fn is None:
            self._inner_shuffle_fn = lambda x: x

        self._local_it: LocalIterator[T] = None

    def __next__(self) -> T:
        assert self._local_it is not None
        return next(self._local_it)

    def __iter__(self) -> Iterator[T]:
        it = self._it.get_shard(self._shard_index, self._batch_ms,
                                self._num_async)
        if self._shuffle:
            it = self.shuffle(it)

        self._local_it = it
        return self

    def shuffle(self, local_it: LocalIterator[T]) -> LocalIterator[DataFrame]:
        shuffle_random = random.Random(self._seed)

        def apply_shuffle(it):
            buffer = []
            for item in it:
                if isinstance(item, _NextValueNotReady):
                    yield item
                else:
                    buffer.append(item)
                    if len(buffer) >= self._shuffle_buffer_size:
                        item = buffer.pop(
                            shuffle_random.randint(0, len(buffer) - 1))
                        item = self._inner_shuffle_fn(item)
                        yield item
            while len(buffer) > 0:
                item = buffer.pop(shuffle_random.randint(0, len(buffer) - 1))
                item = self._inner_shuffle_fn(item)
                yield item

        return LocalIterator(
            local_it.base_iterator,
            local_it.shared_metrics,
            local_it.local_transforms + [apply_shuffle],
            name=local_it.name +
                 ".shuffle(shuffle_buffer_size={}, seed={})".format(
                     self._shuffle_buffer_size,
                     str(self._seed) if self._seed is not None else "None"))
