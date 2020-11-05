import random
from collections import defaultdict
from dataclasses import is_dataclass
from functools import wraps
from typing import Callable, List, Union, Iterable, Iterator

import pandas as pd
from pandas import DataFrame

from ray.util.iter import _NextValueNotReady, LocalIterator, ParallelIterator
from .dataset import Dataset
from ..iter import T, U


def item_check(item) -> bool:
    if (hasattr(item, "__getitem__") or hasattr(item, "__iter__")
            or is_dataclass(item)):
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
                    raise ValueError(
                        "DistributedDataset only support list like "
                        "item or dataclass instance")
        else:
            for item in it:
                if check_fn(item):
                    yield item
                else:
                    raise ValueError(
                        "DistributedDataset only support list like "
                        "item or dataclass instance")

    return wrapper


class DistributedDataset(Dataset[T]):
    """ A distributed dataset implemented based on ParallelIterator

    All item should be a list like object or dataclass instance.

    Args:
        para_it (ParallelIterator[T]): An existing parallel iterator, and each
            should be a list like object or dataclass instance.
        batch_size (int): The batch size of the item.
        add_type_check (bool): Whether we need to wrap the transform function
            with type check.
        item_check_fn (Callable[[T], bool]): A check function for each item.
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
        """
        Apply the fn function to the DistributedDataset
        Args:
            fn (Callable[[Iterable[T]], Iterable[U]]): The function to applied.
                The input is a iterable records, and the output is also a
                iterable records. However, the item of the output iterable
                should also be list like or dataclass instance.
            fn_name (str): the function name.
        Returns:
            A new DistributedDataset
        """
        if self._add_type_check:
            fn = type_check(fn, self._batch_size > 0, self._item_check_fn)
            fn_name = fn_name + ".type_check()"
        para_it = self._para_it._with_transform(
            lambda local_it: local_it.transform(fn), fn_name)
        return DistributedDataset(para_it, self._batch_size,
                                  self._add_type_check)

    def batch(self, batch_size: int) -> "DistributedDataset[U]":
        """
        Batch the DistributedDataset with the given batch size. It will return
        the current dataset if the batch size equals the current one, else will
        rebatch the dataset to the given batch size.
        Args:
            batch_size (int): the batch size
        Returns:
            The current DistributedDataset or rebatched DistributedDataset
        """
        if batch_size == self._batch_size:
            return self

        if self._batch_size == 0:
            para_it = self._para_it.batch(batch_size)
        else:
            para_it = self._para_it.flatten().batch(batch_size)
        return DistributedDataset(para_it, self._batch_size,
                                  self._add_type_check, self._item_check_fn)

    def shuffle(self, shuffle_batch_size: int,
                seed: int = None) -> "DistributedDataset[T]":
        """
        see ParallelIterator.local_shuffle
        Returns
            A shuffled DistributedDataset
        """
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
        """
        Get the given shard of the current dataset. The return is a iterator.
        We support shuffle the return iterator when each call iter on the
        return.
        Args:
            index (int): the shard index id
            batch_ms (int): Batches items for batch_ms milliseconds
                before retrieving it.
                Increasing batch_ms increases latency but improves throughput.
                If this value is 0, then items are returned immediately.
            num_async (int): The max number of requests in flight.
                Increasing this improves the amount of pipeline
                parallelism in the iterator.
            shuffle (bool): whether shuffle the given shard data
            shuffle_buffer_size (int): same as ParallelIterator.local_shuffle
            seed (int): the random seed
            inner_shuffle_fn (Callable[[T], T]): If the shuffle is True and
                current dataset is batched, this function will apply to the
                batched records.
        Returns:
            The given shard iterator. If the shuffle is True, each call iter
            will return a different ordered iterator.
        """
        if shuffle and self._batch_size > 0 and inner_shuffle_fn is None:
            shuffle_random = random.Random(seed)

            def fn(x):
                return shuffle_random.shuffle(x)

            inner_shuffle_fn = fn
        return _RepeatableIterator(self._para_it, index, batch_ms, num_async,
                                   shuffle, shuffle_buffer_size, seed,
                                   inner_shuffle_fn)

    def num_shards(self) -> int:
        return self._para_it.num_shards()

    def repartition(self, num_partitions: int,
                    batch_ms: int = 0) -> "DistributedDataset[T]":
        """see ParallelIterator.repartition"""
        para_it = self._para_it.repartition(num_partitions, batch_ms)
        return DistributedDataset(para_it, self._batch_size,
                                  self._add_type_check, self._item_check_fn)

    def to_pandas(self,
                  column_names: List[Union[int, str]],
                  batch_size: int = 32) -> "PandasDistributedDataset":
        """
        Convert the current DistributedDataset to PandasDistributedDataset. If
        the record is a iterable, we will convert to a list. If the record has
        __getitem__ attr, we will use __getitem__ to get the given column
        indexes data to create pandas DataFrame. If the record is dataclass
        instance we will use __getattr__ to get the given column.
        Args:
            column_names (List[Union[int, str]]): This should be a list of int
                if the item has __getitem__ attr. This should be a list of str
                if the item is a instance of dataclass. And the column names
                will be the pandas DataFrame column names.
            batch_size (int): batch the given size to create a pandas DataFrame
        Returns:
            A PandasDistributedDataset
        """
        typ = type(column_names[0])
        all_equals = all([isinstance(col, typ) for col in column_names])
        assert all_equals, "The column names should all be int or str"
        if isinstance(column_names[0], str):

            def get_column_fn(item, col):
                return getattr(item, col)
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
                    raise ValueError(
                        "DistributedDataset only support list like "
                        "item or dataclass instance")

                values = defaultdict(lambda x: [])
                for item in batch:
                    for col in column_names:
                        values[col].append(get_column_fn(item, col))
                yield DataFrame(values)

        ds = ds.transform(convert_fn, ".to_pandas()")
        return PandasDistributedDataset.from_distributed_ds(ds)

    def to_torch(self,
                 feature_columns=None,
                 feature_shapes=None,
                 feature_types=None,
                 label_column=None,
                 label_shape=None,
                 label_type=None):
        """
        Create a TorchDataset from the current DistributedDataset. This will
        convert to a PandasDistributedDataset first, and then convert a
        TorchDataset.
        Args:
            feature_columns (List[Union[int, str]]): the column indexes
                name. This is a list of int if the record is list like object.
                This is a list of str if the record is dataclass instance.
            feature_shapes (Optional[List[Any]]): the feature shapes matching
               the feature columns. One row will packet into one torch.Tensor
               if this is not provided. Otherwise, each feature column will be
               one torch.Tensor and with the provided shapes.
            feature_types (Optional[List["torch.dtype"]]): the feature types
               matching the feature columns. All feature will be cast into
               torch.float by default. Otherwise, cast into the provided type.
            label_column (Union[int, str]): the label index or name. This is a
               int index if the record is list like object. It should be str if
               the record is dataclass instance.
            label_shape (Optional[int]): the label shape.
            label_type (Optional["torch.dtype"]): the label type, this will be
               cast into torch.float by default
        Returns:
            A TorchDataset
        """
        column_names = feature_columns.copy()
        column_names.append(label_column)
        pandas_ds = self.to_pandas(column_names)
        return pandas_ds.to_torch(feature_columns, feature_shapes,
                                  feature_types, label_column, label_shape,
                                  label_type)

    def to_tf(self,
              feature_columns=None,
              feature_shapes=None,
              feature_types=None,
              label_column=None,
              label_shape=None,
              label_type=None):
        """
        Create a TFDataset from the current DistributedDataset. This will
        convert to a PandasDistributedDataset first, and then convert a
        TFDataset.
        Args:
            feature_columns (List[Union[int, str]]): the column indexes
                name. This is a list of int if the record is list like object.
                This is a list of str if the record is dataclass instance.
            feature_shapes (Optional[List[tf.TensorShape]]): the feature shapes
                matching the feature columns. One row will packet into one
                tf.Tensor if this is not provided. Otherwise, each feature
                column will be one tf.Tensor and with the provided shapes.
            feature_types (Optional[List["tf.DType"]]): the feature types
               matching the feature columns. All feature will be cast into
               tf.float by default. Otherwise, cast into the provided type.
            label_column (Union[int, str]): the label index or name. This is a
               int index if the record is list like object. It should be str if
               the record is dataclass instance.
            label_shape (Optional[tf.TensorShape]): the label shape.
            label_type (Optional["tf.DType"]): the label type, this will be
               cast into tf.float by default
        Returns:
            A TFDataset
        """
        column_names = feature_columns.copy()
        column_names.append(label_column)
        pandas_ds = self.to_pandas(column_names)
        return pandas_ds.to_tf(feature_columns, feature_shapes, feature_types,
                               label_column, label_shape, label_type)


class PandasDistributedDataset(Dataset[DataFrame]):
    """
    A distributed dataset implemented based on ParallelIterator. And each item
    is a pandas DataFrame.
    Args:
        para_it (ParallelIterator[pd.DataFrame]): An existing parallel
            iterator, and each should be a pandas DataFrame.
        batch_size (int): The batch size of the item.
        add_type_check (bool): Whether we need to wrap the transform function
            with type check.
        item_check_fn (Callable[[T], bool]): A check function for each item.
    """

    def __init__(self,
                 para_it: ParallelIterator[DataFrame],
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
            ds: DistributedDataset[DataFrame]) -> "PandasDistributedDataset":
        return PandasDistributedDataset(ds._para_it, ds._batch_size,
                                        ds._add_type_check)

    def transform(self, fn: Callable[[T], U],
                  fn_name: str) -> "PandasDistributedDataset":
        if self._add_type_check:
            fn = type_check(fn, self._batch_size > 0, self._item_check_fn)
            fn_name = fn_name + ".type_check()"
        para_it = self._para_it._with_transform(
            lambda local_it: local_it.transform(fn), fn_name)
        return PandasDistributedDataset(para_it, self._batch_size,
                                        self._add_type_check)

    def num_shards(self) -> int:
        return self._para_it.num_shards()

    def repartition(self, num_partitions: int,
                    batch_ms: int = 0) -> "PandasDistributedDataset":
        para_it = self._para_it.repartition(num_partitions, batch_ms)
        return PandasDistributedDataset(para_it, self._batch_size,
                                        self._add_type_check)

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

    def shuffle(self, shuffle_buffer_size: int,
                seed: int = None) -> "PandasDistributedDataset":
        """
        Unlike the ParallelIterator.local_shuffle. This shuffle will first
        apply the local_shuffle for each shards and then shuffle the each
        pandas DataFrame.
        """
        para_it = self._para_it.local_shuffle(shuffle_buffer_size, seed)

        def shuffle_fn(it: Iterable[DataFrame]) -> Iterable[DataFrame]:
            for df in it:
                df = df.sample(frac=1, random_state=seed)
                yield df

        para_it = para_it._with_transform(
            lambda local_it: local_it.transform(shuffle_fn),
            ".inner_pandas_shuffle()")
        return PandasDistributedDataset(para_it, self._batch_size,
                                        self._add_type_check,
                                        self._item_check_fn)

    def get_shard(
            self,
            shard_index: int,
            batch_ms: int = 0,
            num_async: int = 1,
            shuffle: bool = False,
            shuffle_buffer_size: int = 1,
            seed: int = None,
            inner_shuffle_fn: Callable[[T], T] = None) -> Iterator[DataFrame]:
        if shuffle and inner_shuffle_fn is None:

            def fn(df):
                return df.sample(frac=1, random_state=seed)

            inner_shuffle_fn = fn
        return _RepeatableIterator(self._para_it, shard_index, batch_ms,
                                   num_async, shuffle, shuffle_buffer_size,
                                   seed, inner_shuffle_fn)

    def to_pandas(self, batch_size: int = 32) -> "PandasDistributedDataset":
        return self.batch(batch_size)

    def to_torch(self,
                 feature_columns=None,
                 feature_shapes=None,
                 feature_types=None,
                 label_column=None,
                 label_shape=None,
                 label_type=None):
        """
        Create a TorchDataset from the current DistributedDataset.
        Args:
            feature_columns (List[Union[int, str]]): the column indexes
                name. This is a list of int if the record is list like object.
                This is a list of str if the record is dataclass instance.
            feature_shapes (Optional[List[Any]]): the feature shapes matching
               the feature columns. One row will packet into one torch.Tensor
               if this is not provided. Otherwise, each feature column will be
               one torch.Tensor and with the provided shapes.
            feature_types (Optional[List["torch.dtype"]]): the feature types
               matching the feature columns. All feature will be cast into
               torch.float by default. Otherwise, cast into the provided type.
            label_column (Union[int, str]): the label index or name. This is a
               int index if the record is list like object. It should be str if
               the record is dataclass instance.
            label_shape (Optional[int]): the label shape.
            label_type (Optional["torch.dtype"]): the label type, this will be
               cast into torch.float by default
        Returns:
            A TorchDataset
        """
        from ray.util.sgd.torch.torch_dataset import TorchDataset
        return TorchDataset(self, feature_columns, feature_shapes,
                            feature_types, label_column, label_shape,
                            label_type)

    def to_tf(self,
              feature_columns=None,
              feature_shapes=None,
              feature_types=None,
              label_column=None,
              label_shape=None,
              label_type=None):
        """
        Create a TFDataset from the current DistributedDataset. This will
        convert to a PandasDistributedDataset first, and then convert a
        TFDataset.
        Args:
            feature_columns (List[Union[int, str]]): the column indexes
                name. This is a list of int if the record is list like object.
                This is a list of str if the record is dataclass instance.
            feature_shapes (Optional[List[tf.TensorShape]]): the feature shapes
                matching the feature columns. One row will packet into one
                tf.Tensor if this is not provided. Otherwise, each feature
                column will be one tf.Tensor and with the provided shapes.
            feature_types (Optional[List["tf.DType"]]): the feature types
               matching the feature columns. All feature will be cast into
               tf.float by default. Otherwise, cast into the provided type.
            label_column (Union[int, str]): the label index or name. This is a
               int index if the record is list like object. It should be str if
               the record is dataclass instance.
            label_shape (Optional[tf.TensorShape]): the label shape.
            label_type (Optional["tf.DType"]): the label type, this will be
               cast into tf.float by default
        Returns:
            A TFDataset
        """
        from ray.util.sgd.tf.tf_dataset import TFDataset
        return TFDataset(self, feature_columns, feature_shapes, feature_types,
                         label_column, label_shape, label_type)


class _RepeatableIterator(Iterator[T]):
    """
    A repeatable iterator for the given shard index data. Each call
    iter(_RepeatableIterator instance) will shuffle the iterator and return a
    different order or data.
    Args:
        it (ParallelIterator[T]): a ParallelIterator to fetch the given shard
            data.
        shard_index (int): the shard index id.
        batch_ms (int): Batches items for batch_ms milliseconds
                before retrieving it.
                Increasing batch_ms increases latency but improves throughput.
                If this value is 0, then items are returned immediately.
        num_async (int): The max number of requests in flight.
            Increasing this improves the amount of pipeline
            parallelism in the iterator.
        shuffle (bool): whether shuffle the given shard data
        shuffle_buffer_size (int): same as ParallelIterator.local_shuffle
        seed (int): the random seed
        inner_shuffle_fn (Callable[[T], T]): If the shuffle is True and
            current dataset is batched, this function will apply to the
            batched records.
    """

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
                            shuffle_random.randint(0,
                                                   len(buffer) - 1))
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
