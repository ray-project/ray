from typing import Callable, Generic
from ray.util.iter import T, U


class Dataset(Generic[T]):
    def transform(self, fn: Callable[[T], U], fn_name: str) -> "Dataset[U]":
        raise NotImplementedError

    def batch(self, batch_size: int) -> "Dataset[U]":
        raise NotImplementedError

    def shuffle(self, *args, **kwargs) -> "Dataset[T]":
        raise NotImplementedError

    def get_shard(self, index: int, **kwargs):
        raise NotImplementedError

    def num_shards(self) -> int:
        raise NotImplementedError

    def repartition(self, num_partitions: int, **kwargs) -> "Dataset[T]":
        raise NotImplementedError

    def to_torch(self, *args, **kwargs):
        raise NotImplementedError

    def to_tf(self, *args, **kwargs):
        raise NotImplementedError

    def __iter__(self):
        raise Exception("Unsupported operation")
