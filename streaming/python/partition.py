import importlib
import inspect
from abc import ABC, abstractmethod

from ray import cloudpickle
from ray.streaming.runtime import gateway_client


class Partition(ABC):
    """Interface of the partitioning strategy."""

    @abstractmethod
    def partition(self, record, num_partition: int):
        """Given a record and downstream partitions, determine which partition(s)
         should receive the record.

        Args:
            record: The record.
            num_partition: num of partitions
        Returns:
            IDs of the downstream partitions that should receive the record.
         """
        pass


class BroadcastPartition(Partition):
    """Broadcast the record to all downstream partitions."""

    def __init__(self):
        self.__partitions = []

    def partition(self, record, num_partition: int):
        if len(self.__partitions) != num_partition:
            self.__partitions = list(range(num_partition))
        return self.__partitions


class KeyPartition(Partition):
    """Partition the record by the key."""

    def __init__(self):
        self.__partitions = [-1]

    def partition(self, key_record, num_partition: int):
        # TODO support key group
        self.__partitions[0] = abs(hash(key_record.key)) % num_partition
        return self.__partitions


class RoundRobinPartition(Partition):
    """Partition record to downstream tasks in a round-robin matter."""

    def __init__(self):
        self.__partitions = [-1]
        self.seq = 0

    def partition(self, key_record, num_partition: int):
        self.seq = (self.seq + 1) % num_partition
        self.__partitions[0] = self.seq
        return self.__partitions


class SimplePartition(Partition):
    """Wrap a python function as subclass of :class:`Partition`"""

    def __init__(self, func):
        self.func = func

    def partition(self, record, num_partition: int):
        return self.func(record, num_partition)


def serialize(partition_func):
    """
    Serialize the partition function so that it can be deserialized by
    :func:`deserialize`
    """
    return cloudpickle.dumps(partition_func)


def deserialize(partition_bytes):
    """Deserialize the binary partition function serialized by
    :func:`serialize`"""
    return cloudpickle.loads(partition_bytes)


def load_partition(descriptor_partition_bytes: bytes):
    """
    Deserialize `descriptor_partition_bytes` to get partition info, then
    get or load partition function.
    Note that this function must be kept in sync with
     `io.ray.streaming.runtime.python.GraphPbBuilder.serializePartition`

    Args:
        descriptor_partition_bytes: serialized partition info

    Returns:
        partition function
    """
    assert len(descriptor_partition_bytes) > 0
    partition_bytes, module_name, function_name =\
        gateway_client.deserialize(descriptor_partition_bytes)
    if partition_bytes:
        return deserialize(partition_bytes)
    else:
        assert module_name
        mod = importlib.import_module(module_name)
        assert function_name
        func = getattr(mod, function_name)
        # If func is a python function, user partition is a simple python
        # function, which will be wrapped as a SimplePartition.
        # If func is a python class, user partition is a sub class
        # of Partition.
        if inspect.isfunction(func):
            return SimplePartition(func)
        else:
            assert issubclass(func, Partition)
            return func()
