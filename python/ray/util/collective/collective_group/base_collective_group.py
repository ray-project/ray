"""Abstract class for collective groups."""
from abc import ABCMeta
from abc import abstractmethod

from ray.util.collective.types import (
    AllReduceOptions,
    BarrierOptions,
    ReduceOptions,
    AllGatherOptions,
    BroadcastOptions,
    ReduceScatterOptions,
)


class BaseGroup(metaclass=ABCMeta):
    def __init__(self, world_size, rank, group_name):
        """Init the process group with basic information.

        Args:
            world_size (int): The total number of processes in the group.
            rank (int): The rank of the current process.
            group_name (str): The group name.
        """
        self._world_size = world_size
        self._rank = rank
        self._group_name = group_name

    @property
    def rank(self):
        """Return the rank of the current process."""
        return self._rank

    @property
    def world_size(self):
        """Return the number of processes in this group."""
        return self._world_size

    @property
    def group_name(self):
        """Return the group name of this group."""
        return self._group_name

    def destroy_group(self):
        """GC the communicators."""
        pass

    @classmethod
    def backend(cls):
        """The backend of this collective group."""
        raise NotImplementedError()

    @abstractmethod
    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        raise NotImplementedError()

    @abstractmethod
    def barrier(self, barrier_options=BarrierOptions()):
        raise NotImplementedError()

    @abstractmethod
    def reduce(self, tensor, reduce_options=ReduceOptions()):
        raise NotImplementedError()

    @abstractmethod
    def allgather(self, tensor_list, tensor, allgather_options=AllGatherOptions()):
        raise NotImplementedError()

    @abstractmethod
    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        raise NotImplementedError()

    @abstractmethod
    def reducescatter(
        self, tensor, tensor_list, reducescatter_options=ReduceScatterOptions()
    ):
        raise NotImplementedError()

    @abstractmethod
    def send(self, tensor, dst_rank):
        raise NotImplementedError()

    @abstractmethod
    def recv(self, tensor, src_rank):
        raise NotImplementedError()
