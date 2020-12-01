"""Abstract class for collective groups."""
from abc import ABCMeta
from abc import abstractmethod
import six

from python.ray.util.collective.types import AllReduceOptions, BarrierOptions


class BaseGroup(six.with_metaclass(ABCMeta)):
    def __init__(self, world_size, rank, group_name):
        """
        Init the process group with basic information.

        Args:
            world_size:
            rank:
            group_name:
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

    def destroy_group(self):
        """GC the communicators."""
        pass

    @abstractmethod
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

    # @abstractmethod
    # def reduce(self, tensor, dst, op=SUM, group_name='default'):
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def allgather(self, tensor_list, tensor, gropu_name='default'):
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def gather(self, tensor, gather_list=None, dst=0, group_name='default'):
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def scatter(self, tensor, scatter_list=None, src=0, group_name='default'):
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def barrier(self, group_name='default'):
    #     raise NotImplementedError()

    # @abstractmethod
    # def send(self):
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def recv(self):
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def broadcast(self):
    #     raise NotImplementedError()
