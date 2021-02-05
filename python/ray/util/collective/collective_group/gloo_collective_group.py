import logging
import datetime
import time
from typing import ContextManager

import ray
import pygloo

from ray.util.collective.collective_group import gloo_util
from ray.util.collective.collective_group.base_collective_group \
    import BaseGroup
from ray.util.collective.types import AllReduceOptions, \
    BarrierOptions, Backend
from ray.util.collective.const import get_gloo_store_name

logger = logging.getLogger(__name__)


class Rendezvous:
    """
    A rendezvous class for different actor/task processes to meet.

    To initialize an GLOO collective communication group, different
    actors/tasks spawned in Ray in a collective group needs to meet
    each other to synchronize the GLOOUniqueID. This class guarantees
    they meet via the GLOOUniqueIDStore, initialized on the rank=0
    process.

    Args:
        group_name (str): the unique user-specified group name.
    """

    def __init__(self, group_name, context, store_type, device_type):
        self._group_name = group_name
        self._context = context
        self.create_store(store_type)
        self.create_device(device_type)

    def create_store(self, store_type):
        if store_type == "file":
            store_name = get_gloo_store_name(self._group_name)
            fileStore_path = gloo_util.get_gloo_store_path(store_name)  # Multi-machines needs nfs.
            self._store = pygloo.rendezvous.FileStore(fileStore_path)
        elif store_type == "hash":
            raise RuntimeError("No implementation for hash store")
        elif store_type == "redis":
            raise RuntimeError("No implementation for redis store")
        else:
            raise RuntimeError(f"Unregenize store type: {store_type}")

    def create_device(self, device_type):
        if device_type == "tcp":
            attr = pygloo.transport.tcp.attr("localhost")
            # Perform rendezvous for TCP pairs
            self._device = pygloo.transport.tcp.CreateDevice(attr)
        elif device_type == "uv":
            raise RuntimeError("No implementation for redis store")

    def meet(self):
        """
        Meet at the named actor store.

        Args:
            timeout_s: timeout in seconds.

        Return:
            None
        """
        self._context.connectFullMesh(self._store, self._device)

    @property
    def store(self):
        return self._store

    @property
    def store(self):
        return self._device

class GLOOGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name, store_type = 'file', device_type = 'tcp'):
        """
        Init an GLOO collective group.

        Args:
            world_size (int): The number of processes.
            rank (int): The id of process
            group_name (str): the unique user-specified group name.
            store_type (str): the store type. Optional: "file", "redis".
            device_type (str): the device type to transport. Optional: "tcp", "uv".
        """
        super(GLOOGroup, self).__init__(world_size, rank, group_name)

        self._gloo_context = self._get_gloo_context();

        self._rendezvous = Rendezvous(self.group_name, self._gloo_context, store_type, device_type)
        self._rendezvous.meet()

        self._store_path = None

    def destroy_group(self):
        """
        Destroy the group and release the GLOO communicators safely.
        """
        if self._gloo_context is not None:
            pygloo.barrier(self._gloo_context)
            # destroy the communicator
            self._gloo_context = None
        super(GLOOGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.GLOO

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        """
        AllReduce a list of tensors following options.

        Args:
            tensor: the tensor to be reduced, each tensor locates on a GPU
            allreduce_options:

        Returns:
        """
        # obtain the stream: using default stream by now
        # TODO(Hao): implement a simple stream manager here

        dtype = gloo_util.get_gloo_tensor_dtype(tensor)
        ptr = gloo_util.get_tensor_ptr(tensor)
        n_elems = gloo_util.get_tensor_n_elements(tensor)
        reduce_op = gloo_util.get_gloo_reduce_op(allreduce_options.reduceOp)

        # in-place allreduce
        pygloo.allreduce(self._gloo_context, ptr, ptr, n_elems, dtype, reduce_op)

    def barrier(self, barrier_options=BarrierOptions()):
        """
        Blocks until all processes reach this barrier.

        Args:
            barrier_options:

        Returns:
        """
        self.allreduce(self._barrier_tensor)

    def _get_gloo_context(self):
        """
        Create or use a cached GLOO communicator for the collective task.

        """
        context = gloo_util.create_gloo_context(
            self.world_size, self.rank)
        return context

    # def _collective_call(self, *args):
    #     """Private method to encapsulate all collective calls"""
    #     pass
