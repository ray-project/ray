import logging
import datetime
import time
from typing import ContextManager
import os
import shutil

import ray
from ray import ray_constants
import pygloo
import numpy

from ray.util.collective.collective_group import gloo_util
from ray.util.collective.collective_group.base_collective_group \
    import BaseGroup
from ray.util.collective.types import AllReduceOptions, \
    BarrierOptions, Backend, ReduceOptions, BroadcastOptions, \
    AllGatherOptions, ReduceScatterOptions, SendOptions, \
    RecvOptions


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
        self._ip_address = ray._private.services.get_node_ip_address()
        self._store_type = store_type
        self._device_type =device_type
        self.create_store(store_type)
        self.create_device(device_type)

    def create_store(self, store_type):
        if store_type == "redis":
            redisStore = pygloo.rendezvous.RedisStore(self._ip_address, 6379) # redis_port use the default port. If the redis port of ray isn't 6379, might raise error. Need connect to ray's redis_port variable to fix it.
            redis_password = ray_constants.REDIS_DEFAULT_PASSWORD
            redisStore.authorize(redis_password)

            self._store = pygloo.rendezvous.PrefixStore(self._group_name, redisStore)

        elif store_type == "file":
            store_name = get_gloo_store_name(self._group_name)
            store_path = gloo_util.get_gloo_store_path(store_name)
            if self._context.rank == 0:
                if not os.path.exists(store_path):
                    os.makedirs(store_path)
                elif os.listdir(store_path) and os.listdir(store_path):
                    shutil.rmtree(store_path)
                    os.makedirs(store_path)
            else:
                import time
                while not os.path.exists(store_path):
                    time.sleep(0.1)

            fileStore = pygloo.rendezvous.FileStore(store_path) # Multi-machines needs nfs.
            self._store = pygloo.rendezvous.PrefixStore(self._group_name, fileStore)
        elif store_type == "hash":
            raise RuntimeError("No implementation for hash store")
        else:
            raise RuntimeError(f"Unrecognized store type: {store_type}")

    def create_device(self, device_type):
        if device_type == "tcp":
            attr = pygloo.transport.tcp.attr(self._ip_address)
            self._device = pygloo.transport.tcp.CreateDevice(attr)
        elif device_type == "uv":
            raise RuntimeError("No implementation for uv")

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
    def store_type(self):
        return self._store_type

    @property
    def store(self):
        return self._store

    @property
    def device_type(self):
        return self._device_type

    @property
    def device(self):
        return self._device

class GLOOGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name, store_type = 'redis', device_type = 'tcp'):
        """
        Init an GLOO collective group.

        Args:
            world_size (int): The number of processes.
            rank (int): The id of process
            group_name (str): the unique user-specified group name.
            store_type (str): the store type. Optional: "file", "redis", "hash".
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

        if self.rank == 0 and self._rendezvous.store_type == "file" :
            store_name = get_gloo_store_name(self._group_name)
            store_path = gloo_util.get_gloo_store_path(store_name)
            if os.path.exists(store_path):
                shutil.rmtree(store_path)

        super(GLOOGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.GLOO

    def allreduce(self, tensors, allreduce_options=AllReduceOptions()):
        """
        AllReduce a list of tensors following options.

        Args:
            tensor: the tensor to be reduced, each tensor locates on a GPU
            allreduce_options:

        Returns:
        """

        def collective_fn(input_tensor, output_tensor, context):
            context.allreduce(
                gloo_util.get_tensor_ptr(input_tensor),
                gloo_util.get_tensor_ptr(output_tensor),
                gloo_util.get_tensor_n_elements(input_tensor),
                gloo_util.get_gloo_tensor_dtype(input_tensor),
                gloo_util.get_gloo_reduce_op(allreduce_options.reduceOp))

        self._collective(tensors, tensors, collective_fn)


    def barrier(self, barrier_options=BarrierOptions()):
        """
        Blocks until all processes reach this barrier.

        Args:
            barrier_options:

        Returns:
        """
        barrier_tensor = numpy.array([1])
        self.allreduce([barrier_tensor])

    def reduce(self, tensors, reduce_options=ReduceOptions()):
        """Reduce tensors following options.

        Args:
            tensors (List): the list of tensors to be reduced, this list only have one tensor.
            reduce_options: reduce options.

        Returns:
            None
        """
        root_rank = reduce_options.root_rank

        def collective_fn(input_tensor, output_tensor, context):
            context.reduce(
                gloo_util.get_tensor_ptr(input_tensor),
                gloo_util.get_tensor_ptr(output_tensor),
                gloo_util.get_tensor_n_elements(input_tensor),
                gloo_util.get_gloo_tensor_dtype(input_tensor),
                gloo_util.get_gloo_reduce_op(reduce_options.reduceOp),
                root_rank)

        self._collective(tensors, tensors, collective_fn)

    def _get_gloo_context(self):
        """
        Create or use a cached GLOO communicator for the collective task.

        """
        context = gloo_util.create_gloo_context(
            self.world_size, self.rank)
        return context

    def _collective(self,
                    input_tensors,
                    output_tensors,
                    collective_fn,
                    preprocess_fn=None,
                    postprocess_fn=None):
        """A method to encapsulate all collective calls.

        Args:
            input_tensors: the list of the input tensors.
            output_tensors: the list of the output tensors.
            collective_fn: the collective function call.
            preprocess_fn: preprocess procedures before collective calls.
            postprocess_fn: postprocess procedures after collective calls.

        Returns:
            None
        """
        _check_cpu_tensors(input_tensors)
        _check_cpu_tensors(output_tensors)

        # devices = gloo_util.get_tensor_device_list(input_tensors)
        # key = _get_comm_key_from_devices(devices)
        # comms = self._get_nccl_collective_communicator(key, devices)
        # streams = self._dev_streams_map[key]

        # TODO(Hao): sync streams and events
        # self._sync_streams()

        # Make the collective call
        # if preprocess_fn:
            # preprocess_fn(streams)
        # nccl_util.groupStart()
        for i, tensor in enumerate(input_tensors):
            collective_fn(tensor, output_tensors[i], comms[i], streams[i])
        # nccl_util.groupEnd()
        # if postprocess_fn:
        #     postprocess_fn(streams)


def _check_cpu_tensors(tensors):
    """Check only have one tensor and located on CPU."""
    if not tensors or not isinstance(tensors, list):
        raise RuntimeError("'tensors' must be a nonempty list.")
    if len(tensors) != 1:
        raise RuntimeError("Tensor list only accept one tensor in the tensor list."
                           " Got {} != 1.".format(
                               len(tensors)))
    for tensor in tensors:
        if torch_available() and isinstance(tensor, torch.Tensor):
            device = tensor.device.index
            if not isinstance(device, int):
                raise RuntimeError("The tensor is not on a valid GPU.")