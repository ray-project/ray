import logging
import datetime
from sys import prefix
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
        self._ip_address, self._redis_port = ray.worker._global_node.redis_address.split(":")

        ray._private.services.get_node_ip_address()
        self._store_type = store_type
        self._device_type = device_type
        self._store = None
        self._device =None

        self.create_store(store_type)
        self.create_device(device_type)

    def create_store(self, store_type):
        if store_type == "redis":
            redisStore = pygloo.rendezvous.RedisStore(self._ip_address, int(self._redis_port))

            redis_password = ray_constants.REDIS_DEFAULT_PASSWORD
            redisStore.authorize(redis_password)
            # self._store = redisStore
            print(self._group_name.split("_")[0])
            # self._store = pygloo.rendezvous.PrefixStore(self._group_name, redisStore)  # worker died with no reason. This affect multi-task appears in one application.
            self._store = pygloo.rendezvous.PrefixStore(self._group_name, redisStore)  # worker died with no reason. This affect multi-task appears in one application.
            print("prefix")
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
        if self._store and self._device:
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
            store_type (str): the store type. Optional: "redis", "file", "hash".
            device_type (str): the device type to transport. Optional: "tcp", "uv".
        """
        super(GLOOGroup, self).__init__(world_size, rank, group_name)

        self._gloo_context = self._get_gloo_context()

        self._rendezvous = Rendezvous(self.group_name, self._gloo_context, store_type, device_type)
        self._rendezvous.meet()

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
            pygloo.allreduce(
                context,
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
            pygloo.reduce(
                context,
                gloo_util.get_tensor_ptr(input_tensor),
                gloo_util.get_tensor_ptr(output_tensor),
                gloo_util.get_tensor_n_elements(input_tensor),
                gloo_util.get_gloo_tensor_dtype(input_tensor),
                gloo_util.get_gloo_reduce_op(reduce_options.reduceOp),
                root_rank)

        self._collective(tensors, tensors, collective_fn)

    def broadcast(self, tensors, broadcast_options=BroadcastOptions()):
        """Broadcast tensors to all other gpus following options.

        Args:
            tensors (List): tensors to be broadcast or received.
            broadcast_options: broadcast options.

        Returns:
            None
        """
        root_rank = broadcast_options.root_rank

        def collective_fn(input_tensor, output_tensor, context):
            pygloo.broadcast(
                context,
                gloo_util.get_tensor_ptr(input_tensor),
                gloo_util.get_tensor_ptr(output_tensor),
                gloo_util.get_tensor_n_elements(input_tensor),
                gloo_util.get_gloo_tensor_dtype(input_tensor),
                root_rank)

        self._collective(tensors, tensors, collective_fn)

    def allgather(self,
                  tensor_lists,
                  tensors,
                  allgather_options=AllGatherOptions()):
        """Allgather tensors across gpus into a list of tensors.

        Args:
            tensor_lists (List[List[Tensor]]): allgathered tensors.
            tensors: the list of tensors to allgather across the group.
                     Each tensor must lolcate on a GPU of the process.
            allgather_options: allgather options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, context):
            pygloo.allgather(
                context,
                gloo_util.get_tensor_ptr(input_tensor),
                gloo_util.get_tensor_ptr(output_tensor),
                gloo_util.get_tensor_n_elements(input_tensor),
                gloo_util.get_gloo_tensor_dtype(input_tensor))

        _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists)
        output_flattened = [
            _flatten_for_scatter_gather(tensor_list, copy=False)
            for tensor_list in tensor_lists
        ]

        def postprocess_fn():
            for i, tensor_list in enumerate(tensor_lists):
                for j, tensor in enumerate(tensor_list):
                    gloo_util.copy_tensor(tensor, output_flattened[i][j])

        self._collective(
            tensors,
            output_flattened,
            collective_fn,
            postprocess_fn=postprocess_fn)

    def reducescatter(self,
                      tensors,
                      tensor_lists,
                      reducescatter_options=ReduceScatterOptions()):
        """Reduce the scatter a list of tensors across the group.

        Args:
            tensors (List): the output tensors (could be unspecified), each
                            located on a GPU of the current process.
            tensor_lists (List[List]): the list of tensors to be reduced then
                                       scattered.
            reducescatter_options: reduce-scatter options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, context):
            context.reduceScatter(
                context,
                gloo_util.get_tensor_ptr(input_tensor),
                gloo_util.get_tensor_ptr(output_tensor),
                gloo_util.get_tensor_n_elements(output_tensor),
                gloo_util.get_gloo_tensor_dtype(output_tensor),
                gloo_util.get_gloo_reduce_op(reducescatter_options.reduceOp))


        _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists)
        input_flattened = [
            _flatten_for_scatter_gather(tensor_list, copy=False)
            for tensor_list in tensor_lists
        ]

        def preprocess_fn():
            for i, tensor_list in enumerate(tensor_lists):
                for j, tensor in enumerate(tensor_list):
                    gloo_util.copy_tensor(input_flattened[i][j], tensor)

        self._collective(
            input_flattened,
            tensors,
            collective_fn,
            preprocess_fn=preprocess_fn)

    def send(self, tensors, send_options=SendOptions()):
        """Send a tensor to a destination gpu in the group.

        Args:
            tensors (List): the tensor to send.
            send_options: send options.

        Returns:
            None
        """

        def p2p_fn(tensor, context, peer):
            pygloo.send(
                context,
                gloo_util.get_tensor_ptr(tensor),
                gloo_util.get_tensor_n_elements(tensor),
                gloo_util.get_gloo_tensor_dtype(tensor),
                peer)

        self._point2point(tensors, p2p_fn, send_options.dst_rank)

    def recv(self, tensors, recv_options=RecvOptions()):
        """Receive a tensor from a source gpu in the group.

        Args:
            tensors (List): the received tensor.
            recv_options: Receive options.

        Returns:
            None
        """

        def p2p_fn(tensor, context, peer):
            pygloo.recv(
                context,
                gloo_util.get_tensor_ptr(tensor),
                gloo_util.get_tensor_n_elements(tensor),
                gloo_util.get_gloo_tensor_dtype(tensor),
                peer)

        self._point2point(tensors, p2p_fn, recv_options.src_rank)

    def _get_gloo_context(self):
        """
        Create or use a cached GLOO contextunicator for the collective task.

        """
        context = gloo_util.create_gloo_context(self.rank, self.world_size)
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

        if preprocess_fn:
            preprocess_fn()
        collective_fn(input_tensors[0], output_tensors[0], self._gloo_context)
        if postprocess_fn:
            postprocess_fn()

    def _point2point(self, tensors, p2p_fn, peer_rank: int):
        """A method to encapsulate all peer-to-peer calls (i.e., send/recv).

        Args:
            tensors: the tensor to send or receive.
            p2p_fn: the p2p function call.
            peer_rank (int): the rank of the peer process.
            peer_gpu_idx (int): the index of the gpu on the peer process.

        Returns:
            None
        """
        _check_cpu_tensors(tensors)

        p2p_fn(tensors[0], self._gloo_context, peer_rank)

def _check_cpu_tensors(tensors):
    """Check only have one tensor and located on CPU."""
    if not tensors or not isinstance(tensors, list):
        raise  RuntimeError("'tensors' must be a nonempty list.")
    if len(tensors) != 1:
        raise RuntimeError("Gloo only accept one tensor in the tensor list."
                           " Got {} != 1.".format(
                               len(tensors)))
    d = gloo_util.get_tensor_device(tensors[0])
    if d != 'cpu':
        raise RuntimeError("Gloo only accept cpu tensor."
                           " Got {}.".format(d))


def _flatten_for_scatter_gather(tensor_list, copy=False):
    """Flatten the tensor for gather/scatter operations.

    Args:
        tensor_list: the list of tensors to be scattered/gathered.
        copy: whether the copy the tensors in tensor_list into the buffer.

    Returns:
        The flattened tensor buffer.
    """
    if not tensor_list:
        raise RuntimeError("Received an empty list.")

    t = tensor_list[0]
    # note we need a numpy dtype here.
    dtype = gloo_util.get_cupy_tensor_dtype(t)
    buffer_shape = [len(tensor_list)] + gloo_util.get_tensor_shape(t)

    buffer = numpy.empty(buffer_shape, dtype=dtype)
    if copy:
        for i, tensor in enumerate(tensor_list):
            gloo_util.copy_tensor(buffer[i], tensor)
    return buffer


def _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists):
    """Check the compatibility between tensor input and tensor list input."""
    if not tensors or not isinstance(tensors, list):
        raise RuntimeError(
            "The first argument 'tensors' expects a list of tensors.")

    if len(tensors) != 1:
        raise RuntimeError(
            "Gloo only accept one tensor in the first argument 'tensors'."
                           " Got {} != 1.".format(len(tensors)))

    if not tensor_lists or not isinstance(tensor_lists, list):
        raise RuntimeError("The second argument 'tensor_lists' "
                           "expects a list of tensor list.")

    if len(tensor_lists) != 1:
        raise RuntimeError(
            "Gloo only accept one tensor list "
               "in the second argument 'tensor_lists'."
                   " Got {} != 1.".format(len(tensor_lists)))

    dtype = gloo_util.get_gloo_tensor_dtype(tensors[0])
    shape = gloo_util.get_tensor_shape(tensors[0])

    # check all tensors in `tensor_lists` match.
    for t in tensor_lists[0]:
        # check dtype
        dt = gloo_util.get_gloo_tensor_dtype(t)
        if dt != dtype:
            raise RuntimeError(
                "All tensor operands to scatter/gather must "
                "have the same dtype. Got '{}' and '{}'.".format(
                    dt, dtype))
        s = gloo_util.get_tensor_shape(t)
        if s != shape:
            raise RuntimeError(
                "All tensor operands to scatter/gather must "
                "have the same shape. Got '{}' and '{}'.".format(s, shape))