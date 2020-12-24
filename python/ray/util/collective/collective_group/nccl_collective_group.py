import logging
import datetime
import time

import ray
import cupy

from ray.util.collective.collective_group import nccl_util
from ray.util.collective.collective_group.base_collective_group \
    import BaseGroup
from ray.util.collective.types import AllReduceOptions, \
    BarrierOptions, Backend, ReduceOptions, BroadcastOptions, \
    AllGatherOptions, ReduceScatterOptions
from ray.util.collective.const import get_nccl_store_name

logger = logging.getLogger(__name__)

# TODO(Hao):
# (1) stream management, instead of using the default stream,
#     using a dedicate stream
# (2) communicator management and support num_gpus > 2 per actor.


class Rendezvous:
    """A rendezvous class for different actor/task processes to meet.

    To initialize an NCCL collective communication group, different
    actors/tasks spawned in Ray in a collective group needs to meet
    each other to synchronize the NCCLUniqueID. This class guarantees
    they meet via the NCCLUniqueIDStore, initialized on the rank=0
    process.

    Args:
        group_name (str): the unique user-specified group name.
    """

    def __init__(self, group_name):
        if not group_name:
            raise ValueError("Invalid group name.")
        self._group_name = group_name
        self._store_name = None
        self._store = None

    def meet(self, timeout_s=180):
        """Meet at the named actor store.

        Args:
            timeout_s: timeout in seconds.

        Return:
            None
        """
        if timeout_s <= 0:
            raise ValueError("The 'timeout' argument must be positive. "
                             "Got '{}'.".format(timeout_s))
        self._store_name = get_nccl_store_name(self._group_name)
        timeout_delta = datetime.timedelta(seconds=timeout_s)
        elapsed = datetime.timedelta(seconds=0)
        start_time = datetime.datetime.now()
        while elapsed < timeout_delta:
            try:
                logger.debug("Trying to meet at the store '{}'".format(
                    self._store_name))
                self._store = ray.get_actor(self._store_name)
            except ValueError:
                logger.debug("Failed to meet at the store '{}'."
                             "Trying again...".format(self._store_name))
                time.sleep(1)
                elapsed = datetime.datetime.now() - start_time
                continue
            logger.debug("Successful rendezvous!")
            break
        if not self._store:
            raise RuntimeError("Unable to meet other processes "
                               "at the rendezvous store.")

    @property
    def store(self):
        return self._store

    def get_nccl_id(self, timeout_s=180):
        """Get the NCCLUniqueID from the store through Ray.

        Args:
            timeout_s: timeout in seconds.
        Return:
            str: the NCCLUniqueID if successful.
        """
        if not self._store:
            raise ValueError("Rendezvous store is not setup.")
        uid = None
        timeout_delta = datetime.timedelta(seconds=timeout_s)
        elapsed = datetime.timedelta(seconds=0)
        start_time = datetime.datetime.now()
        while elapsed < timeout_delta:
            uid = ray.get(self._store.get_id.remote())
            if not uid:
                time.sleep(1)
                elapsed = datetime.datetime.now() - start_time
                continue
            break
        if not uid:
            raise RuntimeError(
                "Unable to get the NCCLUniqueID from the store.")
        return uid


class NCCLGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        """Init an NCCL collective group."""
        super(NCCLGroup, self).__init__(world_size, rank, group_name)
        self._nccl_uid = None

        # TODO(Hao): change this to a be a cache
        self._nccl_comm = None

        if nccl_util.get_nccl_build_version() < 2000:
            raise RuntimeError("NCCL in Ray requires NCCL >= 2.0.")
        # TODO(Hao): check version here
        if nccl_util.get_nccl_runtime_version() < 2704:
            logger.warning("NCCL send/recv calls requires NCCL>=2.7.4")

        self._rendezvous = Rendezvous(self.group_name)
        self._rendezvous.meet()

        # Setup the nccl uid using the store
        self._init_nccl_unique_id()

        # Setup a tensor for barrier calls
        self._barrier_tensor = cupy.array([1])

    def _init_nccl_unique_id(self):
        """Init the NCCLUniqueID required for creating NCCL communicators."""
        self._nccl_uid = self._rendezvous.get_nccl_id()

    @property
    def nccl_uid(self):
        return self._nccl_uid

    def destroy_group(self):
        """Destroy the group and release the NCCL communicators safely."""
        if self._nccl_comm is not None:
            self.barrier()
            # We also need a barrier call here.
            stream = self._get_cuda_stream()
            stream.synchronize()
            # destroy the communicator
            self._nccl_comm.destroy()
            self._nccl_comm = None
        super(NCCLGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.NCCL

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        """AllReduce the tensor across the collective group following options.

        Args:
            tensor: the tensor to be reduced, each tensor locates on a GPU
            allreduce_options:

        Returns:
        """
        # obtain the communicator
        comm = self._get_nccl_communicator()
        # obtain the stream: using default stream by now
        # TODO(Hao): implement a simple stream manager here
        stream = self._get_cuda_stream()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        reduce_op = nccl_util.get_nccl_reduce_op(allreduce_options.reduceOp)

        # in-place allreduce
        comm.allReduce(ptr, ptr, n_elems, dtype, reduce_op, stream.ptr)

    def barrier(self, barrier_options=BarrierOptions()):
        """Blocks until all processes reach this barrier.

        Args:
            barrier_options:

        Returns:
        """
        self.allreduce(self._barrier_tensor)

    def reduce(self, tensor, reduce_options=ReduceOptions()):
        """Reduce tensor to a destination process following options.

        Args:
            tensor: the tensor to be reduced.
            reduce_options: reduce options

        Returns:
            None
        """
        comm = self._get_nccl_communicator()
        stream = self._get_cuda_stream()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        reduce_op = nccl_util.get_nccl_reduce_op(reduce_options.reduceOp)

        # in-place reduce
        comm.reduce(ptr, ptr, n_elems, dtype, reduce_op,
                    reduce_options.root_rank, stream.ptr)

    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        """Broadcast tensor to all other processes following options.

        Args:
            tensor: the tensor to be broadcasted.
            broadcast_options: broadcast options.

        Returns:
            None
        """
        comm = self._get_nccl_communicator()
        stream = self._get_cuda_stream()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        # in-place broadcast
        comm.broadcast(ptr, ptr, n_elems, dtype, broadcast_options.root_rank,
                       stream.ptr)

    def allgather(self,
                  tensor_list,
                  tensor,
                  allgather_options=AllGatherOptions()):
        """Allgather tensors across the group into a list of  tensors.

        Args:
            tensor_list: the tensor list to store the results.
            tensor: the tensor to be allgather-ed across the group.
            allgather_options: allgather options.

        Returns:
            None
        """

        _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list)
        comm = self._get_nccl_communicator()
        stream = self._get_cuda_stream()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        send_ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        flattened = _flatten_for_scatter_gather(tensor_list, copy=False)
        recv_ptr = nccl_util.get_tensor_ptr(flattened)
        comm.allGather(send_ptr, recv_ptr, n_elems, dtype, stream.ptr)
        for i, t in enumerate(tensor_list):
            nccl_util.copy_tensor(t, flattened[i])

    def reducescatter(self,
                      tensor,
                      tensor_list,
                      reducescatter_options=ReduceScatterOptions()):
        """Reducescatter a list of tensors across the group.

        Args:
            tensor: the output after reducescatter (could be unspecified).
            tensor_list: the list of tensor to be reduce and scattered.
            reducescatter_options: reducescatter options.

        Returns:
            None
        """
        _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list)

        comm = self._get_nccl_communicator()
        stream = self._get_cuda_stream()
        dtype = nccl_util.get_nccl_tensor_dtype(tensor_list[0])
        n_elems = nccl_util.get_tensor_n_elements(tensor_list[0])
        reduce_op = nccl_util.get_nccl_reduce_op(
            reducescatter_options.reduceOp)

        # get the send_ptr
        flattened = _flatten_for_scatter_gather(tensor_list, copy=True)
        send_ptr = nccl_util.get_tensor_ptr(flattened)
        recv_ptr = nccl_util.get_tensor_ptr(tensor)
        comm.reduceScatter(send_ptr, recv_ptr, n_elems, dtype, reduce_op,
                           stream.ptr)

    def _get_nccl_communicator(self):
        """Create or use a cached NCCL communicator for the collective task.

        """
        # TODO(Hao): later change this to use device keys and query from cache.
        # TODO(Hao): implement a thin wrapper
        if not self._nccl_comm:
            self._nccl_comm = nccl_util.create_nccl_communicator(
                self.world_size, self.nccl_uid, self.rank)
        return self._nccl_comm

    @staticmethod
    def _get_cuda_stream():
        """Obtain an idle stream from a stream pool for the collective task."""
        # TODO: implement a simple stream manager.
        return cupy.cuda.Stream.null

    # def _collective_call(self, *args):
    #     """Private method to encapsulate all collective calls"""
    #     pass


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
    # note we need a cupy dtype here.
    dtype = nccl_util.get_cupy_tensor_dtype(t)
    buffer_shape = [len(tensor_list)] + nccl_util.get_tensor_shape(t)
    buffer = cupy.empty(buffer_shape, dtype=dtype)
    if copy:
        for i, tensor in enumerate(tensor_list):
            nccl_util.copy_tensor(buffer[i], tensor)
    return buffer


def _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list):
    """Check the compatibility between tensor input and tensor list inputs."""
    if not tensor_list:
        raise RuntimeError("Got empty list of tensors.")
    dtype = nccl_util.get_nccl_tensor_dtype(tensor)
    shape = nccl_util.get_tensor_shape(tensor)
    for t in tensor_list:
        # check dtype
        dt = nccl_util.get_nccl_tensor_dtype(t)
        if dt != dtype:
            raise RuntimeError("All tensor operands to scatter/gather must "
                               "have the same dtype. Got '{}' and '{}'"
                               "".format(dt, dtype))
        # Note: typically CCL libraries only requires they have the same
        # number of elements;
        # Here we make it more strict -- we require exact shape match.
        if nccl_util.get_tensor_shape(t) != shape:
            raise RuntimeError("All tensor operands to scatter/gather must "
                               "have the same shape.")
