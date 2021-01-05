import logging
import datetime
import time

import ray
import cupy

from ray.util.collective.collective_group import nccl_util
from ray.util.collective.collective_group.base_collective_group \
    import BaseGroup
from ray.util.collective.const import get_nccl_store_name
from ray.util.collective.types import AllReduceOptions, \
    BarrierOptions, Backend, ReduceOptions, BroadcastOptions, \
    AllGatherOptions, ReduceScatterOptions

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

        # TODO(Hao): change this to a be a cache
        self._collective_comm_cache = None
        self._p2p_comm_cache = {}

        if nccl_util.get_nccl_build_version() < 2000:
            raise RuntimeError("NCCL in Ray requires NCCL >= 2.0.")
        # TODO(Hao): check version here
        if nccl_util.get_nccl_runtime_version() < 2704:
            logger.warning("NCCL send/recv calls requires NCCL>=2.7.4")

        # Setup a tensor for barrier calls
        self._barrier_tensor = cupy.array([1])

    def destroy_group(self):
        """Destroy the group and release NCCL communicators."""
        if self._collective_comm_cache:
            self.barrier()
            # We also need a barrier call here.
            stream = self._get_cuda_stream()
            stream.synchronize()
            # destroy the communicator
            self._collective_comm_cache.destroy()
            self._collective_comm_cache = None

            if self.rank == 0:
                self._destroy_store(self.group_name)

        if self._p2p_comm_cache:
            for key, comm in self._p2p_comm_cache.items():
                comm.destroy()
                min_rank, max_rank = self._parse_p2p_group_key(key)
                if self.rank == min_rank:
                    self._destroy_store(key)
                self._p2p_comm_cache[key] = None
            for key in list(self._p2p_comm_cache.keys()):
                del self._p2p_comm_cache[key]
            self._p2p_comm_cache = None

        super(NCCLGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.NCCL

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        """AllReduce the tensor across the collective group following options.

        Args:
            tensor: the tensor to be reduced, each tensor locates on a GPU.
            allreduce_options: allreduce options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.allReduce(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor),
                nccl_util.get_nccl_reduce_op(allreduce_options.reduceOp),
                stream.ptr)

        self._collective(tensor, tensor, collective_fn)

    def barrier(self, barrier_options=BarrierOptions()):
        """Blocks until all processes reach this barrier.

        Args:
            barrier_options:

        Returns:
            None
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

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.reduce(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor),
                nccl_util.get_nccl_reduce_op(reduce_options.reduceOp),
                reduce_options.root_rank, stream.ptr)

        self._collective(tensor, tensor, collective_fn)

    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        """Broadcast tensor to all other processes following options.

        Args:
            tensor: the tensor to be broadcasted.
            broadcast_options: broadcast options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.broadcast(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor),
                broadcast_options.root_rank, stream.ptr)

        self._collective(tensor, tensor, collective_fn)

    def allgather(self,
                  tensor_list,
                  tensor,
                  allgather_options=AllGatherOptions()):
        """Allgather tensors across the group into a list of tensors.

        Args:
            tensor_list: the tensor list to store the results.
            tensor: the tensor to be allgather-ed across the group.
            allgather_options: allgather options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.allGather(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor), stream.ptr)

        _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list)
        flattened_output_tensor = _flatten_for_scatter_gather(
            tensor_list, copy=False)

        def postprocess_fn(stream):
            for i, tensor in enumerate(tensor_list):
                nccl_util.copy_tensor(tensor, flattened_output_tensor[i])

        self._collective(
            tensor,
            flattened_output_tensor,
            collective_fn,
            postprocess_fn=postprocess_fn)

    def reducescatter(self,
                      tensor,
                      tensor_list,
                      reducescatter_options=ReduceScatterOptions()):
        """Reducescatter a list of tensors across the group.

        Args:
            tensor: the output tensor (could be unspecified).
            tensor_list: the list of tensor to be reduced then scattered.
            reducescatter_options: reducescatter options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.reduceScatter(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(output_tensor),
                nccl_util.get_nccl_tensor_dtype(output_tensor),
                nccl_util.get_nccl_reduce_op(reducescatter_options.reduceOp),
                stream.ptr)

        _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list)
        flattened_input_tensor = _flatten_for_scatter_gather(
            tensor_list, copy=False)

        def preprocess_fn(stream):
            for i, tensor in enumerate(tensor_list):
                nccl_util.copy_tensor(flattened_input_tensor[i], tensor)

        self._collective(
            flattened_input_tensor,
            tensor,
            collective_fn,
            preprocess_fn=preprocess_fn)

    def send(self, tensor, dst_rank):
        """Send tensor to a destination process in the group.

        Args:
            tensor: the tensor to send.
            dst_rank: the rank of the destination process.

        Returns:
            None
        """

        def p2p_fn(tensor, comm, stream, peer):
            comm.send(
                nccl_util.get_tensor_ptr(tensor),
                nccl_util.get_tensor_n_elements(tensor),
                nccl_util.get_nccl_tensor_dtype(tensor), peer, stream.ptr)

        self._point2point(tensor, p2p_fn, dst_rank)

    def recv(self, tensor, src_rank):
        """Receive tensor from a source process in the group.

        Args:
            tensor: the received tensor.
            src_rank: the rank of the source process.

        Returns:
            None
        """

        def p2p_fn(tensor, comm, stream, peer):
            comm.recv(
                nccl_util.get_tensor_ptr(tensor),
                nccl_util.get_tensor_n_elements(tensor),
                nccl_util.get_nccl_tensor_dtype(tensor), peer, stream.ptr)

        self._point2point(tensor, p2p_fn, src_rank)

    def _get_nccl_collective_communicator(self):
        """Create or retrieve a cached NCCL communicator.

        Returns:
            communicator
        """
        if not self._collective_comm_cache:
            # create the communicator
            if self.rank == 0:
                group_uid = self._generate_nccl_uid(self.group_name)
            else:
                rendezvous = Rendezvous(self.group_name)
                rendezvous.meet()
                group_uid = rendezvous.get_nccl_id()
            self._collective_comm_cache = \
                nccl_util.create_nccl_communicator(self.world_size,
                                                   group_uid,
                                                   self.rank)
        return self._collective_comm_cache

    def _get_nccl_p2p_communicator(self, rank1, rank2):
        """Create or retrieve an NCCL communicator for p2p tasks.

        Args:
            rank1 (int): source rank.
            rank2 (int): destination rank.

        Returns:
            communicator
        """
        min_rank = min(rank1, rank2)
        max_rank = max(rank1, rank2)
        my_rank = 0 if self.rank == min_rank else 1
        p2p_group_key = self._generate_p2p_group_key(min_rank, max_rank)
        comm = self._p2p_comm_cache.get(p2p_group_key)
        if not comm:
            if self.rank == min_rank:
                group_uid = self._generate_nccl_uid(p2p_group_key)
            else:
                rendezvous = Rendezvous(p2p_group_key)
                rendezvous.meet()
                group_uid = rendezvous.get_nccl_id()
            comm = nccl_util.create_nccl_communicator(2, group_uid, my_rank)
            self._p2p_comm_cache[p2p_group_key] = comm
        return comm

    def _generate_p2p_group_key(self, min_rank, max_rank):
        return self.group_name + "_" + str(min_rank) + "_" + str(max_rank)

    @staticmethod
    def _parse_p2p_group_key(key):
        strs = key.split("_")
        return int(strs[-2]), int(strs[-1])

    @staticmethod
    def _destroy_store(group_name):
        store_name = get_nccl_store_name(group_name)
        store = ray.get_actor(store_name)
        # ray.get([store.__ray_terminate__.remote()])
        ray.kill(store)

    def _generate_nccl_uid(self, name):
        """Generate an NCCL UID by calling the NCCL API.

        Args:
            name: the name of the collective group.

        Returns:
            str: NCCL uid.
        """
        group_uid = nccl_util.get_nccl_unique_id()
        store_name = get_nccl_store_name(name)
        # Avoid a potential circular dependency in ray/actor.py
        from ray.util.collective.util import NCCLUniqueIDStore
        store = NCCLUniqueIDStore.options(
            name=store_name, lifetime="detached").remote(store_name)
        ray.wait([store.set_id.remote(group_uid)])
        return group_uid

    @staticmethod
    def _get_cuda_stream():
        """Obtain an idle stream from a stream pool for the collective task."""
        # TODO: implement a simple stream manager.
        return cupy.cuda.Stream.null

    def _collective(self,
                    input_tensor,
                    output_tensor,
                    collective_fn,
                    preprocess_fn=None,
                    postprocess_fn=None):
        """A method to encapsulate all collective calls.

        Args:
            input_tensor: the input tensor.
            output_tensor: the output tensor.
            collective_fn: the collective function call.
            preprocess_fn: preprocess function to call before collectives.
            postprocess_fn: postprocess function to call after collectives.

        Returns:
            None
        """
        comm = self._get_nccl_collective_communicator()
        stream = self._get_cuda_stream()

        # Make the collective call
        if preprocess_fn:
            preprocess_fn(stream)
        collective_fn(input_tensor, output_tensor, comm, stream)
        if postprocess_fn:
            postprocess_fn(stream)

    def _point2point(self, tensor, p2p_fn, peer_rank: int):
        """A method to encapsulate all p2p calls.

        Args:
            tensor: the tensor to be sent/received.
            p2p_fn: the p2p function call.
            peer_rank (int): the peer rank of the current process.

        Returns:
            None
        """
        # check send/recv availability.
        if nccl_util.get_nccl_runtime_version() < 2704:
            raise RuntimeError("P2p send/recv requires NCCL >= 2.7.4. "
                               "Got '{}'.".format(
                                   nccl_util.get_nccl_runtime_version()))

        # We have made sure that self.rank != peer_rank during API check.
        peer_p2p_rank = 0 if self.rank > peer_rank else 1
        comm = self._get_nccl_p2p_communicator(self.rank, peer_rank)
        stream = self._get_cuda_stream()
        # Make the p2p call:
        p2p_fn(tensor, comm, stream, peer_p2p_rank)


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
