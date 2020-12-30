import logging
import datetime
import time

import ray
import cupy
from cupy.cuda.nccl import groupStart, groupEnd
from cupy.cuda import Device, Event, Stream, runtime, get_current_stream
from ray.util.collective.collective_group import nccl_util
from ray.util.collective.collective_group.base_collective_group \
    import BaseGroup
from ray.util.collective.const import get_nccl_store_name
from ray.util.collective.types import AllReduceOptions, \
    BarrierOptions, Backend, ReduceOptions, BroadcastOptions, \
    AllGatherOptions, ReduceScatterOptions

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")
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

        # TODO(Hao): fix this cache to accommodate `_dev_comm_map` below.
        self._collective_comm_cache = None
        self._p2p_comm_cache = {}
        self._p2p_stream_cache = {}

        if nccl_util.get_nccl_build_version() < 2000:
            raise RuntimeError("NCCL in Ray requires NCCL >= 2.0.")
        # TODO(Hao): check version here
        if nccl_util.get_nccl_runtime_version() < 2704:
            logger.warning("NCCL send/recv calls requires NCCL>=2.7.4")

        # Setup a tensor for barrier calls
        self._barrier_tensor = cupy.array([1])

        self._dev_comm_map = {}

        # check the stream correctness.
        self._dev_streams_map = {}

    def destroy_group(self):
        """Destroy the group and release NCCL communicators."""

        # if self._collective_comm_cache:
        #     self.barrier()
        #     # We also need a barrier call here.
        #     stream = self._get_cuda_stream()
        #     stream.synchronize()
        #     # destroy the communicator
        #     self._collective_comm_cache.destroy()
        #     self._collective_comm_cache = None
        #
        #     if self.rank == 0:
        #         self._destroy_store(self.group_name)

        if len(self._dev_comm_map.keys()) > 0:
            self.barrier()
            # destroy the streams and communicator
            #for _, streams in self._dev_streams_map.items():
            #    for stream in streams:
            #        runtime.streamDestroy(stream.ptr)
            for _, comms in self._dev_comm_map.items():
                for c in comms:
                    c.destroy()

        self._barrier_tensor = None
        self._dev_comm_map = None
        self._dev_streams_map = None

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

       # if self._p2p_stream_cache:
        #    for _, stream in self._p2p_stream_cache.items():
        #        runtime.streamDestroy(stream.ptr)
        #        self._p2p_stream_cache[key] = None
        #    for key in list(self._p2p_stream_cache.keys()):
        #        del self._p2p_stream_cache[key]
        super(NCCLGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.NCCL

    # TODO (Hao): change all collective/p2p calls to accept a list of tensors.
    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        """AllReduce the tensor across the collective group following options.

        Args:
            tensor(list): the list of tensor to be reduced. The length of the 
                          list is the num_gpus used in this process.
            allreduce_options:

        Returns:
        """
        devices = nccl_util.get_devices(tensor)
        key = nccl_util.get_key_from_devices(devices)
        comms = self._get_nccl_communicator(devices)
        # comm = self._get_nccl_collective_communicator()
        reduce_op = nccl_util.get_nccl_reduce_op(allreduce_options.reduceOp)

        # First wait for current tensor allocation stream
        streams = self._dev_streams_map[key]
        self._sync_streams()
        # for non-blocking calls of all-reduce
        groupStart()
        for i in range(len(tensor)):
            dtype = nccl_util.get_nccl_tensor_dtype(tensor[i])
            ptr = nccl_util.get_tensor_ptr(tensor[i])
            n_elems = nccl_util.get_tensor_n_elements(tensor[i])
            # in-place allreduce
            comms[i].allReduce(ptr, ptr, n_elems, dtype, reduce_op, streams[i].ptr)
        groupEnd()

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
            tensor(list): the list of tensor to be reduced.
            reduce_options: reduce options

        Returns:
            None
        """
        devices = nccl_util.get_devices(tensor)
        key = nccl_util.get_key_from_devices(devices)
        comms = self._get_nccl_communicator(devices)
        # comm = self._get_nccl_collective_communicator()
        reduce_op = nccl_util.get_nccl_reduce_op(reduce_options.reduceOp)

        # First wait for current tensor allocation stream
        streams = self._dev_streams_map[key]
        self._sync_streams()
       
        # compute the actual root rank
        root_rank = reduce_options.root_rank
        root_tensor = reduce_options.root_tensor
        _root_rank = root_rank * len(tensor) + root_tensor
        # for non-blocking calls of all-reduce
        groupStart()
        for i in range(len(tensor)):
            dtype = nccl_util.get_nccl_tensor_dtype(tensor[i])
            ptr = nccl_util.get_tensor_ptr(tensor[i])
            n_elems = nccl_util.get_tensor_n_elements(tensor[i])
            # in-place allreduce
            comms[i].reduce(ptr, ptr, n_elems, dtype, reduce_op, _root_rank,
                    streams[i].ptr)
        groupEnd()
        
    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        """Broadcast tensor to all other processes following options.

        Args:
            tensor(list): the list of tensor to be broadcasted.
            broadcast_options: broadcast options.

        Returns:
            None
        """
        devices = nccl_util.get_devices(tensor)
        key = nccl_util.get_key_from_devices(devices)
        comms = self._get_nccl_communicator(devices)
        # First wait for current tensor allocation stream
        streams = self._dev_streams_map[key]
        self._sync_streams()
       
        # compute the actual root rank
        root_rank = broadcast_options.root_rank
        root_tensor = broadcast_options.root_tensor
        _root_rank = root_rank * len(tensor) + root_tensor
        # for non-blocking calls of all-reduce
        groupStart()
        for i in range(len(tensor)):
            dtype = nccl_util.get_nccl_tensor_dtype(tensor[i])
            ptr = nccl_util.get_tensor_ptr(tensor[i])
            n_elems = nccl_util.get_tensor_n_elements(tensor[i])
            # in-place allreduce
            comms[i].broadcast(ptr, ptr, n_elems, dtype, _root_rank,
                    streams[i].ptr)
        groupEnd()

    def allgather(self,
                  tensor_list,
                  tensor,
                  allgather_options=AllGatherOptions()):
        """Allgather tensors across the group into a list of tensors.

        Args:
            tensor_list(list(list(tensor))): the tensor list to store
                                             the results.
            tensor(list): the list of tensor to be allgather-ed across
                          the group.
            allgather_options: allgather options.

        Returns:
            None
        """
        _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list)
        devices = nccl_util.get_devices(tensor)
        key = nccl_util.get_key_from_devices(devices)
        comms = self._get_nccl_communicator(devices)
        # First wait for current tensor allocation stream
        streams = self._dev_streams_map[key]
        self.sync_streams()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor[0])
        flattened = _flatten_for_scatter_gather(tensor_list, copy=False)
        recv_ptr = nccl_util.get_tensor_ptr(flattened)
        GroupStart()
        for i in range(len(tensor)):
            send_ptr = nccl_util.get_tensor_ptr(tensor[i])
            n_elems = nccl_util.get_tensor_n_elements(tensor[i])
            comms[i].allGather(send_ptr, recv_ptr, n_elems, dtype, streams[i].ptr)
            for index, t in enumerate(tensor_list[i]):
                nccl_util.copy_tensor(t, flattened[i][index])
        GroupEnd()

    def reducescatter(self,
                      tensor,
                      tensor_list,
                      reducescatter_options=ReduceScatterOptions()):
        """Reducescatter a list of tensors across the group.

        Args:
            tensor(list(tensor)): the output after reducescatter (could be unspecified).
            tensor_list(list(list(tensor))): the list of tensor to be reduce and scattered.
            reducescatter_options: reducescatter options.

        Returns:
            None
        """
        _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list)

        devices = nccl_util.get_devices(tensor)
        key = nccl_util.get_key_from_devices(devices)
        comms = self._get_nccl_communicator(devices)
        # First wait for current tensor allocation stream
        streams = self._dev_streams_map[key]
        self.sync_streams()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor[0])
        flattened = _flatten_for_scatter_gather(tensor_list, copy=True)
        send_ptr = nccl_util.get_tensor_ptr(flattened)
        reduce_op = nccl_util.get_nccl_reduce_op(
            reducescatter_options.reduceOp)
        GroupStart()
        for i in range(len(tensor)):
            recv_ptr = nccl_util.get_tensor_ptr(tensor[i])
            n_elems = nccl_util.get_tensor_n_elements(tensor[i])
            comms[i].reduceScatter(send_ptr, recv_ptr, n_elems, dtype, reduce_op,
                               streams[i].ptr)
        GroupEnd()

    def send(self, tensor, dst_rank, dst_index):
        """Send tensor to a destination process in the group.

        Args:
            tensor: the tensor to send.
            dst_rank: the rank of the destination process.
            dst_index: the index at the destination
        Returns:
            None
        """

        # check whether send/recv is available
        if nccl_util.get_nccl_runtime_version() < 2704:
            raise RuntimeError("send is not available requires NCCL >= 2.7.4. "
                               "Got '{}'.".format(
                                   nccl_util.get_nccl_runtime_version()))
        src_index = nccl_utils.get_devices([tensor])
        src_index = src_index[0]
        comm, peer_p2p_rank, key = self._get_nccl_p2p_communicator_and_info(
                           self.rank, src_index, dst_rank, dst_index)
        stream = self._p2p_stream_cache[key]

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        comm.send(ptr, n_elems, dtype, peer_p2p_rank, stream.ptr)

    def recv(self, tensor, src_rank, src_index):
        """Receive tensor from a source process in the group.

        Args:
            tensor: the received tensor.
            src_rank: the rank of the source process.

        Returns:
            None
        """
        if nccl_util.get_nccl_runtime_version() < 2704:
            raise RuntimeError("recv is not available requires NCCL >= 2.7.4. "
                               "Got '{}'.".format(
                                   nccl_util.get_nccl_runtime_version()))
        dst_index = nccl_utils.get_devices([tensor])
        dst_index = dst_index[0]
        comm, peer_p2p_rank, key = self._get_nccl_p2p_communicator_and_info(
                           src_rank, src_index, self.rank, dst_index)
        stream = self._p2p_stream_cache[key]

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        comm.recv(ptr, n_elems, dtype, peer_p2p_rank, stream.ptr)
        
    def _get_nccl_collective_communicator(self):
        """Create or retrieve a cached NCCL communicator.

        Returns:
            communicator
        """
        # TODO(Hao): implement a thin wrapper
        raise RuntimeError("Deprecated.")
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

    def _get_nccl_communicator(self, devices):
        """
        Create or use a cached NCCL communicator for the collective task.

        """
        # TODO(Hao): later change this to use device keys and query from cache.
        # TODO(Hao): implement a thin wrapper
        # try to find from cache
        key = nccl_util.get_key_from_devices(devices)
        if key in self._dev_comm_map.keys():
            return self._dev_comm_map[key]
        else:  # create a new one and cache
            _group_name = self.group_name + key
            if self.rank == 0:
                uid = nccl_util.get_nccl_unique_id()
                _store_name = get_nccl_store_name(_group_name)
                from ray.util.collective.util import NCCLUniqueIDStore
                store = NCCLUniqueIDStore.options(
                    name=_store_name, lifetime="detached").remote(_store_name)
                ray.wait([store.set_id.remote(uid)])

            rendezvous = Rendezvous(_group_name)
            rendezvous.meet()
            nccl_uid = rendezvous.get_nccl_id()
            _world_size = len(devices) * self.world_size
            comms = []

            nccl_streams = []
            # for non-blocking communicator creation
            groupStart()
            for i in range(len(devices)):
                _rank = self.rank * len(devices) + i
                from cupy.cuda import Device
                with Device(devices[i]):
                    comm = nccl_util.create_nccl_communicator(
                        _world_size, nccl_uid, _rank)
                    stream = Stream(non_blocking=True)
                comms.append(comm)
                nccl_streams.append(stream)
            groupEnd()

        # cache the result
        # FIXME: Consider whether to add a lock here or not, I feel like pytorch
        # needs to handle this because they are relatively lower level, we shouldnt
        # need to worry about this. if so,eed to figure out how to add Lock, threading? Asyncio?
        self._dev_comm_map[key] = comms
        self._dev_streams_map[key] = nccl_streams
        return comms

    def _sync_streams(self):
        """Let Nccl streams wait for current streams for every device."""
        # FIXME: This behavior is different from nccl document. It seems like
        # cupy allocate tensors on null streams.
        cupy.cuda.Stream.null.synchronize()

    def _get_nccl_p2p_communicator_and_info(self, src_rank, src_index, dst_rank, dst_index):
        """Create or retrieve an NCCL communicator for p2p tasks.

        Args:
            src_rank (int): source rank.
            src_index (int): GPU index at source rank
            dst_rank (int): destination rank.
            dst_index (int): GPU index at destination rank


        Returns:
            communicator
        """
        if src_rank < dst_rank:
            my_rank = 0 if self.rank == src_rank else 1
            min_str = str(src_rank) + "." + str(src_index)
            max_str = str(dst_rank) + "." + str(dst_index)
        elif scr_rank > dst_rank:
            my_rank = 0 if self.rank == dst_rank else 1
            min_str = str(dst_rank) + "." + str(dst_index)
            max_str = str(src_rank) + "." + str(src_index)
        else:
           raise RuntimeError(f"Sending data to the same machine.")

        p2p_group_key = self._generate_p2p_group_key(min_str, max_str)
        comm = self._p2p_comm_cache.get(p2p_group_key)
        if not comm:
            if my_rank == 0:
                group_uid = self._generate_nccl_uid(p2p_group_key)
            else:
                rendezvous = Rendezvous(p2p_group_key)
                rendezvous.meet()
                group_uid = rendezvous.get_nccl_id()
            my_device = src_index if self.rank == src_rank else dst_index
            with Device(my_device):
                comm = nccl_util.create_nccl_communicator(2, group_uid, my_rank)
                stream = Stream(non_blocking=True)
            self._p2p_comm_cache[p2p_group_key] = comm
            self._p2p_stream_cache[p2p_group_key] = stream
        
        peer_rank = 1 if my_rank == 0 else 0
        return comm, peer_rank, p2p_group_key

    def _generate_p2p_group_key(self, min_str, max_str):
        return self.group_name + "_" + min_rank + "_" + max_rank

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

    # Note(Hao): too many bipolate code -- make some abstractions here.
    def _collective(self, *args):
        """TODO(Hao)"""
        """Method to encapsulate all collective calls"""
        pass

    def _point2point(self, *args):
        """TODO(Hao)"""
        pass


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
    t = tensor_list[0][0]
    # note we need a cupy dtype here.
    dtype = nccl_util.get_cupy_tensor_dtype(t)
    buffer_shape = [len(tensor_list)] + [len(tensor_list[0])]+ \
                    nccl_util.get_tensor_shape(t)
    buffer = cupy.empty(buffer_shape, dtype=dtype)
    if copy:
        for i in range(len(buffer)):
            for index, tensor in enumerate(tensor_list[i]):
                nccl_util.copy_tensor(buffer[i][index], tensor)
    return buffer


def _check_inputs_compatibility_for_scatter_gather(tensor, tensor_list):
    """Check the compatibility between tensor input and tensor list inputs."""
    if not tensor_list:
        raise RuntimeError("Got empty list of tensors.")
    if not tensor:
        raise RuntimeError("Got empty tensor.")
    # note: tensor is a list of tensors, with shape num_gpus * len(tensor)
    # tensor_list is a list of list of tensors, with shape num_gpus * 
    # world_size * len(tensor)
    num_gpu = len(tensor_list)
    world_size = len(tensor_list[0])
    dtype = nccl_util.get_nccl_tensor_dtype(tensor[0])
    shape = nccl_util.get_tensor_shape(tensor[0])
    for i in range(num_gpu):
        # check the ith tensor component the same as the first
        dt = nccl_util.get_nccl_tensor_dtype(tensor[i])
        if dt != dtype:
            raise RuntimeError("All tensor operands to scatter/gather must "
                               "have the same dtype. Got '{}' and '{}'"
                               "".format(dt, dtype))
        if nccl_util.get_tensor_shape(tensor[i]) != shape:
            raise RuntimeError("All tensor operands to scatter/gather must "
                                "have the same shape.")
        # check every element of the ith tensor_list component the same
        # as the first component of tensor
        for t in tensor_list[i]:
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
