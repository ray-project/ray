import logging
import datetime
import time

import ray
import cupy

from ray.util.collective.const import ENV
from ray.util.collective.collective_group import nccl_util
from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray.util.collective.const import get_store_name
from ray.util.collective.types import (
    AllReduceOptions,
    BarrierOptions,
    Backend,
    ReduceOptions,
    BroadcastOptions,
    AllGatherOptions,
    ReduceScatterOptions,
    SendOptions,
    RecvOptions,
)
from ray.util.collective.collective_group.cuda_stream import get_stream_pool

logger = logging.getLogger(__name__)


class Rendezvous:
    """A rendezvous class for different actor/task processes to meet.

    To initialize an NCCL collective communication group, different
    actors/tasks spawned in Ray in a collective group needs to meet
    each other to synchronize the NCCLUniqueID. This class guarantees
    they meet via the NCCLUniqueIDStore, initialized on the rank=0
    process.

    Args:
        store_key (str): the unique store key, usually as a concatanation
            of group_name and communicator key. See `get_nccl_communicator`
            for more details.
    """

    def __init__(self, store_key):
        if not store_key:
            raise ValueError(
                "Invalid store_key. The store_key is a concatenation of "
                "'group_name' and the 'communicator_key'. See the "
                "docstring of `get_nccl_communicator` for details."
            )
        self._store_key = store_key
        self._store_name = None
        self._store = None

    def meet(self, timeout_s=180):
        """Meet at the named actor store.

        Args:
            timeout_s (int): timeout in seconds.

        Return:
            None
        """
        if timeout_s <= 0:
            raise ValueError(
                "The 'timeout' argument must be positive. "
                "Got '{}'.".format(timeout_s)
            )
        self._store_name = get_store_name(self._store_key)
        timeout_delta = datetime.timedelta(seconds=timeout_s)
        elapsed = datetime.timedelta(seconds=0)
        start_time = datetime.datetime.now()
        while elapsed < timeout_delta:
            try:
                logger.debug(
                    "Trying to meet at the store '{}'".format(self._store_name)
                )
                self._store = ray.get_actor(self._store_name)
            except ValueError:
                logger.debug(
                    "Failed to meet at the store '{}'."
                    "Trying again...".format(self._store_name)
                )
                time.sleep(1)
                elapsed = datetime.datetime.now() - start_time
                continue
            logger.debug("Successful rendezvous!")
            break
        if not self._store:
            raise RuntimeError(
                "Unable to meet other processes "
                "at the rendezvous store. If you are using "
                "P2P communication, please check if tensors "
                "are put in the correct GPU. "
            )

    @property
    def store(self):
        return self._store

    def get_nccl_id(self, timeout_s=180):
        """Get the NCCLUniqueID from the store through Ray.

        Args:
            timeout_s: timeout in seconds.

        Return:
            uid (str): the NCCLUniqueID if successful.
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
            raise RuntimeError("Unable to get the NCCLUniqueID from the store.")
        return uid


class NCCLGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        """Init an NCCL collective group."""
        super(NCCLGroup, self).__init__(world_size, rank, group_name)

        # communicator and stream cache.
        # TODO (Hao): we need a lock here...
        self._dev_comm_map = {}
        self._dev_streams_map = {}

        # record the used GPU IDs.
        self._used_gpu_indices = set()

        # TODO(Fu): might need an event map
        self._dev_event_map = {}

        if nccl_util.get_nccl_build_version() < 2000:
            raise RuntimeError("NCCL in Ray requires NCCL >= 2.0.")
        if nccl_util.get_nccl_runtime_version() < 2704:
            logger.warning("NCCL send/recv calls requires NCCL>=2.7.4")

    def destroy_group(self):
        """Destroy the group and release NCCL communicators."""
        if len(self._dev_comm_map.keys()) > 0:

            # TODO(Hao): check this barrier call
            # self.barrier()

            # Destroy the communicators and streams.
            for comm_key, comms in self._dev_comm_map.items():
                for c in comms:
                    c.destroy()
                self._dev_comm_map[comm_key] = None

        if self.rank == 0:
            for comm_key in self._dev_comm_map:
                assert not self._dev_comm_map[comm_key]
                group_key = self._generate_group_key(comm_key)
                self._destroy_store(group_key)
        self._barrier_tensor = None
        self._dev_comm_map = None
        self._dev_streams_map = None
        super(NCCLGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.NCCL

    def allreduce(self, tensors, allreduce_options=AllReduceOptions()):
        """AllReduce tensors across the collective group following options.

        Args:
            tensors (List): the list of tensors to be reduced. Each tensor must
                            reside on one GPU of the current process.
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
                stream.ptr,
            )

        self._collective(tensors, tensors, collective_fn)

    def barrier(self, barrier_options=BarrierOptions()):
        """Blocks until all processes reach this barrier.

        Args:
            barrier_options: barrier options.

        Returns:
            None
        """
        # Get the device list.
        if self._used_gpu_indices:
            devices = list(self._used_gpu_indices)
        else:
            devices = list(range(nccl_util.get_num_gpus()))
        barrier_tensors = [None] * len(devices)
        for i, d in enumerate(devices):
            with nccl_util.Device(d):
                barrier_tensors[i] = cupy.array([1])
        self.allreduce(barrier_tensors)

    def reduce(self, tensors, reduce_options=ReduceOptions()):
        """Reduce tensors to a destination gpu following options.

        Args:
            tensors (List): the list of tensors to be reduced, each tensor
                            must reside on one gpu of the current process.
            reduce_options: reduce options.

        Returns:
            None
        """
        root_rank = len(tensors) * reduce_options.root_rank + reduce_options.root_tensor

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.reduce(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor),
                nccl_util.get_nccl_reduce_op(reduce_options.reduceOp),
                root_rank,
                stream.ptr,
            )

        self._collective(tensors, tensors, collective_fn)

    def broadcast(self, tensors, broadcast_options=BroadcastOptions()):
        """Broadcast tensors to all other gpus following options.

        Args:
            tensors (List): tensors to be broadcast or received.
            broadcast_options: broadcast options.

        Returns:
            None
        """
        root_rank = (
            len(tensors) * broadcast_options.root_rank + broadcast_options.root_tensor
        )

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.broadcast(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor),
                root_rank,
                stream.ptr,
            )

        self._collective(tensors, tensors, collective_fn)

    def allgather(self, tensor_lists, tensors, allgather_options=AllGatherOptions()):
        """Allgather tensors across gpus into a list of tensors.

        Args:
            tensor_lists (List[List[Tensor]]): allgathered tensors.
            tensors: the list of tensors to allgather across the group.
                     Each tensor must lolcate on a GPU of the process.
            allgather_options: allgather options.

        Returns:
            None
        """

        def collective_fn(input_tensor, output_tensor, comm, stream):
            comm.allGather(
                nccl_util.get_tensor_ptr(input_tensor),
                nccl_util.get_tensor_ptr(output_tensor),
                nccl_util.get_tensor_n_elements(input_tensor),
                nccl_util.get_nccl_tensor_dtype(input_tensor),
                stream.ptr,
            )

        _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists)
        output_flattened = [
            _flatten_for_scatter_gather(tensor_list, copy=False)
            for tensor_list in tensor_lists
        ]

        def postprocess_fn(stream):
            # TODO(Hao): designate a copy stream.
            for i, tensor_list in enumerate(tensor_lists):
                for j, tensor in enumerate(tensor_list):
                    nccl_util.copy_tensor(tensor, output_flattened[i][j])

        self._collective(
            tensors, output_flattened, collective_fn, postprocess_fn=postprocess_fn
        )

    def reducescatter(
        self, tensors, tensor_lists, reducescatter_options=ReduceScatterOptions()
    ):
        """Reduce then scatter a list of tensors across the group.

        Args:
            tensors (List): the output tensors (could be unspecified), each
                            located on a GPU of the current process.
            tensor_lists (List[List]): the list of tensors to be reduced then
                                       scattered.
            reducescatter_options: reduce-scatter options.

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
                stream.ptr,
            )

        _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists)
        input_flattened = [
            _flatten_for_scatter_gather(tensor_list, copy=False)
            for tensor_list in tensor_lists
        ]

        def preprocess_fn(stream):
            for i, tensor_list in enumerate(tensor_lists):
                for j, tensor in enumerate(tensor_list):
                    nccl_util.copy_tensor(input_flattened[i][j], tensor)

        self._collective(
            input_flattened, tensors, collective_fn, preprocess_fn=preprocess_fn
        )

    def send(self, tensors, send_options=SendOptions()):
        """Send a tensor to a destination gpu in the group.

        Args:
            tensors (List): the tensor to send.
            send_options: send options.

        Returns:
            None
        """

        def p2p_fn(tensor, comm, stream, peer):
            comm.send(
                nccl_util.get_tensor_ptr(tensor),
                send_options.n_elements
                if send_options.n_elements > 0
                else nccl_util.get_tensor_n_elements(tensor),
                nccl_util.get_nccl_tensor_dtype(tensor),
                peer,
                stream.ptr,
            )

        self._point2point(
            tensors, p2p_fn, send_options.dst_rank, send_options.dst_gpu_index
        )

    def recv(self, tensors, recv_options=RecvOptions()):
        """Receive a tensor from a source gpu in the group.

        Args:
            tensors (List): the received tensor.
            recv_options: Receive options.

        Returns:
            None
        """

        def p2p_fn(tensor, comm, stream, peer):
            comm.recv(
                nccl_util.get_tensor_ptr(tensor),
                recv_options.n_elements
                if recv_options.n_elements > 0
                else nccl_util.get_tensor_n_elements(tensor),
                nccl_util.get_nccl_tensor_dtype(tensor),
                peer,
                stream.ptr,
            )

        self._point2point(
            tensors, p2p_fn, recv_options.src_rank, recv_options.src_gpu_index
        )

    def _get_nccl_collective_communicator(self, comm_key, device_list):
        """Create or retrieve an NCCL communicator from cache.

        If the communicator is found in cache, return the communicator. If not,
        a communicator and a stream will be created and put in cache.
        TODO(Hao): this function is not thread-safe now.

        Args:
            comm_key (str): the key to query the communicator cache.
            device_list (List): a list of GPU devices of the current process
                                that participates into the collective.

        Returns:
            communicator: the NCCL communicator corresponded to the devices.
        """
        if not comm_key:
            raise RuntimeError("Got empty communicator key.")
        for d in device_list:
            self._used_gpu_indices.add(d)

        # TODO(Hao): lock the _dev_comm_map here.
        if comm_key in self._dev_comm_map:
            return self._dev_comm_map[comm_key]

        group_key = self._generate_group_key(comm_key)
        if self.rank == 0:
            nccl_uid = self._generate_nccl_uid(group_key)
        else:
            rendezvous = Rendezvous(group_key)
            rendezvous.meet()
            nccl_uid = rendezvous.get_nccl_id()

        # Now create the communicators
        actual_world_size = len(device_list) * self.world_size
        comms = [None] * len(device_list)
        streams = [None] * len(device_list)
        events = [None] * len(device_list)
        nccl_util.groupStart()
        for i, device in enumerate(device_list):
            actual_rank = self.rank * len(device_list) + i
            with nccl_util.Device(device):
                comms[i] = nccl_util.create_nccl_communicator(
                    actual_world_size, nccl_uid, actual_rank
                )
                # request a stream from the pool
                # note the device_idx is absolute index.
                streams[i] = get_stream_pool(device).get_stream()
                # TODO(Fu): double check the parameters
                events[i] = cupy.cuda.Event()
        nccl_util.groupEnd()
        # TODO(Fu): lock
        self._dev_comm_map[comm_key] = comms
        self._dev_streams_map[comm_key] = streams
        self._dev_event_map[comm_key] = events
        return comms

    @staticmethod
    def _sync_streams(device_list, events, streams):
        """Let NCCL streams wait for current streams for every device."""
        # TODO(Fu): recordStream besides calling this function?
        if ENV.NCCL_USE_MULTISTREAM.val:
            for i, device in enumerate(device_list):
                with nccl_util.Device(device):
                    events[i].record(cupy.cuda.get_current_stream())
                    streams[i].wait_event(events[i])

    def _get_nccl_p2p_communicator(self, comm_key, my_gpu_idx, peer_rank, peer_gpu_idx):
        """Create or retrieve an NCCL communicator for p2p tasks.

        Note(Hao): this function is not thread-safe now.

        Args:
            comm_key (str): communicator key.
            my_gpu_idx (int): the gpu index on the current process.
            peer_rank (int): the rank of the destination process.
            peer_gpu_idx (int): the gpu index on the peer process.
        Returns:
            communicator
        """
        if not comm_key:
            raise RuntimeError("Got empty communicator key.")

        # TODO(Hao): lock the _dev_comm_map here.
        if comm_key in self._dev_comm_map:
            return self._dev_comm_map[comm_key]

        # Note (Hao): This is a bit complex so I decide to take a note here.
        # Here we need to consider three cases:
        # Case 1: src_rank != dst_rank, hence the send and recv happen on
        # different process (actors/tasks); each process makes independent
        # collective calls and manages corresponding communicators.
        # Case 2: src_rank == dst_rank, src_gpu_idx == dst_gpu_idx; for
        # this case, we simply throw a RuntimeError;
        # Case 3: src_rank == dst_rank, src_gpu_idx != dst_gpu_idx, which
        # means the send and recv will be called on the same process. We
        # DO NOT support this case for now. We need to properly scope:
        # (1) communicators creation, and
        # (2) send/recv calls
        # using groupStart(（ and groupEnd() calls to avoid deadlocks.
        if self.rank < peer_rank:
            my_p2p_rank = 0
        elif self.rank > peer_rank:
            my_p2p_rank = 1
        else:
            raise RuntimeError(
                "Send and recv happens on the same process! "
                "ray.util.collective does not support this case as of now. "
                "Alternatively, consider doing GPU to GPU memcpy?"
            )

        group_key = self._generate_group_key(comm_key)
        if my_p2p_rank == 0:
            nccl_uid = self._generate_nccl_uid(group_key)
        else:
            rendezvous = Rendezvous(group_key)
            rendezvous.meet()
            nccl_uid = rendezvous.get_nccl_id()

        # create the p2p communicators
        with nccl_util.Device(my_gpu_idx):
            comm = nccl_util.create_nccl_communicator(2, nccl_uid, my_p2p_rank)
            stream = get_stream_pool(my_gpu_idx).get_stream()
            event = cupy.cuda.Event()

        # TODO(Fu): lock and might need to add event
        self._dev_comm_map[comm_key] = [comm]
        self._dev_streams_map[comm_key] = [stream]
        self._dev_event_map[comm_key] = [event]
        return [comm]

    def _generate_group_key(self, comm_key):
        """Generate a unique key used to initialize the KV store.

        The group key is a concatenation of the communicator key and
        the group name, following: [comm_key]@[group_name].
        """
        return comm_key + "@" + self.group_name

    @staticmethod
    def _destroy_store(group_key):
        """Destroy the KV store (Ray named actor).

        Args:
            group_key (str): the unique key to retrieve the KV store.

        Returns:
            None
        """
        store_name = get_store_name(group_key)
        store = ray.get_actor(store_name)
        # ray.get([store.__ray_terminate__.remote()])
        ray.kill(store)

    def _generate_nccl_uid(self, key):
        """Generate an NCCL unique ID for initializing communicators.

        The method will also create a KV store using Ray named actor and store
        the NCCLUniqueID in the store. The store needs to be garbage collected
        when destroying the collective group.

        Args:
            key (str): the key of the .

        Returns:
            NCCLUniqueID (str): NCCL unique ID.
        """
        group_uid = nccl_util.get_nccl_unique_id()
        store_name = get_store_name(key)
        # Avoid a potential circular dependency in ray/actor.py
        from ray.util.collective.util import NCCLUniqueIDStore

        store = NCCLUniqueIDStore.options(name=store_name, lifetime="detached").remote(
            store_name
        )
        ray.get([store.set_id.remote(group_uid)])
        return group_uid

    def _collective(
        self,
        input_tensors,
        output_tensors,
        collective_fn,
        preprocess_fn=None,
        postprocess_fn=None,
    ):
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
        _check_gpu_tensors(input_tensors)
        _check_gpu_tensors(output_tensors)

        devices = nccl_util.get_tensor_device_list(input_tensors)
        key = _get_comm_key_from_devices(devices)
        comms = self._get_nccl_collective_communicator(key, devices)
        streams = self._dev_streams_map[key]
        events = self._dev_event_map[key]

        # TODO(Hao): sync streams and events
        self._sync_streams(devices, events, streams)

        # Make the collective call
        if preprocess_fn:
            preprocess_fn(streams)

        nccl_util.groupStart()
        # TODO(Fu): how to recordStreams as there are no library functions
        # We also need to make sure input tensors are not freed before their
        # usages on ncclStreams finish. This can be achieved by calling
        # c10::cuda::CUDACachingAllocator::recordStream, which remembers the
        # usage stream (ncclStream), creates an event on the usage stream
        # when GC attempts to free the input tensor, and delays GC until that
        # event is done.
        for i, tensor in enumerate(input_tensors):
            collective_fn(tensor, output_tensors[i], comms[i], streams[i])
        nccl_util.groupEnd()
        if postprocess_fn:
            postprocess_fn(streams)

    def _point2point(self, tensors, p2p_fn, peer_rank: int, peer_gpu_idx: int):
        """A method to encapsulate all peer-to-peer calls (i.e., send/recv).

        Args:
            tensors: the tensor to send or receive.
            p2p_fn: the p2p function call.
            peer_rank (int): the rank of the peer process.
            peer_gpu_idx (int): the index of the gpu on the peer process.

        Returns:
            None
        """
        # check send/recv availability.
        if nccl_util.get_nccl_runtime_version() < 2704:
            raise RuntimeError(
                "P2p send/recv requires NCCL >= 2.7.4. "
                "Got '{}'.".format(nccl_util.get_nccl_runtime_version())
            )
        _check_gpu_tensors(tensors)

        # we currently only support single device to single device send/recv.
        assert len(tensors) == 1
        my_gpu_idx = nccl_util.get_tensor_device(tensors[0])
        comm_key = _get_comm_key_send_recv(
            self.rank, my_gpu_idx, peer_rank, peer_gpu_idx
        )
        comms = self._get_nccl_p2p_communicator(
            comm_key, my_gpu_idx, peer_rank, peer_gpu_idx
        )
        streams = self._dev_streams_map[comm_key]
        events = self._dev_event_map[comm_key]

        # TODO(Hao): sync streams and events
        self._sync_streams([my_gpu_idx], events, streams)

        # We have made sure that self.rank != peer_rank during API check.
        peer_p2p_rank = 0 if self.rank > peer_rank else 1
        for i, tensor in enumerate(tensors):
            p2p_fn(tensors[i], comms[i], streams[i], peer_p2p_rank)


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
    device = nccl_util.get_tensor_device(t)
    with nccl_util.Device(device):
        buffer = cupy.empty(buffer_shape, dtype=dtype)
    if copy:
        for i, tensor in enumerate(tensor_list):
            nccl_util.copy_tensor(buffer[i], tensor)
    return buffer


def _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists):
    """Check the compatibility between tensor input and tensor list input."""
    if not tensors or not isinstance(tensors, list):
        raise RuntimeError("The first argument 'tensors' expects a list of tensors.")
    if not tensor_lists or not isinstance(tensor_lists, list):
        raise RuntimeError(
            "The second argument 'tensor_lists' expects a list of tensor list."
        )
    dtype = nccl_util.get_nccl_tensor_dtype(tensors[0])
    shape = nccl_util.get_tensor_shape(tensors[0])
    for i, tensor_list in enumerate(tensor_lists):
        # check all tensor in `tensors` match.
        dt = nccl_util.get_nccl_tensor_dtype(tensors[i])
        if dt != dtype:
            raise RuntimeError(
                "All tensor operands to scatter/gather must "
                "have the same dtype. Got '{}' and '{}'.".format(dt, dtype)
            )
        # Note: typically CCL libraries only requires they have the same
        # number of elements; Here we make it more strict -- we require
        # exact shape match.
        s = nccl_util.get_tensor_shape(tensors[i])
        if s != shape:
            raise RuntimeError(
                "All tensor operands to scatter/gather must "
                "have the same shape. Got '{}' and '{}'.".format(s, shape)
            )
        # check all tensors in `tensor_lists` match.
        for t in tensor_lists[i]:
            # check dtype
            dt = nccl_util.get_nccl_tensor_dtype(t)
            if dt != dtype:
                raise RuntimeError(
                    "All tensor operands to scatter/gather must "
                    "have the same dtype. Got '{}' and '{}'.".format(dt, dtype)
                )
            s = nccl_util.get_tensor_shape(t)
            if s != shape:
                raise RuntimeError(
                    "All tensor operands to scatter/gather must "
                    "have the same shape. Got '{}' and '{}'.".format(s, shape)
                )


def _check_gpu_tensors(tensors):
    """Check all tensors are distributed on different GPUs."""
    if not tensors or not isinstance(tensors, list):
        raise RuntimeError("'tensors' must be a nonempty list.")
    if len(tensors) > nccl_util.get_num_gpus():
        raise RuntimeError(
            "Tensor list cannot be larger than the number"
            "of available GPUs. Got {} > {}.".format(
                len(tensors), nccl_util.get_num_gpus()
            )
        )
    t0 = tensors[0]
    dt = nccl_util.get_nccl_tensor_dtype(t0)
    s = nccl_util.get_tensor_shape(t0)
    d = nccl_util.get_tensor_device(t0)
    for i, t in enumerate(tensors):
        if i == 0:
            continue
        # We need to check the following:
        # (1) tensor is cuda (already checked during API)
        # (2) tensor dtype
        # (3) tensor shape match
        # (4) each tensor is on a different GPU
        dtype = nccl_util.get_nccl_tensor_dtype(t)
        if dt != dtype:
            raise RuntimeError(
                "Tensors must have identical dtype. Got: '{}'.".format(dtype)
            )
        shape = nccl_util.get_tensor_shape(t)
        if s != shape:
            raise RuntimeError(
                "Tensor must have identical shape. Got: '{}'.".format(shape)
            )
        device = nccl_util.get_tensor_device(t)
        if device == d:
            raise RuntimeError("Tensor must be on distinct GPUs.")


def _get_comm_key_from_devices(devices):
    """Return a key from a list of devices for collective calls.

    For example, if the tensors are on gpus 0, 1, 2, 3,
    then the key would be "0,1,2,3".

    Args:
        devices(list): a list of GPU device indices

    Returns:
        str: a string represents the key to query the communicator cache.

    """
    return ",".join([str(d) for d in devices])


def _get_comm_key_send_recv(my_rank, my_gpu_idx, peer_rank, peer_gpu_idx):
    """Return a key given source and destination ranks for p2p tasks.

    The p2p key is in the following form:
                [min_rank]_[gpu_index]:[max_rank]_[gpu_index].

    Args:
        my_rank (int): the rank of the source process.
        my_gpu_idx (int): the source gpu index on the process.
        peer_rank (int): the rank of the destination process.
        peer_gpu_idx (int): the destination gpu index on the process.

    Returns:
        comm_key (str): a string key to query the communication cache.
    """
    if my_rank < peer_rank:
        lower_key = str(my_rank) + "_" + str(my_gpu_idx)
        higher_key = str(peer_rank) + "_" + str(peer_gpu_idx)
    elif my_rank > peer_rank:
        lower_key = str(peer_rank) + "_" + str(peer_gpu_idx)
        higher_key = str(my_rank) + "_" + str(my_gpu_idx)
    else:
        raise RuntimeError(
            "Send and recv happens on the same process. ray.util.collective "
            "does not support this case as of now. Alternatively, consider "
            "doing GPU to GPU memcpy?"
        )
    comm_key = lower_key + ":" + higher_key
    return comm_key
