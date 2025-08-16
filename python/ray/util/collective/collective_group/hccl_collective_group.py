import ctypes
import datetime
import os
import threading
import time
from typing import List, Optional
import logging

import torch
import torch.distributed as dist
import ray
import ray.experimental.internal_kv as internal_kv
from ray.util.collective.collective_group.base_collective_group import (
    BaseGroup,
    get_master_address_metadata_key,
)
from ray.util.collective.const import get_store_name
from ray.util.collective.types import (
    ReduceOp,
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

logger = logging.getLogger(__name__)

libhccl = None
try:
    libhccl = ctypes.CDLL("libhccl.so")
except OSError:
    raise ImportError("libhccl.so is not available.")


class HcclRootInfo(ctypes.Structure):
    _fields_ = [("internal", ctypes.c_byte * 4108)]


buffer_type = ctypes.c_void_p
npuStream_t = ctypes.c_void_p
hcclComm_t = ctypes.c_void_p


class hcclDataTypeEnum:
    HCCL_DATA_TYPE_INT8 = 0
    HCCL_DATA_TYPE_INT16 = 1
    HCCL_DATA_TYPE_INT32 = 2
    HCCL_DATA_TYPE_FP16 = 3
    HCCL_DATA_TYPE_FP32 = 4
    HCCL_DATA_TYPE_INT64 = 5
    HCCL_DATA_TYPE_UINT8 = 7
    HCCL_DATA_TYPE_FP64 = 10
    HCCL_DATA_TYPE_BFP16 = 11

    @classmethod
    def from_torch(cls, dtype: torch.dtype) -> int:
        _DTYPE_MAP = {
            torch.int8: cls.HCCL_DATA_TYPE_INT8,
            torch.int16: cls.HCCL_DATA_TYPE_INT16,
            torch.int32: cls.HCCL_DATA_TYPE_INT32,
            torch.float16: cls.HCCL_DATA_TYPE_FP16,
            torch.float32: cls.HCCL_DATA_TYPE_FP32,
            torch.int64: cls.HCCL_DATA_TYPE_INT64,
            torch.uint8: cls.HCCL_DATA_TYPE_UINT8,
            torch.float64: cls.HCCL_DATA_TYPE_FP64,
            torch.bfloat16: cls.HCCL_DATA_TYPE_BFP16,
        }
        hccl_dtype = _DTYPE_MAP.get(dtype)
        if hccl_dtype is None:
            raise ValueError(f"Unsupported dtype: {dtype}")
        return hccl_dtype


class hcclRedOpTypeEnum:
    HCCL_REDUCE_SUM = 0
    HCCL_REDUCE_PROD = 1
    HCCL_REDUCE_MAX = 2
    HCCL_REDUCE_MIN = 3

    @classmethod
    def from_ray(cls, op: ReduceOp) -> int:
        _OP_MAP = {
            ReduceOp.SUM: cls.HCCL_REDUCE_SUM,
            ReduceOp.PRODUCT: cls.HCCL_REDUCE_PROD,
            ReduceOp.MAX: cls.HCCL_REDUCE_MAX,
            ReduceOp.MIN: cls.HCCL_REDUCE_MIN,
        }
        hccl_op = _OP_MAP.get(op)
        if hccl_op is None:
            raise ValueError(f"Unsupported op: {op}")
        return hccl_op


@ray.remote
class HCCLRootInfoStore:
    """HcclRootInfo store as a named actors class.

    Args:
        name: the unique name for this named actor.

    Attributes:
        name: the unique name for this named actor.
        root_info: the HcclRootInfo held in this store.
    """

    def __init__(self, name):
        self.name = name
        self.root_info_bytes = None

    def set_root_info_bytes(self, root_info_bytes):
        self.root_info_bytes = root_info_bytes
        return self.root_info_bytes

    def get_root_info_bytes(self):
        if not self.root_info_bytes:
            logger.warning(
                f"The HcclRootInfo has not been set yet for store {self.name}."
            )
        return self.root_info_bytes


class HCCLGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        """Init an HCCL collective group."""
        # Ensure HCCL backend is registered with torch.distributed
        import torch_npu  # noqa: F401

        metadata_key = get_master_address_metadata_key(group_name)
        try:
            metadata = internal_kv._internal_kv_get(metadata_key)
        except ValueError:
            raise RuntimeError(
                f"HCCLGroup expected metadata in internal_kv with name `{metadata_key}`. "
                "HCCLGroup should not be instantiated directly. "
                "Use ray.experimental.collective.create_collective_group to create the group."
            )

        metadata = metadata.decode()
        master_addr, master_port = metadata.split(":")
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = master_port
        dist.init_process_group(backend="hccl", rank=rank, world_size=world_size)
        super(HCCLGroup, self).__init__(world_size, rank, group_name)
        self._dev_comm_map = {}
        self._dev_streams_map = {}

        # record used NPU IDs.
        self._used_npu_indices = set()
        self.libhccl = libhccl

    def destroy_group(self):
        if len(self._dev_comm_map.keys()) > 0:
            for comm_key, comms in self._dev_comm_map.items():
                if comms:
                    for comm in comms:
                        self.libhccl.HcclCommDestroy(comm)
                self._dev_comm_map[comm_key] = None

        if self.rank == 0:
            for comm_key in self._dev_comm_map:
                assert not self._dev_comm_map[comm_key]
                group_key = self._generate_group_key(comm_key)
                self._destroy_store(group_key)
        self._barrier_tensor = None
        self._dev_comm_map = None
        self._dev_streams_map = None
        super(HCCLGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return Backend.HCCL

    def broadcast(self, tensors, broadcast_options=BroadcastOptions()):
        """Broadcast tensors to all other npus following options.

        Args:
            tensors: tensors to be broadcast or received.
            broadcast_options: broadcast options.

        Returns:
            None
        """
        root_rank = (
            len(tensors) * broadcast_options.root_rank + broadcast_options.root_tensor
        )

        def collective_fn(
            input_tensor: torch.Tensor, output_tensor: torch.Tensor, comm, stream
        ):
            with torch.npu.device(input_tensor.device):
                exec_result = self.libhccl.HcclBroadcast(
                    buffer_type(input_tensor.data_ptr()),
                    input_tensor.numel(),
                    hcclDataTypeEnum.from_torch(input_tensor.dtype),
                    root_rank,
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclBroadcast`. Error code: {exec_result}."

        self._collective(tensors, tensors, collective_fn)

    def allgather(self, tensor_lists, tensors, allgather_options=AllGatherOptions()):
        """Allgather tensors across npus into a list of tensors.

        Args:
            tensor_lists (List[List[Tensor]]): allgathered tensors.
            tensors: the list of tensors to allgather across the group.
                     Each tensor must locate on a NPU of the process.
            allgather_options: allgather options.

        Returns:
            None
        """
        _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists)

        def collective_fn(
            input_tensor: torch.Tensor, output_tensor: torch.Tensor, comm, stream
        ):
            with torch.npu.device(input_tensor.device):
                exec_result = self.libhccl.HcclAllGather(
                    buffer_type(input_tensor.data_ptr()),
                    buffer_type(output_tensor.data_ptr()),
                    input_tensor.numel(),
                    hcclDataTypeEnum.from_torch(input_tensor.dtype),
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclAllGather`. Error code: {exec_result}."

        output_flattened = [
            _flatten_for_scatter_gather(tensor_list, copy=False)
            for tensor_list in tensor_lists
        ]

        self._collective(tensors, output_flattened, collective_fn)

        for i, tensor_list in enumerate(tensor_lists):
            for j, tensor in enumerate(tensor_list):
                tensor.copy_(output_flattened[i][j])

    def allreduce(self, tensors, allreduce_options=AllReduceOptions()):
        """AllReduce tensors across the collective group following options.

        Args:
            tensors: the list of tensors to be reduced. Each tensor must
                        reside on one NPU of the current process.
            allreduce_options: allreduce options.

        Returns:
            None
        """

        def collective_fn(
            input_tensor: torch.Tensor, output_tensor: torch.Tensor, comm, stream
        ):
            with torch.npu.device(input_tensor.device):
                exec_result = self.libhccl.HcclAllReduce(
                    buffer_type(input_tensor.data_ptr()),
                    buffer_type(output_tensor.data_ptr()),
                    input_tensor.numel(),
                    hcclDataTypeEnum.from_torch(input_tensor.dtype),
                    hcclRedOpTypeEnum.from_ray(allreduce_options.reduceOp),
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclAllReduce`. Error code: {exec_result}."

        self._collective(tensors, tensors, collective_fn)

    def barrier(self, barrier_options=BarrierOptions()):
        """Blocks until all processes reach this barrier.

        Args:
            barrier_options: barrier options.

        Returns:
            None
        """
        if self._used_npu_indices:
            devices = list(self._used_npu_indices)
        else:
            devices = list(range(torch.npu.device_count()))
        barrier_tensors = [None] * len(devices)
        for i, d in enumerate(devices):
            with torch.npu.device(d):
                barrier_tensors[i] = torch.ones(1).npu()
        self.allreduce(barrier_tensors)

    def reduce(self, tensors, reduce_options=ReduceOptions()):
        """Reduce tensors to a distination npu following options.

        Args:
            tensors: the list of tensors to be reduced, each tensor
                        must reside on one npu of the current process.
            reduce_options: reduce options.

        Returns:
            None
        """
        root_rank = len(tensors) * reduce_options.root_rank + reduce_options.root_tensor

        def collective_fn(
            input_tensor: torch.Tensor, output_tensor: torch.Tensor, comm, stream
        ):
            with torch.npu.device(input_tensor.device):
                exec_result = self.libhccl.HcclReduce(
                    buffer_type(input_tensor.data_ptr()),
                    buffer_type(output_tensor.data_ptr()),
                    input_tensor.numel(),
                    hcclDataTypeEnum.from_torch(input_tensor.dtype),
                    hcclRedOpTypeEnum.from_ray(reduce_options.reduceOp),
                    root_rank,
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclReduce`. Error code: {exec_result}."

        self._collective(tensors, tensors, collective_fn)

    def reducescatter(
        self, tensors, tensor_lists, reducescatter_options=ReduceScatterOptions()
    ):
        """Reduce then scatter a list of tensors across the group.

        Args:
            tensors: the output tensors (could be unspecified), each
                        located on a NPU of the current process.
            tensor_lists (List[List]): the list of tensors to be reduced the scattered.
            reducescatter_options: reduce-scatter options.

        Returns:
            None
        """

        _check_inputs_compatibility_for_scatter_gather(tensors, tensor_lists)

        def collective_fn(
            input_tensor: torch.Tensor, output_tensor: torch.Tensor, comm, stream
        ):
            with torch.npu.device(input_tensor.device):
                exec_result = self.libhccl.HcclReduceScatter(
                    buffer_type(input_tensor.data_ptr()),
                    buffer_type(output_tensor.data_ptr()),
                    output_tensor.numel(),
                    hcclDataTypeEnum.from_torch(input_tensor.dtype),
                    hcclRedOpTypeEnum.from_ray(reducescatter_options.reduceOp),
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
                logger.info(f"HcclReduceScatter execute result: {exec_result}")

        input_flattened = [
            _flatten_for_scatter_gather(tensor_list, copy=True)
            for tensor_list in tensor_lists
        ]

        self._collective(input_flattened, tensors, collective_fn)

    def send(self, tensors, send_options=SendOptions()):
        """Send a tensor to a destination npu in the group.

        Args:
            tensors: the tensor to send.
            send_options: send options.

        Returns:
            None
        """

        def p2p_fn(tensor: torch.Tensor, comm, stream, peer):
            with torch.npu.device(tensor.device):
                exec_result = self.libhccl.HcclSend(
                    buffer_type(tensor.data_ptr()),
                    tensor.numel(),
                    hcclDataTypeEnum.from_torch(tensor.dtype),
                    peer,
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclSend`. Error code: {exec_result}."

        self._point2point(
            tensors, p2p_fn, send_options.dst_rank, send_options.dst_device_index
        )

    def recv(self, tensors, recv_options=RecvOptions()):
        """Receive a tensor from a source npu in the group.

        Args:
            tensors: the received tensor.
            recv_options: receive options.

        Returns:
            None
        """

        def p2p_fn(tensor: torch.Tensor, comm, stream, peer):
            with torch.npu.device(tensor.device):
                exec_result = self.libhccl.HcclRecv(
                    buffer_type(tensor.data_ptr()),
                    tensor.numel(),
                    hcclDataTypeEnum.from_torch(tensor.dtype),
                    peer,
                    comm,
                    npuStream_t(stream.npu_stream),
                )
                stream.synchronize()
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclRecv`. Error code: {exec_result}."

        self._point2point(
            tensors, p2p_fn, recv_options.src_rank, recv_options.src_device_index
        )

    def _generate_group_key(self, comm_key):
        return comm_key + "@" + self.group_name

    def _generate_hccl_root_info(self, store_name, dev=0):
        root_info = HcclRootInfo()

        with torch.npu.device(f"npu:{dev}"):
            exec_result = self.libhccl.HcclGetRootInfo(ctypes.byref(root_info))
        assert (
            exec_result == 0
        ), f"Failed to execute `HcclGetRootInfo`. Error code: {exec_result}."

        store = HCCLRootInfoStore.options(name=store_name, lifetime="detached").remote(
            store_name
        )
        ray.get([store.set_root_info_bytes.remote(bytes(root_info))])

        return root_info

    def _get_store_ref(self, store_name, timeout_s=180):
        """Get the reference of the named actor store.

        Args:
            store_name: the unique store name
            timeout_s: timeout in seconds.

        Return:
            store_ref: reference to store actor
        """
        if timeout_s <= 0:
            raise ValueError(
                f"The `timeout` argument must be positive. Got: {timeout_s}"
            )
        store_ref = None
        timeout_delta = datetime.timedelta(seconds=timeout_s)
        elapsed = datetime.timedelta(seconds=0)
        start_time = datetime.datetime.now()
        while elapsed < timeout_delta:
            try:
                logger.debug(f"Trying to meet at the store {store_name}.")
                store_ref = ray.get_actor(store_name)
            except ValueError:
                logger.debug(
                    f"Failed to meet at the store {store_name}. Trying again..."
                )
                time.sleep(1)
                elapsed = datetime.datetime.now() - start_time
                continue
            logger.debug("Successful rendezvous!")
            break
        if not store_ref:
            raise RuntimeError(
                "Unable to meet other processes "
                "at the rendezvous store. If you are using "
                "P2P communication, please check if tensors "
                "are put in the correct NPU."
            )
        return store_ref

    @staticmethod
    def _destroy_store(group_key):
        store_name = get_store_name(group_key)
        store = ray.get_actor(store_name)
        ray.kill(store)

    def _get_hccl_root_info(self, store_ref, timeout_s=180):
        """Get the HcclRootInfo from the store through Ray.

        Args:
            timeout_s: timeout in seconds.

        Return:
            root_info: the HcclRootInfo if successful.
        """
        root_info_bytes = None
        timeout_delta = datetime.timedelta(seconds=timeout_s)
        elapsed = datetime.timedelta(seconds=0)
        start_time = datetime.datetime.now()
        while elapsed < timeout_delta:
            root_info_bytes = ray.get(store_ref.get_root_info_bytes.remote())
            if not root_info_bytes:
                time.sleep(1)
                elapsed = datetime.datetime.now() - start_time
                continue
            break
        if not root_info_bytes:
            raise RuntimeError("Unable to get the HcclRootInfo from the store.")
        return HcclRootInfo.from_buffer_copy(bytearray(root_info_bytes))

    def _get_hccl_collective_communicator(self, comm_key, device_list):
        if not comm_key:
            raise RuntimeError("Got empty communicator key.")
        for d in device_list:
            self._used_npu_indices.add(d)

        if comm_key in self._dev_comm_map:
            return self._dev_comm_map[comm_key]

        group_key = self._generate_group_key(comm_key)
        store_name = get_store_name(group_key)
        if self.rank == 0:
            root_info = self._generate_hccl_root_info(store_name=store_name)
        else:
            store_ref = self._get_store_ref(store_name=store_name)
            root_info = self._get_hccl_root_info(store_ref)

        device_world_size = len(device_list) * self.world_size
        comms = [None] * len(device_list)
        streams = [None] * len(device_list)

        def comm_init_task(i, device):
            device_rank = self.rank * len(device_list) + i
            with torch.npu.device(device):
                comms[i] = hcclComm_t()
                exec_result = self.libhccl.HcclCommInitRootInfo(
                    device_world_size,
                    ctypes.byref(root_info),
                    device_rank,
                    ctypes.byref(comms[i]),
                )
                assert (
                    exec_result == 0
                ), f"Failed to execute `HcclCommInitRootInfo`. Error code: {exec_result}."

                streams[i] = torch.npu.Stream()

        tasks: List[Optional[threading.Thread]] = [None] * len(device_list)
        for i, device in enumerate(device_list):
            tasks[i] = threading.Thread(target=comm_init_task, args=(i, device))
            tasks[i].start()

        for t in tasks:
            t.join()

        self._dev_comm_map[comm_key] = comms
        self._dev_streams_map[comm_key] = streams

        return comms

    def _get_hccl_p2p_communicator(self, comm_key, my_npu_idx, peer_rank, peer_npu_idx):
        """Create or retrieve an HCCL communicator for p2p tasks.

        Args:
            comm_key: communicator key.
            my_npu_idx: the npu index on the current process.
            peer_rank: the rank of the destination process.
            peer_npu_idx: the npu index on the peer process.

        Returns:
            communicator
        """
        if not comm_key:
            raise RuntimeError("Got empty communicator key.")

        if comm_key in self._dev_comm_map:
            return self._dev_comm_map[comm_key]

        if self.rank < peer_rank:
            my_p2p_rank = 0
        elif self.rank > peer_rank:
            my_p2p_rank = 1
        else:
            raise RuntimeError(
                "Send and recv happens on the same process! "
                "ray.util.collective does not support this case as of now."
            )

        group_key = self._generate_group_key(comm_key)
        store_name = get_store_name(group_key)
        if my_p2p_rank == 0:
            root_info = self._generate_hccl_root_info(
                store_name=store_name, dev=my_npu_idx
            )
        else:
            store_ref = self._get_store_ref(store_name=store_name)
            root_info = self._get_hccl_root_info(store_ref)

        with torch.npu.device(f"npu:{my_npu_idx}"):
            comm = hcclComm_t()
            exec_result = self.libhccl.HcclCommInitRootInfo(
                2, ctypes.byref(root_info), my_p2p_rank, ctypes.byref(comm)
            )
            assert (
                exec_result == 0
            ), f"Failed to execute `HcclCommInitRootInfo`. Error code: {exec_result}."

            stream = torch.npu.Stream()

        self._dev_comm_map[comm_key] = [comm]
        self._dev_streams_map[comm_key] = [stream]

        return [comm]

    def _collective(self, input_tensors, output_tensors, collective_func):
        devices = get_tensor_device_list(input_tensors)
        key = _get_comm_key_from_devices(devices)
        comms = self._get_hccl_collective_communicator(key, devices)
        streams = self._dev_streams_map[key]

        tasks: List[Optional[threading.Thread]] = [None] * len(input_tensors)
        for i, tensor in enumerate(input_tensors):
            tasks[i] = threading.Thread(
                target=collective_func,
                args=(tensor, output_tensors[i], comms[i], streams[i]),
            )
            tasks[i].start()
        for task in tasks:
            task.join()

    def _point2point(self, tensors, p2p_fn, peer_rank: int, peer_npu_idx: int):
        assert len(tensors) == 1

        my_npu_idx = get_tensor_device(tensors[0])
        comm_key = _get_comm_key_send_recv(
            self.rank, my_npu_idx, peer_rank, peer_npu_idx
        )
        comms = self._get_hccl_p2p_communicator(
            comm_key, my_npu_idx, peer_rank, peer_npu_idx
        )
        streams = self._dev_streams_map[comm_key]

        peer_p2p_rank = 0 if self.rank > peer_rank else 1
        for i, tensor in enumerate(tensors):
            p2p_fn(tensor, comms[i], streams[i], peer_p2p_rank)


def _get_comm_key_send_recv(my_rank, my_npu_idx, peer_rank, peer_npu_idx):
    """Return a key given source and destination ranks for p2p tasks.

    The p2p key is in the following form:
                [min_rank]_[npu_index]:[max_rank]_[npu_index]

    Args:
        my_rank: the rank of the source process.
        my_npu_idx: the source npu index on the process.
        peer_rank: the rank of the destination process.
        peer_npu_idx: the destination npu index on the process.

    Returns:
        comm_key: a string key to query the communication cache.
    """
    if my_rank < peer_rank:
        lower_key = str(my_rank) + "_" + str(my_npu_idx)
        higher_key = str(peer_rank) + "_" + str(peer_npu_idx)
    elif my_rank > peer_rank:
        lower_key = str(peer_rank) + "_" + str(peer_npu_idx)
        higher_key = str(my_rank) + "_" + str(my_npu_idx)
    else:
        raise RuntimeError(
            "Send and recv happens on the same process. ray.util.collective "
            "does not support this case as of now. Alternatively, consider "
            "doing NPU to NPU memcpy?"
        )
    comm_key = lower_key + ":" + higher_key
    return comm_key


def _get_comm_key_from_devices(devices: List[int]):
    """Return a key from a list of devices for collective calls.

    For example, if the tensors are on npus 0, 1, 2, 3,
    then the key would be "0,1,2,3".

    Args:
        devices: a list of NPU device indices

    Returns:
        str: a string represents the key to query the communicator cache.

    """
    return ",".join([str(d) for d in devices])


def get_tensor_device(tensor):
    """Return the NPU index of a tensor."""
    if isinstance(tensor, torch.Tensor):
        device = tensor.device.index
        if not isinstance(device, int):
            raise RuntimeError("The tensor is not on a valid NPU.")
    else:
        raise ValueError(f"Unsupported tensor type. Got: {type(tensor)}")
    return device


def get_tensor_device_list(tensors):
    """Returns the npu devices of the list of input tensors.

    Args:
        tensors: a list of tensors, each locates on a NPU.

    Returns:
        list: the list of NPU devices.
    """
    if not isinstance(tensors, list):
        raise RuntimeError(
            "Expect a list of tensors each locates on a NPU device. "
            f"Got: {type(tensors)}"
        )

    devices = [get_tensor_device(t) for t in tensors]
    return devices


def get_tensor_ptr(tensor):
    if isinstance(tensor, torch.Tensor):
        if not tensor.is_npu:
            raise RuntimeError(
                "Torch tensor must be on NPU when using HCCL collectives."
            )
        return tensor.data_ptr()
    raise ValueError(
        f"Unsupported tensor type. Got: {type(tensor)}. Only torch.Tensor "
        "supported for NPU so far."
    )


def _flatten_for_scatter_gather(tensor_list, copy=False):
    """Flatten the tensor for gather/scatter operations.

    Args:
        tensor_list: the list of tensors to be scattered/gathered.
        copy: whether to copy the tensors in tensor_list into the buffer.

    Returns:
        The flattened tensor buffer.
    """
    if not tensor_list:
        raise RuntimeError("Received an empty list.")
    t: torch.Tensor = tensor_list[0]
    buffer_shape = [len(tensor_list)] + list(t.shape)

    buffer = torch.empty(tuple(buffer_shape), dtype=t.dtype, device=t.device)
    if copy:
        for i, tensor in enumerate(tensor_list):
            buffer[i].copy_(tensor)
    return buffer


def _check_inputs_compatibility_for_scatter_gather(
    tensors: List[torch.Tensor], tensor_lists: List[List[torch.Tensor]]
) -> None:
    """Check the compatibility between tensor input and tensor list input."""
    if not tensors or not isinstance(tensors, list):
        raise RuntimeError("The first argument 'tensors' expects a list of tensors.")
    if not tensor_lists or not isinstance(tensor_lists, list):
        raise RuntimeError(
            "The second argument 'tensor_lists' expects a list of tensor list."
        )
    dtype = tensors[0].dtype
    shape = list(tensors[0].shape)
    for i, tensor_list in enumerate(tensor_lists):
        # check all tensor in `tensors` match.
        dt = tensors[i].dtype
        if dt != dtype:
            raise RuntimeError(
                "All tensor operands to scatter/gather must "
                f"have the same dtype. Got '{dt}' and '{dtype}'."
            )
        s = list(tensors[i].shape)
        if s != shape:
            raise RuntimeError(
                "All tensor operands to scatter/gather must "
                f"have the same shape. Got '{s}' and '{shape}'."
            )
        # check all tensors in `tensor_lists` match.
        for t in tensor_lists[i]:
            # check dtype
            dtl = t.dtype
            if dtl != dtype:
                raise RuntimeError(
                    "All tensor operands to scatter/gather must "
                    f"have the same dtype. Got '{dtl}' and '{dtype}'."
                )
            sl = list(t.shape)
            if sl != shape:
                raise RuntimeError(
                    "All tensor operands to scatter/gather must "
                    f"have the same shape. Got '{sl}' and '{shape}'."
                )
