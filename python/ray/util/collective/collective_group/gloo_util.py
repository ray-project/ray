"""Code to wrap some GLOO API calls."""
import asyncio
import time

import numpy

import ray
import ray.experimental.internal_kv as internal_kv
from ray._private.gcs_utils import GcsClient
from ray.util.collective.types import ReduceOp, torch_available
from ray.util.queue import _QueueActor

try:
    import pygloo
except ImportError:
    raise ImportError(
        "Can not import pygloo. Please run 'pip install pygloo' to install pygloo."
    )


GLOO_REDUCE_OP_MAP = {
    ReduceOp.SUM: pygloo.ReduceOp.SUM,
    ReduceOp.PRODUCT: pygloo.ReduceOp.PRODUCT,
    ReduceOp.MIN: pygloo.ReduceOp.MIN,
    ReduceOp.MAX: pygloo.ReduceOp.MAX,
}

NUMPY_GLOO_DTYPE_MAP = {
    # INT types
    numpy.int: pygloo.glooDataType_t.glooInt64,
    numpy.uint8: pygloo.glooDataType_t.glooUint8,
    numpy.uint32: pygloo.glooDataType_t.glooUint32,
    numpy.uint64: pygloo.glooDataType_t.glooUint64,
    numpy.int8: pygloo.glooDataType_t.glooInt8,
    numpy.int32: pygloo.glooDataType_t.glooInt32,
    numpy.int64: pygloo.glooDataType_t.glooInt64,
    # FLOAT types
    numpy.half: pygloo.glooDataType_t.glooFloat16,
    numpy.float: pygloo.glooDataType_t.glooFloat64,
    numpy.float16: pygloo.glooDataType_t.glooFloat16,
    numpy.float32: pygloo.glooDataType_t.glooFloat32,
    numpy.float64: pygloo.glooDataType_t.glooFloat64,
    numpy.double: pygloo.glooDataType_t.glooFloat64,
}

if torch_available():
    import torch

    TORCH_GLOO_DTYPE_MAP = {
        torch.int: pygloo.glooDataType_t.glooInt32,
        torch.uint8: pygloo.glooDataType_t.glooUint8,
        torch.int8: pygloo.glooDataType_t.glooInt8,
        torch.int32: pygloo.glooDataType_t.glooInt32,
        torch.int64: pygloo.glooDataType_t.glooInt64,
        torch.long: pygloo.glooDataType_t.glooInt64,
        # FLOAT types
        torch.half: pygloo.glooDataType_t.glooFloat16,
        torch.float: pygloo.glooDataType_t.glooFloat32,
        torch.float16: pygloo.glooDataType_t.glooFloat16,
        torch.float32: pygloo.glooDataType_t.glooFloat32,
        torch.float64: pygloo.glooDataType_t.glooFloat64,
        torch.double: pygloo.glooDataType_t.glooFloat64,
    }

    TORCH_NUMPY_DTYPE_MAP = {
        # INT types
        torch.int: numpy.int32,
        torch.uint8: numpy.uint8,
        torch.int8: numpy.int8,
        torch.int32: numpy.int32,
        torch.int64: numpy.int64,
        torch.long: numpy.int64,
        # FLOAT types
        torch.half: numpy.half,
        torch.float: numpy.float32,
        torch.float16: numpy.float16,
        torch.float32: numpy.float32,
        torch.float64: numpy.float64,
    }


def create_gloo_context(rank, world_size):
    """Create a GLOO context using GLOO APIs.

    Args:
        rank: the rank of this process.
        world_size: the number of processes of this collective group.

    Returns:
        context (pygloo.Context): a GLOO context.
    """
    context = pygloo.rendezvous.Context(rank, world_size)
    return context


def get_gloo_reduce_op(reduce_op):
    """Map the reduce op to GLOO reduce op type.

    Args:
        reduce_op: ReduceOp Enum (SUM/PRODUCT/MIN/MAX).

    Returns:
        (pygloo.ReduceOp): the mapped GLOO reduce op.
    """
    if reduce_op not in GLOO_REDUCE_OP_MAP:
        raise RuntimeError("Gloo does not support reduce op: '{}'.".format(reduce_op))
    return GLOO_REDUCE_OP_MAP[reduce_op]


def get_gloo_tensor_dtype(tensor):
    """Return the corresponded GLOO dtype given a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return NUMPY_GLOO_DTYPE_MAP[tensor.dtype.type]
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            if not tensor.is_cuda:
                return TORCH_GLOO_DTYPE_MAP[tensor.dtype]
            else:
                raise ValueError(
                    "Expect torch CPU tensor. Got {}.".format(tensor.device)
                )
    raise ValueError("Unsupported tensor type. Got: {}.".format(type(tensor)))


def get_numpy_tensor_dtype(tensor):
    """Return the corresponded Cupy dtype given a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.dtype.type
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return TORCH_NUMPY_DTYPE_MAP[tensor.dtype]
    raise ValueError(
        "Unsupported tensor type. Got: {}. Supported "
        "CPU tensor types are: torch.Tensor, "
        "numpy.ndarray.".format(type(tensor))
    )


def get_tensor_ptr(tensor):
    """Return the pointer to the underlying memory storage of a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.ctypes.data
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            if tensor.is_cuda:
                raise RuntimeError(
                    "Torch tensor must be on CPU when using GLOO collectives."
                )
            return tensor.data_ptr()
    raise ValueError(
        "Unsupported tensor type. Got: {}. Supported "
        "CPU tensor types are: torch.Tensor, "
        "numpy.ndarray.".format(type(tensor))
    )


def get_tensor_n_elements(tensor):
    """Return the number of elements in a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.size
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return torch.numel(tensor)
    raise ValueError("Unsupported tensor type. Got: {}.".format(type(tensor)))


def get_gloo_store_path(store_name):
    from ray._private.utils import get_ray_temp_dir

    store_path = f"{get_ray_temp_dir()}_collective/gloo/{store_name}"
    return store_path


def get_tensor_device(tensor):
    if isinstance(tensor, numpy.ndarray):
        return "cpu"
    elif torch_available() and isinstance(tensor, torch.Tensor):
        if not tensor.is_cuda:
            return "cpu"
        else:
            return "cuda"
    else:
        raise RuntimeError("Unrecognized tensor type: '{}'.".format(type(tensor)))


def get_tensor_shape(tensor):
    """Return the shape of the tensor as a list."""
    if isinstance(tensor, numpy.ndarray):
        return list(tensor.shape)
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return list(tensor.size())
    raise ValueError(
        "Unsupported tensor type. Got: {}. Supported "
        "CPU tensor types are: torch.Tensor, "
        "numpy.ndarray.".format(type(tensor))
    )


def copy_tensor(dst_tensor, src_tensor):
    """Copy the content from src_tensor to dst_tensor.

    Args:
        dst_tensor: the tensor to copy from.
        src_tensor: the tensor to copy to.

    Returns:
        None
    """
    copied = True
    if isinstance(dst_tensor, numpy.ndarray) and isinstance(src_tensor, numpy.ndarray):
        numpy.copyto(dst_tensor, src_tensor)
    elif torch_available():
        if isinstance(dst_tensor, torch.Tensor) and isinstance(
            src_tensor, torch.Tensor
        ):
            dst_tensor.copy_(src_tensor)
        elif isinstance(dst_tensor, torch.Tensor) and isinstance(
            src_tensor, numpy.ndarray
        ):
            t = torch.Tensor(src_tensor)
            dst_tensor.copy_(t)
        elif isinstance(dst_tensor, numpy.ndarray) and isinstance(
            src_tensor, torch.Tensor
        ):
            t = src_tensor.numpy()
            numpy.copyto(dst_tensor, t)
        else:
            copied = False
    else:
        copied = False
    if not copied:
        raise ValueError(
            "Unsupported tensor type. Got: {} and {}. Supported "
            "CPU tensor types are: torch.Tensor, numpy.ndarray.".format(
                type(dst_tensor), type(src_tensor)
            )
        )


# Note(Hao): this requires Ray >= 1.2.0,
# otherwise _QueueActor is an actor class.
class glooQueue(_QueueActor):
    def index(self, group_name):
        try:
            return self.queue._queue.index(group_name)
        except ValueError:
            return -1


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self, world_size):
        self.ready_events = [asyncio.Event() for _ in range(world_size)]
        self.world_size = world_size

    def send(self, rank, clear=False):
        self.ready_events[rank].set()
        if clear:
            self.ready_events[rank].clear()

    async def wait(self, should_wait=True):
        if should_wait:
            for i in range(self.world_size):
                await self.ready_events[i].wait()


# The custom store which is implementated in Ray internal kv storage, helping
# to store the rank meta information when setting up the gloo collective group.
class RayInternalKvStore:
    def __init__(self, group_name: str):
        self._group_name = group_name
        self._job_id = ray.get_runtime_context().job_id
        gcs_address = ray._private.worker._global_node.gcs_address
        self._gcs_client = GcsClient(address=gcs_address, nums_reconnect_retry=10)
        internal_kv._initialize_internal_kv(self._gcs_client)

    def set(self, key: str, data: bytes) -> bool:
        key = self.__concat_key_with_prefixes(key)
        ret = internal_kv._internal_kv_put(key, data)
        return ret

    def get(self, key: str) -> bytes:
        key = self.__concat_key_with_prefixes(key)
        ret = internal_kv._internal_kv_get(key)
        return ret

    def wait(self, keys: list):
        while True:
            all_exist = True
            for key in keys:
                key = self.__concat_key_with_prefixes(key)
                result = internal_kv._internal_kv_exists(key)
                if not result:
                    all_exist = False
                    break
            if all_exist:
                return True
            time.sleep(1)

    def __concat_key_with_prefixes(self, original_key):
        """Concat the necessary prefixes and key for isolation purpose for
        different jobs and different groups."""
        return f"{self._job_id.hex()}-{self._group_name}-{original_key}"
