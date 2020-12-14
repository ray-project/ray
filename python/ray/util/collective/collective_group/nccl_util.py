"""Code to wrap some NCCL API calls."""
import numpy
try:
    import cupy
    from cupy.cuda import nccl
    from cupy.cuda.nccl import get_version
    from cupy.cuda.nccl import get_build_version
    from cupy.cuda.nccl import NcclCommunicator
except ImportError:
    raise ImportError("NCCL in Ray requires Cupy being available!")

from ray.util.collective.types import ReduceOp, torch_available

NCCL_REDUCE_OP_MAP = {
    ReduceOp.SUM: nccl.NCCL_SUM,
    ReduceOp.PRODUCT: nccl.NCCL_PROD,
    ReduceOp.MIN: nccl.NCCL_MIN,
    ReduceOp.MAX: nccl.NCCL_MAX,
}

# cupy types are the same with numpy types
NUMPY_NCCL_DTYPE_MAP = {
    numpy.uint8: nccl.NCCL_UINT8,
    numpy.float16: nccl.NCCL_FLOAT16,
    numpy.float32: nccl.NCCL_FLOAT32,
    numpy.float64: nccl.NCCL_FLOAT64,
}

if torch_available():
    import torch
    import torch.utils.dlpack
    TORCH_NCCL_DTYPE_MAP = {
        torch.uint8: nccl.NCCL_UINT8,
        torch.float16: nccl.NCCL_FLOAT16,
        torch.float32: nccl.NCCL_FLOAT32,
        torch.float64: nccl.NCCL_FLOAT64,
    }

    TORCH_NUMPY_DTYPE_MAP = {
        torch.uint8: numpy.uint8,
        torch.float16: numpy.float16,
        torch.float32: numpy.float32,
        torch.float64: numpy.float64,
    }


def get_nccl_build_version():
    return get_build_version()


def get_nccl_runtime_version():
    return get_version()


def get_nccl_unique_id():
    return nccl.get_unique_id()


def create_nccl_communicator(world_size, nccl_unique_id, rank):
    """
    Create an NCCL communicator using NCCL APIs.

    Args:
        world_size (int): the number of processes of this communcator group.
        nccl_unique_id (str): the NCCLUniqueID for this group.
        rank (int): the rank of this process.
    Returns:
        comm (nccl.ncclComm_t): an NCCL communicator.
    """
    # TODO(Hao): make this inside the NCCLComm class,
    #  and implement the abort method. Make it RAII.
    comm = NcclCommunicator(world_size, nccl_unique_id, rank)
    return comm


def get_nccl_reduce_op(reduce_op):
    """
    Map the reduce op to NCCL reduce op type.

    Args:
        reduce_op (ReduceOp): ReduceOp Enum (SUM/PRODUCT/MIN/MAX).
    Returns:
        (nccl.ncclRedOp_t): the mapped NCCL reduce op.
    """
    if reduce_op not in NCCL_REDUCE_OP_MAP:
        raise RuntimeError(
            "NCCL does not support reduce op: '{}'".format(reduce_op))
    return NCCL_REDUCE_OP_MAP[reduce_op]


def get_nccl_tensor_dtype(tensor):
    """Return the corresponded NCCL dtype given a tensor."""
    if isinstance(tensor, cupy.ndarray):
        return NUMPY_NCCL_DTYPE_MAP[tensor.dtype.type]
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return TORCH_NCCL_DTYPE_MAP[tensor.dtype]
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_cupy_tensor_dtype(tensor):
    """Return the corresponded Cupy dtype given a tensor."""
    if isinstance(tensor, cupy.ndarray):
        return tensor.dtype.type
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return TORCH_NUMPY_DTYPE_MAP[tensor.dtype]
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_ptr(tensor):
    """Return the pointer to the underlying memory storage of a tensor."""
    if isinstance(tensor, cupy.ndarray):
        return tensor.data.ptr
    if isinstance(tensor, numpy.ndarray):
        return tensor.data
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            if not tensor.is_cuda:
                raise RuntimeError("torch tensor must be on gpu.")
            return tensor.data_ptr()
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_n_elements(tensor):
    """Return the number of elements in a tensor."""
    if isinstance(tensor, cupy.ndarray) or isinstance(tensor, numpy.ndarray):
        return tensor.size
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return torch.numel(tensor)
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_shape(tensor):
    """Return the shape of the tensor as a list."""
    if isinstance(tensor, cupy.ndarray):
        return list(tensor.shape)
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return list(tensor.size())
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_strides(tensor):
    """Return the strides of the tensor as a list."""
    if isinstance(tensor, cupy.ndarray):
        return [int(stride / tensor.dtype.itemsize) for stride in tensor.strides]
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return list(tensor.stride())
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def copy_tensor(dst_tensor, src_tensor):
    """
    Copy the content from src_tensor to dst_tensor.

    Args:
        dst_tensor:
        src_tensor:

    Returns:
        None
    """
    if isinstance(dst_tensor, cupy.ndarray) \
        and isinstance(src_tensor, cupy.ndarray):
        cupy.copyto(dst_tensor, src_tensor)
        return
    if torch_available():
        if isinstance(dst_tensor, torch.Tensor) and isinstance(src_tensor, torch.Tensor):
            dst_tensor.copy_(src_tensor)
            return
        if isinstance(dst_tensor, torch.Tensor) and isinstance(src_tensor, cupy.ndarray):
            t = torch.utils.dlpack.from_dlpack(src_tensor.toDlpack())
            dst_tensor.copy_(t)
            return
        if isinstance(dst_tensor, cupy.ndarray) and isinstance(src_tensor, torch.Tensor):
            t = cupy.fromDlpack(torch.utils.dlpack.to_dlpack(src_tensor))
            cupy.copyto(dst_tensor, t)
            return
    raise ValueError("Unsupported tensor type. "
                     "Got: {} and {}.".format(type(dst_tensor), type(src_tensor)))


