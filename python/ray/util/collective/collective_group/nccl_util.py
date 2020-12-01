"""Code to wrap some NCCL API calls."""
import numpy
try:
    import cupy
    from cupy.cuda import nccl
    from cupy.cuda.nccl import get_unique_id
    from cupy.cuda.nccl import get_version
    from cupy.cuda.nccl import get_build_version
    from cupy.cuda.nccl import NcclCommunicator
    from cupy.cuda.nccl import NcclError
except ImportError:
    raise ImportError('NCCL in Ray requires Cupy being available!')

from python.ray.util.collective.types import ReduceOp, torch_available


NCCL_REDUCE_OP_MAP = {
    ReduceOp.SUM: nccl.NCCL_SUM,
    ReduceOp.PRODUCT: nccl.NCCL_PROD,
    ReduceOp.MIN: nccl.NCCL_MIN,
    ReduceOp.MAX: nccl.NCCL_MAX,
}


# cupy types are the same with numpy types
NUMPY_NCCL_DTYPE_MAP = {
    numpy.bool: nccl.NCCL_UINT8,
    numpy.float16: nccl.NCCL_FLOAT16,
    numpy.float32: nccl.NCCL_FLOAT32,
    numpy.float64: nccl.NCCL_FLOAT64,
}

if torch_available():
    import torch
    TORCH_NCCL_DTYPE_MAP = {
        torch.bool: nccl.NCCL_UINT8,
        torch.float16: nccl.NCCL_FLOAT16,
        torch.float32: nccl.NCCL_FLOAT32,
        torch.float64: nccl.NCCL_FLOAT64,
    }


def get_nccl_build_version():
    return get_build_version()


def get_nccl_runtime_version():
    return get_build_version()


def create_nccl_communicator(world_size, nccl_unique_id, rank):
    """
    Create an NCCL communicator using NCCL APIs.

    TODO(Hao): verify that the call has big overhead.

    Returns:
        comm(type): an NCCL communicator.

    """
    # TODO(Hao): make this inside the NCCLComm class, and implement the abort method.
    #            Make it RAII
    comm = NcclCommunicator(world_size, nccl_unique_id, rank)
    return comm


def get_nccl_reduce_op(reduce_op):
    """
    Map the reduce op to NCCL reduce op type.

    Returns:
        Nccl_op (nccl.ncclRedOp_t)
    """
    if reduce_op not in NCCL_REDUCE_OP_MAP:
        raise RuntimeError('NCCL does not support ReduceOp: {}'.format(reduce_op))
    return NCCL_REDUCE_OP_MAP[reduce_op]


def get_nccl_tensor_dtype(tensor):
    """Return the corresponded NCCL dtype given a tensor."""
    if isinstance(tensor, cupy.ndarray):
        return NUMPY_NCCL_DTYPE_MAP[tensor.dtype]
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return TORCH_NCCL_DTYPE_MAP[tensor.dtype]
    raise ValueError('Unsupported tensor type')


def get_tensor_ptr(tensor):
    """Return the pointer to the underlying memory storage of a tensor."""
    if not tensor:
        raise RuntimeError('None Tensor.')
    if isinstance(tensor, cupy.ndarray):
        return tensor.data.ptr
    if isinstance(tensor, numpy.ndarray):
        return tensor.data
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return tensor.data_ptr()
    raise ValueError('Unsupported tensor type')


def get_tensor_n_elements(tensor):
    """Return the number of elements in a tensor."""
    if not tensor:
        raise RuntimeError('None Tensor.')
    if isinstance(tensor, cupy.ndarray) or isinstance(tensor, numpy.ndarray):
        return tensor.size
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return torch.numel(tensor)
    raise ValueError('Unsupported tensor type')
