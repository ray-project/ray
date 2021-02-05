"""Code to wrap some GLOO API calls."""
import numpy
try:
    import pygloo
except ImportError:
    raise ImportError("Can not import pygloo. Please run 'pip install pygloo' to install pygloo.")

from ray.util.collective.types import ReduceOp, torch_available

GLOO_REDUCE_OP_MAP = {
    ReduceOp.SUM: pygloo.ReduceOp.SUM,
    ReduceOp.PRODUCT: pygloo.ReduceOp.PRODUCT,
    ReduceOp.MIN: pygloo.ReduceOp.MIN,
    ReduceOp.MAX: pygloo.ReduceOp.MAX,
}

# cupy types are the same with numpy types
NUMPY_GLOO_DTYPE_MAP = {
    numpy.uint8: pygloo.glooDataType_t.glooUint8,
    numpy.float16: pygloo.glooDataType_t.glooFloat16,
    numpy.float32: pygloo.glooDataType_t.glooFloat32,
    numpy.float64: pygloo.glooDataType_t.glooFloat64,
}

if torch_available():
    import torch
    TORCH_GLOO_DTYPE_MAP = {
        torch.uint8: pygloo.glooDataType_t.glooUint8,
        torch.float16: pygloo.glooDataType_t.glooFloat16,
        torch.float32: pygloo.glooDataType_t.glooFloat32,
        torch.float64: pygloo.glooDataType_t.glooFloat64,
    }


# def get_gloo_build_version():
#     return get_build_version()


# def get_gloo_runtime_version():
#     return get_version()


def create_gloo_context(world_size, rank):
    """
    Create an gloo context using gloo APIs.

    Args:
        world_size (int): the number of processes of this communcator group.
        rank (int): the rank of this process.
    Returns:
        context (pygloo.Context): a gloo context.
    """
    context = pygloo.rendezvous.Context(rank, world_size);

    return context


def get_gloo_reduce_op(reduce_op):
    """
    Map the reduce op to GLOO reduce op type.

    Args:
        reduce_op (ReduceOp): ReduceOp Enum (SUM/PRODUCT/MIN/MAX).
    Returns:
        (pygloo.ReduceOp): the mapped gloo reduce op.
    """
    if reduce_op not in GLOO_REDUCE_OP_MAP:
        raise RuntimeError(
            "Gloo does not support reduce op: '{}'".format(reduce_op))
    return GLOO_REDUCE_OP_MAP[reduce_op]


def get_gloo_tensor_dtype(tensor):
    """Return the corresponded GLOO dtype given a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return NUMPY_GLOO_DTYPE_MAP[tensor.dtype.type]
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return TORCH_GLOO_DTYPE_MAP[tensor.dtype]
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_ptr(tensor):
    """Return the pointer to the underlying memory storage of a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.ctypes.data
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            if not tensor.is_cuda:
                raise RuntimeError("torch tensor must be on gpu.")
            return tensor.data_ptr()
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_n_elements(tensor):
    """Return the number of elements in a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.size
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return torch.numel(tensor)
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))

def get_gloo_store_path(store_name):
    from ray.utils import get_ray_temp_dir
    store_path = f"{get_ray_temp_dir()}_collective/gloo/{store_name}"
    return store_path