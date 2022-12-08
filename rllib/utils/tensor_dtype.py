from typing import Union
import torch
import numpy as np

from ray.util.annotations import PublicAPI

# Dict of NumPy dtype -> torch dtype
numpy_to_torch_dtype_dict = {
    np.bool       : torch.bool,
    np.uint8      : torch.uint8,
    np.int8       : torch.int8,
    np.int16      : torch.int16,
    np.int32      : torch.int32,
    np.int64      : torch.int64,
    np.float16    : torch.float16,
    np.float32    : torch.float32,
    np.float64    : torch.float64,
    np.complex64  : torch.complex64,
    np.complex128 : torch.complex128
}

# Dict of torch dtype -> NumPy dtype
torch_to_numpy_dtype_dict = {value : key for (key, value) in numpy_to_torch_dtype_dict.items()}

@PublicAPI(stability="alpha")
def get_np_dtype(x: Union[torch.Tensor, np.ndarray]):
    """Returns the NumPy dtype of the given tensor or array."""
    if isinstance(x, torch.Tensor):
        return torch_to_numpy_dtype_dict[x.dtype]
    elif isinstance(x, np.ndarray):
        return x.dtype
    else:
        raise TypeError("Unsupported type: {}".format(type(x)))