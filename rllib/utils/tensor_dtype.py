import numpy as np

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


# Dict of NumPy dtype -> torch dtype
if torch:
    numpy_to_torch_dtype_dict = {
        np.bool_: torch.bool,
        np.uint8: torch.uint8,
        np.int8: torch.int8,
        np.int16: torch.int16,
        np.int32: torch.int32,
        np.int64: torch.int64,
        np.float16: torch.float16,
        np.float32: torch.float32,
        np.float64: torch.float64,
        np.complex64: torch.complex64,
        np.complex128: torch.complex128,
    }
else:
    numpy_to_torch_dtype_dict = {}

# Dict of NumPy dtype -> tf dtype
if tf:
    numpy_to_tf_dtype_dict = {
        np.bool_: tf.bool,
        np.uint8: tf.uint8,
        np.int8: tf.int8,
        np.int16: tf.int16,
        np.int32: tf.int32,
        np.int64: tf.int64,
        np.float16: tf.float16,
        np.float32: tf.float32,
        np.float64: tf.float64,
        np.complex64: tf.complex64,
        np.complex128: tf.complex128,
    }
else:
    numpy_to_tf_dtype_dict = {}

# Dict of torch dtype -> NumPy dtype
torch_to_numpy_dtype_dict = {
    value: key for (key, value) in numpy_to_torch_dtype_dict.items()
}
# Dict of tf dtype -> NumPy dtype
tf_to_numpy_dtype_dict = {value: key for (key, value) in numpy_to_tf_dtype_dict.items()}


@DeveloperAPI
def get_np_dtype(x: TensorType) -> np.dtype:
    """Returns the NumPy dtype of the given tensor or array."""
    if torch and isinstance(x, torch.Tensor):
        return torch_to_numpy_dtype_dict[x.dtype]
    if tf and isinstance(x, tf.Tensor):
        return tf_to_numpy_dtype_dict[x.dtype]
    elif isinstance(x, np.ndarray):
        return x.dtype
    else:
        raise TypeError("Unsupported type: {}".format(type(x)))
