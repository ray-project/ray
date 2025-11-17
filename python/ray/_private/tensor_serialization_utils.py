import warnings
from typing import TYPE_CHECKING, Any, Tuple, Union

import numpy as np

if TYPE_CHECKING:
    import torch


class ZeroCopyTensorsWarning(UserWarning):
    """
    Warning for unsafe or failed zero-copy tensor serialization/deserialization.
    """

    pass


warnings.filterwarnings("once", category=ZeroCopyTensorsWarning)


def _zero_copy_tensors_deserializer(
    np_array: np.ndarray, dtype_str: str, shape: Tuple[int, ...], device_str: str
) -> Union["torch.Tensor", np.ndarray]:
    """
    Reconstructs a torch.Tensor from a zero-copy NumPy byte array.

    Args:
        np_array: 1D uint8 NumPy array of the original tensor's raw bytes.
        dtype_str: Full string representation of the original tensor's dtype (e.g., 'torch.float32').
        shape: The original shape of the tensor before serialization.
        device_str: String representation of the original device (e.g., 'cpu', 'cuda:0').

    Returns:
        Reconstructed torch.Tensor on the specified device if successful;
        otherwise, returns the input np_array unchanged and issues a warning.
    """
    try:
        import torch

        # Step 1: Convert uint8 numpy array back to torch tensor
        uint8_tensor = torch.from_numpy(np_array)

        # Step 2: Restore original dtype
        dtype_name = dtype_str.split(".")[-1]
        original_dtype = getattr(torch, dtype_name)

        # Compute number of bytes per element
        dtype_size = torch.tensor([], dtype=original_dtype).element_size()
        if np_array.size % dtype_size != 0:
            raise ValueError(
                f"Byte array size ({np_array.size}) is not divisible by "
                f"dtype size ({dtype_size}) for dtype {dtype_str}"
            )

        # Reshape uint8 tensor to (-1, dtype_size), then view as target dtype
        num_elements = np_array.size // dtype_size
        restored_tensor = uint8_tensor[: num_elements * dtype_size].view(
            dtype=original_dtype
        )

        # Step 3: Reshape to original shape
        restored_tensor = restored_tensor.reshape(shape)

        # Step 4: Move to target device
        dev = torch.device(device_str)
        if dev.type == "cuda" and torch.cuda.is_available():
            target_device = torch.device("cuda")
        else:
            target_device = torch.device("cpu")

        return restored_tensor.to(device=target_device)

    except Exception as e:
        if isinstance(e, ImportError):
            msg = "Zero-copy tensor deserialization failed because PyTorch is not installed. "
        elif isinstance(e, (RuntimeError, TypeError, ValueError, AttributeError)):
            msg = (
                "Zero-copy tensor deserialization failed due to data or runtime issue. "
            )
        else:
            msg = "Unexpected error during zero-copy tensor deserialization. "

        warnings.warn(
            f"{msg}Returning raw NumPy array instead of torch.Tensor. "
            f"This may indicate missing dependencies, corrupted/misaligned data, "
            f"non-contiguous arrays, or incompatible serialization format. Error: {e}",
            ZeroCopyTensorsWarning,
            stacklevel=3,
        )
        return np_array


def zero_copy_tensors_reducer(tensor: "torch.Tensor") -> Tuple[Any, Tuple[Any, ...]]:
    """
    Reducer for zero-copy serialization of read-only torch.Tensor.
    Only supports detached, contiguous, CPU tensors. Uses uint8 NumPy view
    to enable pickle5 out-of-band buffer transmission.

    Args:
        tensor: Contiguous, detached tensor to serialize.

    Returns:
        A tuple (deserializer_callable, args_tuple) suitable for pickle.
    """
    warnings.warn(
        "Zero-copy tensor serialization is enabled, but it only works safely for read-only tensors "
        "(detached, no gradients, contiguous). Modifiable or non-contiguous tensors may cause data corruption.",
        ZeroCopyTensorsWarning,
        stacklevel=3,
    )

    import torch

    # Move to CPU and ensure contiguous
    cpu_tensor = tensor.detach().cpu().contiguous()
    # Flatten to 1D for safe uint8 view (handles scalars)
    flat_tensor = cpu_tensor.reshape(-1)
    # View as uint8 bytes
    uint8_view = flat_tensor.view(torch.uint8)
    np_array = uint8_view.numpy()
    return _zero_copy_tensors_deserializer, (
        np_array,
        str(tensor.dtype),
        tuple(tensor.shape),
        str(tensor.device),
    )
