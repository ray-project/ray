import warnings
from typing import TYPE_CHECKING, Any, Tuple

if TYPE_CHECKING:
    import numpy as np
    import torch


class ZeroCopyTensorsWarning(UserWarning):
    """
    Warning for unsafe or failed zero-copy tensor serialization/deserialization.
    """

    pass


warnings.filterwarnings("once", category=ZeroCopyTensorsWarning)


def _zero_copy_tensors_deserializer(
    np_array: "np.ndarray", dtype_str: str, shape: Tuple[int, ...], device_str: str
) -> "torch.Tensor":
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

    Raises:
        ImportError/DeserializationError: If deserialization fails for any reason (e.g., missing PyTorch
                            dtype mismatch, shape inconsistency, device error, etc.).
    """
    try:
        import torch
    except ImportError as e:
        raise ImportError(
            "Zero-copy tensor deserialization failed: PyTorch is not installed."
        ) from e

    try:
        # Step 1: Convert uint8 numpy array back to torch tensor
        uint8_tensor = torch.from_numpy(np_array)

        # Step 2: Restore original dtype
        dtype_name = dtype_str.split(".")[-1]
        if not hasattr(torch, dtype_name):
            raise ValueError(f"Invalid or unsupported dtype string: {dtype_str}")
        original_dtype = getattr(torch, dtype_name)

        # Compute number of bytes per element
        dtype_size = torch.tensor([], dtype=original_dtype).element_size()
        if np_array.size % dtype_size != 0:
            raise ValueError(
                f"Byte array size ({np_array.size}) is not divisible by "
                f"dtype size ({dtype_size}) for dtype {dtype_str}"
            )

        # Step 3: Reshape and reinterpret bytes as target dtype
        restored_tensor = uint8_tensor.view(original_dtype).reshape(shape)

        # Step 4: Move to target device
        return restored_tensor.to(device=device_str)

    except Exception as e:
        from ray._private.serialization import DeserializationError

        raise DeserializationError(
            f"Failed to deserialize zero-copy tensor from byte array. "
            f"Input dtype={dtype_str}, shape={shape}, device={device_str}. "
            f"Underlying error: {type(e).__name__}: {e}"
        ) from e


def zero_copy_tensors_reducer(tensor: "torch.Tensor") -> Tuple[Any, Tuple[Any, ...]]:
    """Pickle serializer for zero-copy serialization of read-only torch.Tensor.

    This serializer aims to avoid copying tensor data by using a NumPy uint8 view,
    which enables pickle5's out-of-band buffer transmission. However, true zero-copy
    is only possible when the input tensor is already:

    - On CPU,
    - Detached from the computation graph (no gradients),
    - Contiguous in memory.

    If the input tensor does **not** meet these conditions, this function will:

    - Call `.detach()` to remove gradient information,
    - Move the tensor to CPU (copying data if it's on GPU or another device),
    - Make the tensor contiguous (copying data if it's non-contiguous).

    These operations may incur one or two full copies of the tensor data,
    negating zero-copy benefits. A warning is issued in such cases.

    Args:
        tensor: The input torch.Tensor to serialize. Can be on any device,
                with or without gradients, contiguous or not — but zero-copy
                is only achieved if it is already CPU, detached, and contiguous.

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

    # Detach the tensor from gradients and computation graph.
    # Move it to cpu (this is a noop if the tensor is already in main memory, but will create a copy if the
    # the tensor is on an accelerator).
    # Ensure that the tensor is contiguous. If the tensor is not contiguous, this will create a contiguous
    # copy.
    cpu_tensor = tensor.detach().cpu()
    if not cpu_tensor.is_contiguous():
        warnings.warn(
            "The input tensor is non-contiguous. A copy will be made to ensure contiguity. "
            "For zero-copy serialization, please ensure the tensor is contiguous before passing it "
            "(e.g., by calling `.contiguous()`).",
            ZeroCopyTensorsWarning,
            stacklevel=3,
        )
        cpu_tensor = cpu_tensor.contiguous()

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
