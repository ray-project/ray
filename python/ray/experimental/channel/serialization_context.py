from typing import TYPE_CHECKING, List, Optional, Union

if TYPE_CHECKING:
    import numpy as np
    import torch


class _SerializationContext:
    def __init__(self):
        self.torch_device: Optional["torch.device"] = None
        self.use_external_transport: bool = False
        self.tensors: List["torch.Tensor"] = []

    def set_use_external_transport(self, use_external_transport: bool) -> None:
        self.use_external_transport = use_external_transport

    def set_torch_device(self, torch_device: "torch.device") -> None:
        self.torch_device = torch_device

    def reset_tensors(self, tensors: List["torch.Tensor"]) -> List["torch.Tensor"]:
        prev_tensors = self.tensors
        self.tensors = tensors
        return prev_tensors

    def serialize_tensor(self, tensor: "torch.Tensor") -> Union[int, "np.ndarray"]:
        if self.use_external_transport and tensor.device == self.torch_device:
            # External transport is enabled and we found a tensor that matches
            # our device.  Add the actual tensor to a buffer. The buffer of
            # tensors should later be popped by the caller and sent via
            # external transport.
            self.tensors.append(tensor)
            # Return a placeholder.
            return len(self.tensors) - 1

        return self.serialize_to_numpy(tensor)

    def serialize_to_numpy(self, tensor: "torch.Tensor") -> "np.ndarray":
        # Transfer through Ray's shared memory store for now.
        # TODO(swang): This requires two copies, one to transfer from GPU to
        # CPU and another from CPU to shared memory. Ideally we should elide
        # the first copy and memcpy directly from GPU to the shared memory
        # buffer.
        if tensor.device.type == "cuda":
            tensor = tensor.to("cpu")

        return tensor.numpy()

    def deserialize_tensor(self, val: Union["np.ndarray", int]):
        # Found a placeholder for a tensor that was serialized via NCCL.
        # Replace it with the corresponding deserialized tensor.
        if isinstance(val, int):
            return self.tensors[val]

        return self.deserialize_from_numpy(val)

    def deserialize_from_numpy(self, np_array: "np.ndarray"):
        import torch

        # TODO(swang): Support local P2P transfers if available.
        # If there is a GPU assigned to this worker, move it there.
        if self.torch_device is not None and self.torch_device.type == "cuda":
            # Use zero-copy from_numpy() because we are going to copy to GPU
            # anyway.
            # TODO: Pin the np_array memory to reduce data movement time.
            # TODO: Set np_array.flags.writeable=True to avoid the PyTorch
            # warning about not owning the underlying memory. This is safe to
            # do as long as all other readers are also copying the data to a
            # GPU.
            cpu_tensor = torch.from_numpy(np_array)
            return cpu_tensor.to(device=self.torch_device)

        # TODO(swang): Use zero-copy from_numpy() if np_array.flags.writeable
        # is True. This is safe to set when deserializing np_array if the
        # upstream task has num_readers=1.
        return torch.tensor(np_array, device=self.torch_device)
