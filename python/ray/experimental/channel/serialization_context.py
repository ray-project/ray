from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union

from ray.exceptions import RayAdagDeviceMismatchError

if TYPE_CHECKING:
    import numpy as np
    import torch


class _SerializationContext:
    def __init__(self):
        self.use_external_transport: bool = False
        # The target device type for tensors.
        self._target_device_type: Optional[Literal["cuda", "cpu", "auto"]] = None
        self.tensors: List["torch.Tensor"] = []
        # Buffer for transferring data between tasks in the same worker process.
        # The key is the channel ID, and the value is the data. We don't use a
        # lock when reading/writing the buffer because a DAG node actor will only
        # execute one task at a time in `do_exec_tasks`. It will not execute multiple
        # Ray tasks on a single actor simultaneously.
        self.intra_process_channel_buffers: Dict[str, Any] = {}
        # The number of readers for each channel. When the number of readers
        # reaches 0, remove the data from the buffer.
        self.channel_id_to_num_readers: Dict[str, int] = {}

    # TODO(sang): Make it context manager.
    def set_use_external_transport(self, use_external_transport: bool) -> None:
        self.use_external_transport = use_external_transport

    def set_target_device_type(self, target_device_type: Optional[str]):
        self._target_device_type = target_device_type

    @property
    def target_device_type(self) -> Optional[str]:
        return self._target_device_type

    def set_data(self, channel_id: str, value: Any, num_readers: int) -> None:
        assert num_readers > 0, "num_readers must be greater than 0."
        assert (
            channel_id not in self.intra_process_channel_buffers
        ), f"Channel {channel_id} already exists in the buffer."
        assert (
            channel_id not in self.channel_id_to_num_readers
        ), f"Channel {channel_id} already exists in the channel_id_to_num_readers."

        self.intra_process_channel_buffers[channel_id] = value
        self.channel_id_to_num_readers[channel_id] = num_readers

    def has_data(self, channel_id: str) -> bool:
        return channel_id in self.intra_process_channel_buffers

    def get_data(self, channel_id: str) -> Any:
        assert (
            channel_id in self.intra_process_channel_buffers
        ), f"Channel {channel_id} does not exist in the buffer."
        assert (
            channel_id in self.channel_id_to_num_readers
        ), f"Channel {channel_id} does not exist in the channel_id_to_num_readers."

        self.channel_id_to_num_readers[channel_id] -= 1
        if self.channel_id_to_num_readers[channel_id] == 0:
            # All readers have read the data, so we can remove it.
            self.channel_id_to_num_readers.pop(channel_id)
            return self.intra_process_channel_buffers.pop(channel_id)
        return self.intra_process_channel_buffers[channel_id]

    def reset_data(self, channel_id: str) -> None:
        self.intra_process_channel_buffers.pop(channel_id, None)
        self.channel_id_to_num_readers.pop(channel_id, None)

    def reset_tensors(self, tensors: List["torch.Tensor"]) -> List["torch.Tensor"]:
        prev_tensors = self.tensors
        self.tensors = tensors
        return prev_tensors

    def serialize_tensor(self, tensor: "torch.Tensor") -> Union[int, "np.ndarray"]:
        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current()
        if self.use_external_transport and tensor.device == ctx.torch_device:
            # External transport is enabled and we found a tensor that matches
            # our device.  Add the actual tensor to a buffer. The buffer of
            # tensors should later be popped by the caller and sent via
            # external transport.
            self.tensors.append(tensor)
            # Return a placeholder.
            return len(self.tensors) - 1

        if self._target_device_type is None or self._target_device_type == "auto":
            # By default, use the same device type as a tensor.
            target_device_type = tensor.device.type
        else:
            target_device_type = self._target_device_type
        return (self.serialize_to_numpy(tensor), target_device_type)

    def serialize_to_numpy(self, tensor: "torch.Tensor") -> "np.ndarray":
        # Transfer through Ray's shared memory store for now.
        # TODO(swang): This requires two copies, one to transfer from GPU to
        # CPU and another from CPU to shared memory. Ideally we should elide
        # the first copy and memcpy directly from GPU to the shared memory
        # buffer.
        if tensor.device.type == "cuda":
            tensor = tensor.to("cpu")

        return tensor.numpy()

    def deserialize_tensor(self, val: Union[Tuple["np.ndarray", str], int]):
        # Found a placeholder for a tensor that was serialized via NCCL.
        # Replace it with the corresponding deserialized tensor.
        if isinstance(val, int):
            return self.tensors[val]

        val, target_device_type = val
        return self.deserialize_from_numpy(val, target_device_type)

    def deserialize_from_numpy(self, np_array: "np.ndarray", target_device_type: str):
        import torch

        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current()

        # TODO(swang): Support local P2P transfers if available.
        # If there is a GPU assigned to this worker, move it there.
        if target_device_type == "cuda":
            # OPTIMIZATION: Use zero-copy from_numpy() because we are going
            # to copy to GPU anyway.
            # TODO: Set np_array.flags.writeable=True to avoid the PyTorch
            # warning about not owning the underlying memory. This is safe to
            # do as long as all other readers are also copying the data to a
            # GPU.
            if ctx.torch_device.type != target_device_type:
                # TODO(sang): This should have a better error message. I.e.,
                # it should say what's the upstream task.
                raise RayAdagDeviceMismatchError(
                    f"The tensor should be created to a device type '{target_device_type}', "
                    f"but there's only '{ctx.torch_device.type}' available. Please specify "
                    f".with_type_hint(TorchTensorType(_device='{ctx.torch_device.type}')) "
                    "to transfer tensor to the correct device."
                )

            cpu_tensor = torch.from_numpy(np_array)
            return cpu_tensor.to(device=ctx.torch_device, non_blocking=True)
        # TODO(swang): Use zero-copy from_numpy() if np_array.flags.writeable
        # is True. This is safe to set when deserializing np_array if the
        # upstream task has num_readers=1.
        # TODO(sang): This doesn't work with different accelerators. Fix it.
        elif target_device_type == "cpu":
            device = "cpu"
        else:
            raise TypeError(f"The device type {target_device_type} is not supported.")

        return torch.tensor(np_array, device=device)
