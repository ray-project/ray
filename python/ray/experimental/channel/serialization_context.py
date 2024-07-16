from typing import TYPE_CHECKING, Any, Dict, List, Union

if TYPE_CHECKING:
    import numpy as np
    import torch


class _SerializationContext:
    def __init__(self):
        self.use_external_transport: bool = False
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

    def set_use_external_transport(self, use_external_transport: bool) -> None:
        self.use_external_transport = use_external_transport

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

        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current()

        # TODO(swang): Support local P2P transfers if available.
        # If there is a GPU assigned to this worker, move it there.
        if ctx.torch_device is not None and ctx.torch_device.type == "cuda":
            # Use zero-copy from_numpy() because we are going to copy to GPU
            # anyway.
            # TODO: Pin the np_array memory to reduce data movement time.
            # TODO: Set np_array.flags.writeable=True to avoid the PyTorch
            # warning about not owning the underlying memory. This is safe to
            # do as long as all other readers are also copying the data to a
            # GPU.
            cpu_tensor = torch.from_numpy(np_array)
            return cpu_tensor.to(device=ctx.torch_device)

        # TODO(swang): Use zero-copy from_numpy() if np_array.flags.writeable
        # is True. This is safe to set when deserializing np_array if the
        # upstream task has num_readers=1.
        return torch.tensor(np_array, device=ctx.torch_device)
