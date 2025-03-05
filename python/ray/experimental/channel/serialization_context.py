import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Set, Tuple, Union

if TYPE_CHECKING:
    import numpy as np
    import torch


_TORCH_WARNING_FILTER_ACTIVATE = True


class _SerializationContext:
    def __init__(self):
        # If true, then tensors found in the data to serialize are extracted
        # and the caller should send them through an external transport.
        self._use_external_transport: bool = False
        # If _use_external_transport is True, then these are
        # the tensors that should be sent or received
        # out-of-band, through the external transport.
        self._out_of_band_tensors: List["torch.Tensor"] = []
        # During serialization, tensors sent out-of-band are replaced with
        # integer placeholders. This tracks the set of placeholders seen.
        self._deserialized_tensor_placeholders: Set[int] = set()

        # Buffer for transferring data between tasks in the same worker process.
        # The key is the channel ID, and the value is the data. We don't use a
        # lock when reading/writing the buffer because a DAG node actor will only
        # execute one task at a time in `do_exec_tasks`. It will not execute multiple
        # Ray tasks on a single actor simultaneously.
        self.intra_process_channel_buffers: Dict[str, Any] = {}
        # The number of readers for each channel. When the number of readers
        # reaches 0, remove the data from the buffer.
        self.channel_id_to_num_readers: Dict[str, int] = {}

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

    def set_use_external_transport(self, use_external_transport: bool) -> None:
        self._use_external_transport = use_external_transport

    def reset_out_of_band_tensors(
        self, tensors: List["torch.Tensor"]
    ) -> Tuple[List["torch.Tensor"], Set[int]]:
        """
        Return and reset the out-of-band tensors and all tensor placeholders
        that were deserialized since the last call to reset.
        """
        prev_tensors = self._out_of_band_tensors
        deserialized_tensor_placeholders = self._deserialized_tensor_placeholders
        self._out_of_band_tensors = tensors
        self._deserialized_tensor_placeholders = set()
        return prev_tensors, deserialized_tensor_placeholders

    def serialize_tensor(self, tensor: "torch.Tensor") -> Union[int, "np.ndarray"]:
        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current()
        if self._use_external_transport and tensor.device == ctx.torch_device:
            # External transport is enabled and we found a tensor that matches
            # our device.  Add the actual tensor to a buffer. The buffer of
            # tensors should later be popped by the caller and sent via
            # external transport.
            self._out_of_band_tensors.append(tensor)
            # Return a placeholder.
            return len(self._out_of_band_tensors) - 1

        return self.serialize_to_numpy(tensor)

    def serialize_to_numpy(
        self, tensor: "torch.Tensor"
    ) -> Tuple["np.ndarray", "torch.dtype"]:
        import torch

        # Transfer through Ray's shared memory store for now.
        # TODO(swang): This requires two copies, one to transfer from GPU to
        # CPU and another from CPU to shared memory. Ideally we should elide
        # the first copy and memcpy directly from GPU to the shared memory
        # buffer.
        if tensor.device.type == "cuda":
            tensor = tensor.to("cpu")

        # Numpy does not have an equivalent dtype for all torch dtypes, so
        # instead of casting directly to numpy, we first use a view with a
        # common dtype and then view as numpy array.
        return (tensor.view(torch.uint8).numpy(), tensor.dtype)

    def deserialize_tensor(self, val: Union["np.ndarray", int]):
        # Found a placeholder for a tensor that was serialized via NCCL.
        # Replace it with the corresponding deserialized tensor.
        if isinstance(val, int):
            placeholder = val
            self._deserialized_tensor_placeholders.add(placeholder)
            assert placeholder < len(self._out_of_band_tensors)
            return self._out_of_band_tensors[placeholder]

        return self.deserialize_from_numpy(val)

    def deserialize_from_numpy(
        self, np_array_dtype: Tuple["np.ndarray", "torch.dtype"]
    ):
        import torch

        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current()

        np_array, dtype = np_array_dtype

        # TODO(swang): Support local P2P transfers if available.
        # If there is a GPU assigned to this worker, move it there.
        if ctx.torch_device is not None and ctx.torch_device.type == "cuda":

            def convert_numpy_to_tensor(np_array, ctx):
                # It does zero-copy convert np_array inside shared memroy to
                # a tensor. Since we move data to GPU immediately, it is safe.
                cpu_tensor = torch.from_numpy(np_array).view(dtype)
                return cpu_tensor.to(device=ctx.torch_device)

            global _TORCH_WARNING_FILTER_ACTIVATE
            # filtering warning messages would be the bottleneck for
            # deserializing torch tensors. Since the warning only prompts once,
            # we would only deal with it for the first time.
            if _TORCH_WARNING_FILTER_ACTIVATE:
                with warnings.catch_warnings():
                    # Since np_array.is_writable is False (it is set by Ray),
                    # this raises a warning. Suppress it.
                    warnings.filterwarnings(
                        "ignore",
                        category=UserWarning,
                        message="The given NumPy array is not writable",
                    )
                    # gpu_tensor = convert_numpy_to_tensor(np_array, ctx)
                    gpu_tensor = convert_numpy_to_tensor(np_array, ctx)
                _TORCH_WARNING_FILTER_ACTIVATE = False
            else:
                gpu_tensor = convert_numpy_to_tensor(np_array, ctx)

            return gpu_tensor

        # TODO(swang): Use zero-copy from_numpy() if np_array.flags.writeable
        # is True. This is safe to set when deserializing np_array if the
        # upstream task has num_readers=1.
        return torch.tensor(np_array, device=ctx.torch_device).view(dtype)
