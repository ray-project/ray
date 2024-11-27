from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

if TYPE_CHECKING:
    import numpy as np
    import torch


class _ExternalTransportSerializationContext:
    """
    Helper context manager to set out-of-band tensors during
    serialization/deserialization. During serialization, this can be used to
    set the serialization context to extract torch.Tensors and replace them
    with integer placeholders. During deserialization, this can be used to
    replace the placeholders with out-of-band torch.Tensors.
    """

    def __init__(
        self,
        ctx: "_SerializationContext",
        tensors: Optional[List["torch.Tensor"]] = None,
    ):
        self.ctx = ctx
        self.tensors = tensors

    def __enter__(self):
        self.ctx.reset_out_of_band_tensors(self.tensors)
        self.ctx.set_use_external_transport(True)

    def __exit__(self, exc_type, exc_value, traceback):
        self.ctx.set_use_external_transport(False)
        tensors, seen_tensor_placeholders = self.ctx.reset_out_of_band_tensors()

        if self.tensors is not None:
            # Check that the serialization context's out-of-band tensors was
            # not modified.
            assert self.tensors is tensors
        if exc_type is None:
            # If no exception was thrown, check that all tensors provided were
            # found in the serialized/deserialized data.
            assert seen_tensor_placeholders == set(range(len(tensors))), (
                seen_tensor_placeholders,
                tensors,
            )


class _SerializationContext:
    def __init__(self):
        # If true, then tensors found in the data to serialize are extracted
        # and the caller should send them through an external transport.
        self._use_external_transport: bool = False
        # If _use_external_transport is True, then these are
        # the tensors that should be sent or received
        # out-of-band, through the external transport.
        self._out_of_band_tensors: List["torch.Tensor"] = []
        # Reverse mapping of above. Used to deduplicate references to the same
        # tensor and for cases where the tensor extraction and order of
        # transfer is done before serialization.
        self._out_of_band_tensors_to_placeholders: Dict["torch.Tensor", int] = {}
        # During serialization/deserialization, tensors sent out-of-band are
        # replaced with integer placeholders or vice versa. This tracks the set
        # of placeholders seen.
        self._seen_tensor_placeholders: Set[int] = set()

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

    def external_transport(self, tensors: Optional[List["torch.Tensor"]] = None):
        return _ExternalTransportSerializationContext(self, tensors)

    def reset_out_of_band_tensors(
        self, tensors: Optional[List["torch.Tensor"]] = None
    ) -> Tuple[List["torch.Tensor"], Set[int]]:
        """
        Return and reset the out-of-band tensors and all tensor placeholders
        that were deserialized since the last call to reset.
        """
        prev_tensors = self._out_of_band_tensors
        seen_tensor_placeholders = self._seen_tensor_placeholders
        if tensors is None:
            tensors = []
        self._out_of_band_tensors = tensors
        self._out_of_band_tensors_to_placeholders = {
            tensor: idx for idx, tensor in enumerate(self._out_of_band_tensors)
        }
        self._seen_tensor_placeholders = set()
        return prev_tensors, seen_tensor_placeholders

    def serialize_tensor(self, tensor: "torch.Tensor") -> Union[int, "np.ndarray"]:
        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current()
        if self._use_external_transport and tensor.device == ctx.torch_device:
            if tensor not in self._out_of_band_tensors_to_placeholders:
                # External transport is enabled and we found a tensor that matches
                # our device. Add the actual tensor to a buffer. The buffer of
                # tensors should later be popped by the caller and sent via
                # external transport.
                idx = len(self._out_of_band_tensors)
                self._out_of_band_tensors.append(tensor)
                self._out_of_band_tensors_to_placeholders[tensor] = idx

            placeholder = self._out_of_band_tensors_to_placeholders[tensor]
            self._seen_tensor_placeholders.add(placeholder)
            return placeholder

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
            placeholder = val
            self._seen_tensor_placeholders.add(placeholder)
            assert placeholder < len(self._out_of_band_tensors)
            return self._out_of_band_tensors[placeholder]

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
