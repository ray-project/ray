from typing import TYPE_CHECKING, Any, List, Optional, Tuple

import ray
from ray.experimental.channel import ChannelOutputType
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import numpy as np
    import torch

# Add this much padding to all shared memory buffers used to store tensors.
# This shouldn't affect performance because the buffer is allocated once and
# when transferring across nodes, only the serialized buffer is copied.
TENSOR_BUFFER_PADDING_FRACTION = 0.2
# 100KB so that we have room to store an exception if needed.
MIN_TENSOR_BUFFER_SIZE = 100_000


@PublicAPI(stability="alpha")
class TorchTensorType(ChannelOutputType):
    NCCL = "nccl"

    def __init__(
        self, shape: Tuple[int], dtype: "torch.dtype", transport: Optional[str] = None
    ):
        self.shape = shape
        self.dtype = dtype
        self.transport = transport

    @staticmethod
    def register_custom_serializer(outer: Any) -> None:
        # Helper method to run on the DAG driver and actors to register custom
        # serializers.
        import torch

        from ray.air._internal import torch_utils

        if ray.get_gpu_ids():
            default_device = torch_utils.get_devices()[0]
        else:
            # torch_utils defaults to returning GPU 0 if no
            # GPU IDs were assigned by Ray. We instead want
            # the default to be CPU.
            default_device = torch.device("cpu")

        torch_tensor_serializer = _TorchTensorSerializer(default_device)

        CUSTOM_SERIALIZERS = (
            (
                _TorchTensorWrapper,
                torch_tensor_serializer.serialize_to_numpy,
                torch_tensor_serializer.deserialize_from_numpy,
            ),
        )

        for cls, serializer, deserializer in CUSTOM_SERIALIZERS:
            ray.util.serialization.register_serializer(
                cls, serializer=serializer, deserializer=deserializer
            )

        outer._torch_tensor_serializer = torch_tensor_serializer

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        readers: List[Optional["ray.actor.ActorHandle"]],
    ) -> type:
        if self.transport == self.NCCL:
            from ray.experimental.channel.torch_tensor_nccl_channel import (
                TorchTensorNcclChannel,
            )

            return TorchTensorNcclChannel(writer, readers, self)
        else:
            import torch

            from ray.experimental.channel.shared_memory_channel import Channel

            TORCH_DTYPE_ITEMSIZE_MAP = {
                # INT types
                torch.int: 4,
                torch.uint8: 1,
                torch.int8: 1,
                torch.int32: 4,
                torch.int64: 8,
                torch.long: 8,
                # FLOAT types
                torch.half: 2,
                torch.float: 4,
                torch.float16: 2,
                torch.float32: 4,
                torch.float64: 8,
                torch.double: 8,
            }

            shape = self.shape
            if isinstance(shape, int):
                shape = (shape,)

            num_elements = 1
            for dim in shape:
                num_elements *= dim
            element_size_bytes = TORCH_DTYPE_ITEMSIZE_MAP[self.dtype]
            buffer_size_bytes = int(
                (num_elements * element_size_bytes)
                * (1 + TENSOR_BUFFER_PADDING_FRACTION)
            )
            buffer_size_bytes = max(buffer_size_bytes, MIN_TENSOR_BUFFER_SIZE)

            return Channel(writer, readers, buffer_size_bytes)


@DeveloperAPI
class _TorchTensorWrapper:
    def __init__(
        self,
        tensor: "torch.Tensor",
        typ: TorchTensorType,
    ):
        import torch

        if not isinstance(tensor, torch.Tensor):
            raise ValueError(
                "DAG nodes wrapped with ray.experimental.TorchTensor must return a "
                "torch.Tensor."
            )
        if tensor.shape != typ.shape:
            raise ValueError(
                "DAG node wrapped with ray.experimental.TorchTensor(shape="
                f"{typ.shape}) returned "
                f"a torch.Tensor of the shape {tensor.shape}"
            )
        if tensor.dtype != typ.dtype:
            raise ValueError(
                "DAG node wrapped with ray.experimental.TorchTensor(dtype="
                f"{typ.dtype}) returned "
                f"a torch.Tensor of the dtype {tensor.dtype}"
            )

        self.tensor = tensor
        self.typ = typ


class _TorchTensorSerializer:
    def __init__(self, device: "torch.device"):
        self.device = device

    @staticmethod
    def serialize_to_numpy(instance: "_TorchTensorWrapper") -> "np.ndarray":
        tensor = instance.tensor
        # Transfer through Ray's shared memory store for now.
        # TODO(swang): This requires two copies, one to transfer from GPU to
        # CPU and another from CPU to shared memory. Ideally we should elide
        # the first copy and memcpy directly from GPU to the shared memory
        # buffer.
        if tensor.device.type == "cuda":
            tensor = tensor.to("cpu")

        return tensor.numpy()

    def deserialize_from_numpy(self, np_array: "np.ndarray"):
        import torch

        # TODO(swang): Support local P2P transfers if available.
        # If there is a GPU assigned to this worker, move it there.
        if self.device.type == "cuda":
            # Use zero-copy from_numpy() because we are going to copy to GPU
            # anyway.
            # TODO: Pin the np_array memory to reduce data movement time.
            # TODO: Set np_array.flags.writeable=True to avoid the PyTorch
            # warning about not owning the underlying memory. This is safe to
            # do as long as all other readers are also copying the data to a
            # GPU.
            cpu_tensor = torch.from_numpy(np_array)
            return cpu_tensor.to(device=self.device)

        # TODO(swang): Use zero-copy from_numpy() if np_array.flags.writeable
        # is True. This is safe to set when deserializing np_array if the
        # upstream task has num_readers=1.
        return torch.tensor(np_array, device=self.device)
