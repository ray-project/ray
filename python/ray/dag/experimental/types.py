from typing import (
    TYPE_CHECKING,
    Tuple,
    Any,
)

import ray.util.serialization
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import torch
    import numpy as np


class DAGNodeOutputType:
    pass


def _do_register_custom_dag_serializers(self: Any) -> None:
    # Helper method to run on the DAG driver and actors to register custom
    # serializers.
    from ray.air._internal import torch_utils

    default_device = torch_utils.get_devices()[0]
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

    self._torch_tensor_serializer = torch_tensor_serializer


@PublicAPI(stability="alpha")
class TorchTensorType(DAGNodeOutputType):
    def __init__(self, shape: Tuple[int], dtype: "torch.dtype"):
        self.shape = shape
        self.dtype = dtype


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
        # TODO(swang): Support multinode transfers with NCCL.

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
