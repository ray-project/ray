from typing import Any, Dict, Tuple

import numpy as np
import torch

import ray
from ray.util.annotations import DeveloperAPI, PublicAPI


@PublicAPI(stability="alpha")
class TorchTensor:
    def __init__(
        self, dag_node: "ray.dag.DAGNode", shape: Tuple[int], dtype: "torch.dtype"
    ):
        self.dag_node = dag_node
        self.tensor_meta = {
            "expected_shape": shape,
            "expected_dtype": dtype,
        }

    def get_dag_node(self):
        return self.dag_node

    def get_tensor_meta(self) -> Dict[str, Any]:
        return self.tensor_meta


@DeveloperAPI
class _TorchTensorWrapper:
    def __init__(
        self,
        tensor: "torch.Tensor",
        expected_shape: Tuple[int],
        expected_dtype: "torch.dtype",
    ):
        if not isinstance(tensor, torch.Tensor):
            raise ValueError(
                "DAG nodes wrapped with ray.experimental.TorchTensor must return a "
                "torch.Tensor."
            )
        if tensor.shape != expected_shape:
            raise ValueError(
                "DAG node wrapped with ray.experimental.TorchTensor(shape="
                f"{expected_shape}) returned "
                f"a torch.Tensor of the shape {tensor.shape}"
            )
        if tensor.dtype != expected_dtype:
            raise ValueError(
                "DAG node wrapped with ray.experimental.TorchTensor(dtype="
                f"{expected_dtype}) returned "
                f"a torch.Tensor of the dtype {tensor.dtype}"
            )

        self.tensor = tensor


@DeveloperAPI
class _TorchTensorSerializer:
    def __init__(self, device: "torch.device"):
        self.device = device

    @staticmethod
    def serialize_to_numpy(instance: "_TorchTensorWrapper") -> np.ndarray:
        tensor = instance.tensor
        # Transfer through Ray's shared memory store for now.
        if tensor.device.type == "cuda":
            tensor = tensor.to("cpu")
        return tensor.numpy()

    def deserialize_from_numpy(self, np_array: np.ndarray):
        # TODO(swang): Use zero-copy from_numpy() if np_array.flags.writeable
        # is True. This is safe if the upstream task has num_readers=1 or if
        # the tensor will anyway be moved to GPU.
        # TODO(swang): If there is a GPU assigned to this worker, move it
        # there. Can also pin the underlying shared memory buffer to reduce
        # data movement time.
        # TODO(swang): Support multinode transfers with NCCL.
        return torch.tensor(np_array, device=self.device)
