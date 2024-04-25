from typing import Tuple, Optional, List

import numpy as np
import torch

import ray.util.serialization
from ray.util.annotations import DeveloperAPI, PublicAPI


class DAGNodeOutputType:
    pass


@DeveloperAPI
def do_register_custom_dag_serializers(self):
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
    def __init__(
        self, shape: Tuple[int], dtype: "torch.dtype", transport: Optional[str] = None
    ):
        self.shape = shape
        self.dtype = dtype
        self.transport = transport


@DeveloperAPI
class _TorchTensorWrapper:
    def __init__(
        self,
        tensor: "torch.Tensor",
        typ: TorchTensorType,
    ):
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


@DeveloperAPI
class _TorchTensorSerializer:
    def __init__(self, device: "torch.device"):
        self.device = device

    @staticmethod
    def serialize_to_numpy(instance: "_TorchTensorWrapper") -> np.ndarray:
        tensor = instance.tensor
        # Transfer through Ray's shared memory store for now.
        # TODO(swang): This requires two copies, one to transfer from GPU to
        # CPU and another from CPU to shared memory. Ideally we should elide
        # the first copy and memcpy directly from GPU to the shared memory
        # buffer.
        if tensor.device.type == "cuda":
            tensor = tensor.to("cpu")

        return tensor.numpy()

    def deserialize_from_numpy(self, np_array: np.ndarray):
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


def do_init_nccl_group(self, world_size, comm_id, rank):
    assert ray.get_gpu_ids()

    from cupy.cuda import nccl

    self._ray_dag_nccl_comm = nccl.NcclCommunicator(world_size, comm_id, rank)


@DeveloperAPI
def do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _init_nccl_group(
    compiled_dag: "ray.dag.compiled_dag_node.CompiledDAG",
    actors: List[ray.actor.ActorHandle],
):
    has_gpus = ray.get(
        [actor.__ray_call__.remote(do_check_has_gpu) for actor in actors]
    )
    for has_gpu, actor in zip(has_gpus, actors):
        if not has_gpu:
            raise ValueError(
                f"Actor {actor} returns a tensor with type hint "
                'TorchTensor(transport="nccl") but actor does not have a '
                "GPU assigned by Ray."
            )

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()
    # TODO(swang): Handle timeout errors.
    ray.get(
        [
            actor.__ray_call__.remote(
                do_init_nccl_group, world_size=len(actors), comm_id=comm_id, rank=rank
            )
            for rank, actor in enumerate(actors)
        ],
        timeout=30,
    )
    # TODO(swang): Destroy the communicator.

    compiled_dag._nccl_group = (
        comm_id,
        {rank: actor for rank, actor in enumerate(actors)},
    )
