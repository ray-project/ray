from typing import Tuple, Optional, List, Dict
import torch

import numpy as np

import ray.util.serialization
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.collective.collective_group import nccl_util


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
        self.typ = typ


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


class NcclGroup:
    def __init__(self,
            world_size: int,
            comm_id: int,
            rank: Optional[int],
            actor_handles_to_ranks: Dict[ray.actor.ActorHandle, int],
            cuda_stream: Optional[int],
            ):
        self._rank : Optional[int] = rank
        if rank is not None:
            from cupy.cuda import nccl

            self._comm = nccl.NcclCommunicator(world_size, comm_id, rank)
        else:
            # Driver does not have a rank.
            self._comm = None
        self._actor_handles_to_ranks = actor_handles_to_ranks

        self._cuda_stream = cuda_stream

        # TODO(swang): Serialize and pass a handle to the reader actor.

    def get_peer_rank(self, actor: ray.actor.ActorHandle) -> int:
        return self._actor_handles_to_ranks[actor]

    def get_self_rank(self, actor: ray.actor.ActorHandle) -> int:
        return self._rank

    def send(self, value: torch.Tensor, peer_rank: int):
        self._comm.send(
                nccl_util.get_tensor_ptr(value),
                value.numel(),
                nccl_util.get_nccl_tensor_dtype(value),
                peer_rank,
                self._cuda_stream,
                )


    def recv(self, buf: torch.Tensor, peer_rank: int):
        self._comm.recv(
                nccl_util.get_tensor_ptr(buf),
                buf.numel(),
                nccl_util.get_nccl_tensor_dtype(buf),
                rank,
                self._cuda_stream,
                )


@DeveloperAPI
class TorchTensorNcclChannel:
    def __init__(self,
            readers: List[ray.actor.ActorHandle],
            nccl_group: NcclGroup,
            device: "torch.device",
            typ: TorchTensorType,
            ):
        self._writer_rank = self._nccl_group.get_rank()
        self._reader_ranks : List[int] = [nccl_group.get_peer_rank(reader) for reader in readers]
        self._nccl_group = nccl_group

        if self._device.type != "cuda":
            raise ValueError(f"Actor's default device has type \"{self.device.type}\", need \"cuda\"")
        self._device = device

        assert typ.transport == "nccl"
        self._typ = typ

    def write(self, value: torch.Tensor, readers: Optional[List[ray.actor.ActorHandle]] = None):
        if value.shape != self._typ.shape:
            raise ValueError(f"torch.Tensor has shape {value.shape}, expected {self._typ.shape}")
        if value.dtype != self._typ.dtype:
            raise ValueError(f"torch.Tensor has shape {value.shape}, expected {self._typ.shape}")
        if value.device != self._device:
            raise ValueError(f"torch.Tensor must be on the default device: {self._device}")

        if readers is not None:
            raise NotImplementedError("TorchTensorNcclChannel.write() not supported for dynamically passed readers.")

        # TODO: If there are multiple readers, can replace with a broadcast.
        for rank in self._reader_ranks:
            self.comm.send(
                    nccl_util.get_tensor_ptr(value),
                    buf.numel(),
                    nccl_util.get_nccl_tensor_dtype(value),
                    rank,
                    self._torch_stream_ptr,
                    )

    @staticmethod
    def begin_read(self) -> torch.Tensor:
        # TODO(swang): Perform the NCCL recv. Pass in the source actor.
        buf = torch.zeros(self._typ.shape, dtype=self._typ.dtype, device=self._device)
        self._nccl_group.recv(buf, self._writer_rank)

    def end_read(self) -> None:
        return


def do_init_nccl_group(self, world_size, comm_id, rank, actor_handles_to_ranks):
    assert ray.get_gpu_ids()

    self._ray_dag_nccl_group = NcclGroup(
            world_size,
            comm_id,
            rank,
            actor_handles_to_ranks,
            torch.cuda.current_stream().cuda_stream,
            )


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

    actor_handles_to_ranks = {
            actor: rank for rank, actor in enumerate(actors)}

    from cupy.cuda import nccl
    comm_id = nccl.get_unique_id()

    # TODO(swang): Handle timeout errors.
    world_size = len(actors)
    ray.get(
        [
            actor.__ray_call__.remote(
                do_init_nccl_group,
                world_size,
                comm_id,
                rank,
                actor_handles_to_ranks,
            )
            for actor, rank in actor_handles_to_ranks.items()
        ],
        timeout=30,
    )

    # TODO(swang): Destroy the communicator.

    compiled_dag._ray_dag_nccl_group = NcclGroup(
            world_size,
            comm_id,
            rank=None,
            actor_handles_to_ranks=actor_handles_to_ranks,
            cuda_stream=None)
