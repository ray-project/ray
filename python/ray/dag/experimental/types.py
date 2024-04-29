from typing import Tuple, Optional, List, Dict
import torch
import uuid

import numpy as np

import ray.util.serialization
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.collective.collective_group import nccl_util
from ray.experimental.channel import (
        Channel,
        )


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


class ChannelContext:
    def __init__(self):
        self.nccl_group : Optional["NcclGroup"] = None

def _get_or_create_ray_dag_context(self):
    if not hasattr(self, "_ray_dag_context"):
        self._ray_dag_context = ChannelContext()
    return self._ray_dag_context


class NcclGroup:
    def __init__(self,
            world_size: int,
            comm_id: int,
            rank: Optional[int],
            actor_ids_to_ranks: Dict[ray.ActorID, int],
            cuda_stream: Optional[int],
            ):
        self._rank : Optional[int] = rank
        if rank is not None:
            from cupy.cuda import nccl

            self._comm = nccl.NcclCommunicator(world_size, comm_id, rank)
        else:
            # Driver does not have a rank.
            self._comm = None
        self._actor_ids_to_ranks = actor_ids_to_ranks

        self._cuda_stream = cuda_stream

        self._closed = False

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        return self._actor_ids_to_ranks[actor._ray_actor_id]

    def get_self_rank(self) -> int:
        return self._rank

    def send(self, value: torch.Tensor, peer_rank: int):
        if self._closed:
            raise IOError("NCCL group has already been destroyed.")
        self._comm.send(
                nccl_util.get_tensor_ptr(value),
                value.numel(),
                nccl_util.get_nccl_tensor_dtype(value),
                peer_rank,
                self._cuda_stream,
                )


    def recv(self, buf: torch.Tensor, peer_rank: int):
        if self._closed:
            raise IOError("NCCL group has already been destroyed.")
        self._comm.recv(
                nccl_util.get_tensor_ptr(buf),
                buf.numel(),
                nccl_util.get_nccl_tensor_dtype(buf),
                peer_rank,
                self._cuda_stream,
                )

    def destroy(self):
        if self._closed:
            return

        self._closed = True
        self._comm.abort()
        self._comm.destroy()


@DeveloperAPI
class TorchTensorNcclChannel(Channel):
    def __init__(self,
            writer: ray.actor.ActorHandle,
            readers: List[ray.actor.ActorHandle],
            typ: TorchTensorType,
            device: Optional["torch.device"] = None,
            ):
        from ray.air._internal import torch_utils

        self._nccl_group = None

        self._writer = writer
        self._writer_rank : Optional[int] = None
        self._readers = readers
        self._reader_ranks : Optional[List[int]] = None

        self._device = device
        if self._device is None:
            # TODO(swang): Allow default device to be overridden.
            self._device = torch_utils.get_devices()[0]
        if self._device.type != "cuda":
            raise ValueError(f"Actor's default device has type \"{self._device.type}\", need \"cuda\"")

        assert typ.transport == "nccl"
        self._typ = typ

    def _init(self, channel_context: "ChannelContext"):
        if self._nccl_group is not None:
            return
        self._nccl_group = channel_context.nccl_group
        assert self._nccl_group is not None, "Actor is not part of a NcclGroup"
        self._writer_rank = self._nccl_group.get_rank(self._writer)
        self._reader_ranks = [self._nccl_group.get_rank(reader) for reader in self._readers]

        self._writer_registered = (self._nccl_group.get_self_rank() == self._writer_rank)
        self._reader_registered = (self._nccl_group.get_self_rank() in self._reader_ranks)

    def __reduce__(self):
        return (self.__class__, (self._writer, self._readers, self._typ, self._device))

    def write(self, wrapped: _TorchTensorWrapper, readers: Optional[List[ray.actor.ActorHandle]] = None):
        value = wrapped.tensor
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
            self._nccl_group.send(value, rank)

    def begin_read(self) -> torch.Tensor:
        # TODO(swang): Perform the NCCL recv. Pass in the source actor.
        buf = torch.zeros(self._typ.shape, dtype=self._typ.dtype, device=self._device)
        self._nccl_group.recv(buf, self._writer_rank)
        return buf

    def end_read(self) -> None:
        return

    def close(self) -> None:
        self._nccl_group.destroy()

        # TODO(swang): Set flag for downstream kernels that have already been
        # launched to the GPU to abort.


def do_init_nccl_group(self, world_size, comm_id, rank, actor_ids_to_ranks):
    assert ray.get_gpu_ids()

    ctx = _get_or_create_ray_dag_context(self)
    ctx.nccl_group = NcclGroup(
            world_size,
            comm_id,
            rank,
            actor_ids_to_ranks,
            torch.cuda.current_stream().cuda_stream,
            )


@DeveloperAPI
def do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _init_nccl_group(
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
    actor_ids_to_ranks = {
            actor._ray_actor_id: rank for actor, rank in actor_handles_to_ranks.items()}
    assert len(actor_ids_to_ranks) == len(actor_handles_to_ranks), "actors must be unique"

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
                actor_ids_to_ranks,
            )
            for actor, rank in actor_handles_to_ranks.items()
        ],
        timeout=30,
    )
