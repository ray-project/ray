import logging
from types import ModuleType
from typing import TYPE_CHECKING, List, Optional

import ray
import ray.util.serialization
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch

    from ray.experimental.channel.torch_tensor_type import (
        TorchTensorType,
        _TorchTensorWrapper,
    )


# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


@DeveloperAPI
class TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        readers: List[ray.actor.ActorHandle],
        typ: "TorchTensorType",
        device: Optional["torch.device"] = None,
    ):
        import torch

        from ray.air._internal import torch_utils
        from ray.experimental.channel.torch_tensor_type import TorchTensorType

        self.torch: ModuleType = torch

        self._nccl_group = None

        self._writer = writer
        self._writer_rank: Optional[int] = None
        self._readers = readers
        self._reader_ranks: Optional[List[int]] = None
        self._writer_registered: bool = False
        self._reader_registered: bool = False

        self._device = device
        if self._device is None:
            # TODO(swang): Allow default device to be overridden.
            self._device = torch_utils.get_devices()[0]
        if self._device.type != "cuda":
            raise ValueError(
                f'Actor\'s default device has type "{self._device.type}", need "cuda"'
            )

        assert isinstance(typ, TorchTensorType)
        assert typ.transport == typ.NCCL
        self._typ = typ

        ctx = ChannelContext.get_current()
        self._nccl_group = ctx.nccl_group
        assert (
            self._nccl_group is not None
        ), "ChannelContext.nccl_group is not initialized."

        self._writer_rank = self._nccl_group.get_rank(self._writer)
        self._reader_ranks = [
            self._nccl_group.get_rank(reader) for reader in self._readers
        ]

        if (
            self._writer_rank is not None
            and self._writer_rank == self._nccl_group.get_self_rank()
        ):
            self._writer_registered = True

        if (
            self._reader_ranks
            and self._nccl_group.get_self_rank() in self._reader_ranks
        ):
            self._reader_registered = True

    def ensure_registered_as_writer(self):
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"

    def ensure_registered_as_reader(self) -> bool:
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"

    def __reduce__(self):
        return (self.__class__, (self._writer, self._readers, self._typ, self._device))

    def write(
        self,
        wrapped: "_TorchTensorWrapper",
        readers: Optional[List[ray.actor.ActorHandle]] = None,
    ):
        value = wrapped.tensor
        if value.shape != self._typ.shape:
            raise ValueError(
                f"torch.Tensor has shape {value.shape}, expected {self._typ.shape}"
            )
        if value.dtype != self._typ.dtype:
            raise ValueError(
                f"torch.Tensor has dtype {value.dtype}, expected {self._typ.dtype}"
            )
        if value.device != self._device:
            raise ValueError(
                f"torch.Tensor must be on the default device: {self._device}"
            )

        if readers is not None:
            raise NotImplementedError(
                "TorchTensorNcclChannel.write() not supported for dynamically "
                "passed readers."
            )

        # TODO: If there are multiple readers, can replace with a broadcast.
        for rank in self._reader_ranks:
            self._nccl_group.send(value, rank)

    def begin_read(self) -> "torch.Tensor":
        buf = self.torch.zeros(
            self._typ.shape, dtype=self._typ.dtype, device=self._device
        )
        self._nccl_group.recv(buf, self._writer_rank)
        return buf

    def end_read(self) -> None:
        return

    def close(self) -> None:
        self._nccl_group.destroy()


def _do_init_nccl_group(self, world_size, comm_id, rank, actor_ids_to_ranks):
    import torch

    assert (
        ray.get_gpu_ids()
    ), "Actors participating in NCCL group must have at least one GPU assigned"

    ctx = ChannelContext.get_current()
    ctx.nccl_group = _NcclGroup(
        world_size,
        comm_id,
        rank,
        actor_ids_to_ranks,
        torch.cuda.current_stream().cuda_stream,
    )


def _do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _do_get_unique_nccl_id(self) -> bool:
    from cupy.cuda import nccl

    return nccl.get_unique_id()


def _init_nccl_group(
    actors: List[ray.actor.ActorHandle],
):
    ctx = ChannelContext.get_current()
    if ctx.nccl_group is not None:
        return

    has_gpus = ray.get(
        [actor.__ray_call__.remote(_do_check_has_gpu) for actor in actors]
    )
    for has_gpu, actor in zip(has_gpus, actors):
        if not has_gpu:
            raise ValueError(
                f"Actor {actor} returns a tensor with type hint "
                'TorchTensor(transport="nccl") but actor does not have a '
                "GPU assigned by Ray."
            )

    actor_handles_to_ranks = {actor: rank for rank, actor in enumerate(actors)}
    actor_ids_to_ranks = {
        actor._ray_actor_id: rank for actor, rank in actor_handles_to_ranks.items()
    }
    assert len(actor_ids_to_ranks) == len(
        actor_handles_to_ranks
    ), "actors must be unique"

    # Allocate a communicator ID on one of the actors that will participate in
    # the group. This is in case the driver is not on the same node as one of
    # the NCCL actors.
    comm_id = ray.get(actors[0].__ray_call__.remote(_do_get_unique_nccl_id))

    logger.info(f"Creating NCCL group on actors->ranks: {actor_handles_to_ranks}")

    world_size = len(actors)
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_nccl_group,
            world_size,
            comm_id,
            rank,
            actor_ids_to_ranks,
        )
        for actor, rank in actor_handles_to_ranks.items()
    ]
    try:
        ray.get(init_tasks, timeout=30)
    except ray.exceptions.GetTimeoutError:
        logger.warning(
            "NCCL group creation not done after 30s. NCCL group creation may be hung."
        )
        ray.get(init_tasks)

    logger.info("NCCL group created.")

    ctx.nccl_group = _NcclGroup(
        world_size,
        comm_id,
        rank=None,
        actor_ids_to_ranks=actor_ids_to_ranks,
        cuda_stream=None,
    )
