import logging
import uuid
from types import ModuleType
from typing import TYPE_CHECKING, List, Optional

import ray
import ray.util.serialization
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.shared_memory_channel import Channel, SharedMemoryType
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


class NestedTorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        readers: List[ray.actor.ActorHandle],
        metadata_typ: "SharedMemoryType",
        gpu_data_typ: "TorchTensorType",
        data_typ: Optional["SharedMemoryType"] = None,
        _meta_channel: Optional["Channel"] = None,
        _gpu_data_channel: Optional["TorchTensorNcclChannel"] = None,
        _cpu_data_channel: Optional["Channel"] = None,
    ):
        self._writer = writer
        self._readers = readers

        if _meta_channel is not None:
            self._meta_channel = _meta_channel
            self._gpu_data_channel = _gpu_data_channel
            self._cpu_data_channel = _cpu_data_channel
        else:
            self._meta_channel: Channel = metadata_typ.create_channel(writer, readers)
            self._cpu_data_channel: Optional[Channel] = None
            self._gpu_data_channel: TorchTensorNcclChannel = gpu_data_typ.create_channel(writer, readers)
            if data_typ is not None:
                self._cpu_data_channel = data_typ.create_channel(writer, readers)

    @classmethod
    def from_channels(cls,
        meta_channel: "Channel",
        gpu_data_channel: "TorchTensorNcclChannel",
        cpu_data_channel: Optional["Channel"],
        ):
        return cls(
                writer=None,
                readers=None,
                metadata_typ=None,
                gpu_data_typ=None,
                data_typ=None,
                _meta_channel=meta_channel,
                _gpu_data_channel=gpu_data_channel,
                _cpu_data_channel=cpu_data_channel,
                )

    def __reduce__(self):
        return (
            NestedTorchTensorNcclChannel.from_channels,
            (self._meta_channel, self._gpu_data_channel, self._cpu_data_channel),
        )

    def ensure_registered_as_writer(self):
        self._meta_channel.ensure_registered_as_writer()
        self._gpu_data_channel.ensure_registered_as_writer()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.ensure_registered_as_writer()

    def ensure_registered_as_reader(self):
        self._meta_channel.ensure_registered_as_reader()
        self._gpu_data_channel.ensure_registered_as_reader()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.ensure_registered_as_reader()

    def write(self):
        # TODO
        pass

    def begin_read(self) -> Any:
        # TODO
        pass

    def end_read(self) -> None:
        # TODO
        pass

    def close(self) -> None:
        self._meta_channel.close()
        self._gpu_data_channel.close()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.close()


@DeveloperAPI
class TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        readers: List[ray.actor.ActorHandle],
        typ: "TorchTensorType",
        _meta_channel: Optional["Channel"] = None,
    ):
        import torch

        from ray.air._internal import torch_utils
        from ray.experimental.channel.torch_tensor_type import (
                _get_default_torch_device,
                TorchTensorType,
                )

        self.torch: ModuleType = torch

        self._writer = writer
        self._writer_rank: Optional[int] = None
        self._readers = readers
        self._reader_ranks: Optional[List[int]] = None
        self._writer_registered: bool = False
        self._reader_registered: bool = False

        # TODO(swang): Allow default device to be overridden.
        self._device = _get_default_torch_device()
        if self._device.type != "cuda":
            raise ValueError(
                f'Actor\'s default device has type "{self._device.type}", need "cuda"'
            )

        assert isinstance(typ, TorchTensorType)
        assert typ.transport == typ.NCCL
        self._typ = typ
        self._typ: "TorchTensorType" = typ

        ctx = ChannelContext.get_current()
        assert self._typ.transport_group_id is not None, "No NCCL group specified."
        self._nccl_group_id: str = self._typ.transport_group_id
        self._nccl_group: "_NcclGroup" = ctx.nccl_groups[self._typ.transport_group_id]
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

        self._meta_channel: Optional[Channel] = _meta_channel
        if (
            self._meta_channel is None
            and self._writer_registered
            and (
                self._typ.shape == TorchTensorType.AUTO
                or self._typ.dtype == TorchTensorType.AUTO
            )
        ):
            # Allocate 1KiB for metadata.
            metadata_type = SharedMemoryType(buffer_size_bytes=1_000)
            self._meta_channel = metadata_type.create_channel(
                self._writer,
                self._readers,
            )

    def ensure_registered_as_writer(self):
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"

    def ensure_registered_as_reader(self) -> bool:
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"

    def __reduce__(self):
        return (
            self.__class__,
            (self._writer, self._readers, self._typ, self._device, self._meta_channel),
        )

    def write(
        self,
        wrapped: "_TorchTensorWrapper",
    ):
        value = wrapped.tensor
        if value.device != self._device:
            raise ValueError(
                f"torch.Tensor must be on the default device: {self._device}"
            )

        if self._meta_channel is not None:
            meta = (value.shape, value.dtype)
            self._meta_channel.write(meta)
        else:
            if value.shape != self._typ.shape:
                raise ValueError(
                    f"torch.Tensor has shape {value.shape}, expected {self._typ.shape}"
                )
            if value.dtype != self._typ.dtype:
                raise ValueError(
                    f"torch.Tensor has dtype {value.dtype}, expected {self._typ.dtype}"
                )

        # TODO: If there are multiple readers, can replace with a broadcast.
        for rank in self._reader_ranks:
            self._nccl_group.send(value, rank)

    def begin_read(self) -> "torch.Tensor":
        if self._meta_channel is not None:
            shape, dtype = self._meta_channel.begin_read()
            # It's safe to release the channel because shape and dtype should get
            # copied during deserialization.
            self._meta_channel.end_read()
        else:
            shape, dtype = self._typ.shape, self._typ.dtype

        buf = self.torch.zeros(shape, dtype=dtype, device=self._device)
        self._nccl_group.recv(buf, self._writer_rank)
        return buf

    def end_read(self) -> None:
        return

    def close(self) -> None:
        if self._meta_channel is not None:
            self._meta_channel.close()

        self._nccl_group.destroy()
        ctx = ChannelContext.get_current()
        if self._nccl_group_id in ctx.nccl_groups:
            del ctx.nccl_groups[self._nccl_group_id]


def _do_init_nccl_group(self, group_id, world_size, comm_id, rank, actor_handles):
    import torch

    assert (
        ray.get_gpu_ids()
    ), "Actors participating in NCCL group must have at least one GPU assigned"

    ctx = ChannelContext.get_current()
    ctx.nccl_groups[group_id] = _NcclGroup(
        world_size,
        comm_id,
        rank,
        actor_handles,
        torch.cuda.current_stream().cuda_stream,
    )


def _do_destroy_nccl_group(self, group_id):
    ctx = ChannelContext.get_current()
    if group_id not in ctx.nccl_groups:
        return

    ctx.nccl_groups[group_id].destroy()
    del ctx.nccl_groups[group_id]


def _do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _do_get_unique_nccl_id(self) -> bool:
    from cupy.cuda import nccl

    return nccl.get_unique_id()


def _init_nccl_group(
    actors: List[ray.actor.ActorHandle],
) -> str:
    ctx = ChannelContext.get_current()

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

    actor_ids = set(actor._ray_actor_id for actor in actors)
    assert len(actor_ids) == len(actors), "Actors must be unique"

    # Allocate a communicator ID on one of the actors that will participate in
    # the group. This is in case the driver is not on the same node as one of
    # the NCCL actors.
    nccl_comm_id = ray.get(actors[0].__ray_call__.remote(_do_get_unique_nccl_id))
    # Used to uniquely identify this NCCL group.
    group_id = str(uuid.uuid4())

    logger.info(f"Creating NCCL group {group_id} on actors: {actors}")

    world_size = len(actors)
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_nccl_group,
            group_id,
            world_size,
            nccl_comm_id,
            rank,
            actors,
        )
        for rank, actor in enumerate(actors)
    ]
    try:
        ray.get(init_tasks, timeout=30)
    except ray.exceptions.GetTimeoutError:
        logger.warning(
            "NCCL group creation not done after 30s. NCCL group creation may be hung."
        )
        ray.get(init_tasks)

    logger.info("NCCL group created.")

    ctx.nccl_groups[group_id] = _NcclGroup(
        world_size,
        nccl_comm_id,
        rank=None,
        actor_handles=actors,
        cuda_stream=None,
    )
    return group_id


def _destroy_nccl_group(group_id: str) -> None:
    ctx = ChannelContext.get_current()
    group = ctx.nccl_groups[group_id]
    actors = group._get_actor_handles()
    destroy_tasks = [
        actor.__ray_call__.remote(
            _do_destroy_nccl_group,
            group_id,
        )
        for actor in actors
    ]

    _, unready = ray.wait(destroy_tasks, timeout=30, num_returns=len(destroy_tasks))
    if unready:
        logger.warning(
            "NCCL group destruction not done after 30s. NCCL group destruction "
            "may be hung."
        )

    del ctx.nccl_groups[group_id]
