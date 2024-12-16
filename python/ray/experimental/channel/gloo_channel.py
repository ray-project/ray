import logging
import pickle
from typing import Any, List, Optional

import numpy as np

import ray
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class GlooChannel(ChannelInterface):
    def __init__(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        readers: List[Optional[ray.actor.ActorHandle]],
        group_name: str,
    ):
        """
        Create a Gloo channel to transfer data between actors.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
            group_name: The name of the Gloo group to use for the channel.
        """
        self._writer = writer
        self._readers = readers
        self._group_name = group_name

        ctx = ChannelContext.get_current()
        self._gloo_group = ctx.gloo_groups[self._group_name]
        # TODO: currently, we hardcode a big buffer size for the recv buffer.
        # 100000000 np.uint8 ~= 100MB
        self._shape = (100000000,)
        self._metadata_bytes = 5

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        if not hasattr(self, "_recv_buffer") or self._recv_buffer is None:
            self._recv_buffer = np.empty(self._shape, dtype=np.uint8)

    def __reduce__(self):
        return GlooChannel, (
            self._writer,
            self._readers,
            self._group_name,
        )

    def write(self, value: Any, timeout: Optional[float] = None):
        serialized_value = pickle.dumps(value)
        num_bytes = len(serialized_value)
        size_bytes = num_bytes.to_bytes(self._metadata_bytes, byteorder="big")
        combined_data = size_bytes + serialized_value
        np_data = np.frombuffer(combined_data, dtype=np.uint8)

        for reader in self._readers:
            reader_rank = self._gloo_group.get_rank(reader)
            self._gloo_group.send(np_data, reader_rank)

    def read(self, timeout: Optional[float] = None, deserialize: bool = True) -> Any:
        self.ensure_registered_as_reader()
        writer_rank = self._gloo_group.get_rank(self._writer)
        np_data = self._gloo_group.recv(self._recv_buffer, writer_rank)
        num_bytes = int.from_bytes(np_data[: self._metadata_bytes], byteorder="big")
        data = np_data[self._metadata_bytes : self._metadata_bytes + num_bytes]
        return pickle.loads(data)

    def close(self) -> None:
        # TODO (kevin85421): can't teardown successfully
        self._gloo_group.destroy()
        ctx = ChannelContext.get_current()
        del ctx.gloo_groups[self._group_name]


def _do_destroy_gloo_group(self, group_id):
    # TODO (kevin85421): can't teardown successfully
    ctx = ChannelContext.get_current()
    if group_id not in ctx.gloo_groups:
        return
    ctx.gloo_groups[group_id].destroy()


def _destroy_gloo_group(group_id: str) -> None:
    """
    Destroy the Gloo group with the given ID.
    """
    # TODO (kevin85421): can't teardown successfully
    ctx = ChannelContext.get_current()
    if group_id not in ctx.gloo_groups:
        return

    group = ctx.gloo_groups[group_id]
    actors = group._actor_handles
    destroy_tasks = [
        actor.__ray_call__.remote(
            _do_destroy_gloo_group,
            group_id,
        )
        for actor in actors
    ]

    _, unready = ray.wait(destroy_tasks, timeout=30, num_returns=len(destroy_tasks))
    if unready:
        logger.warning(
            "Gloo group destruction not done after 30s. Gloo group destruction "
            "may be hung."
        )

    del ctx.gloo_groups[group_id]


@PublicAPI(stability="alpha")
class GlooNumpyChannel(ChannelInterface):
    def __init__(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        readers: List[Optional[ray.actor.ActorHandle]],
        group_name: str,
        typ: "TorchTensorType",
    ):
        """
        Create a Gloo channel to transfer data between actors.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
            group_name: The name of the Gloo group to use for the channel.
            typ: The type of the data to be sent through the channel.
        """
        self._writer = writer
        self._readers = readers
        self._group_name = group_name
        self._typ = typ

        ctx = ChannelContext.get_current()
        self._gloo_group = ctx.gloo_groups[self._group_name]
        self._shape = None
        self._meta_channel = GlooChannel(self._writer, self._readers, self._group_name)
        self._recv_buf = None

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        pass

    def __reduce__(self):
        return GlooNumpyChannel, (
            self._writer,
            self._readers,
            self._group_name,
            self._typ,
        )

    def write(self, value: Any, timeout: Optional[float] = None):
        assert isinstance(value, np.ndarray)
        shape = value.shape
        need_to_send_metadata = self._shape is None
        if self._typ.static_shape and self._shape is None:
            self._shape = value.shape
        assert shape is not None

        # Send the metadata if needed.
        if need_to_send_metadata:
            self._meta_channel.write(shape, timeout=timeout)

        for reader in self._readers:
            reader_rank = self._gloo_group.get_rank(reader)
            self._gloo_group.send(value, reader_rank)

    def read(self, timeout: Optional[float] = None, deserialize: bool = True) -> Any:
        # If self._shape is None, then we need to read the shape from the
        # metadata channel. If the shape is static, we only need to read it
        # once.
        shape = self._shape
        if shape is None:
            shape = self._meta_channel.read(timeout=timeout)
        if self._typ.static_shape:
            self._shape = shape

        # Allocate the recv buffer if needed.
        recv_buf = self._recv_buf
        if recv_buf is None:
            if self._typ.static_shape:
                self._recv_buf = np.empty(self._shape, dtype=np.float32)
                recv_buf = self._recv_buf
            else:
                recv_buf = np.empty(shape, dtype=np.float32)
        # Read the data from the writer.
        writer_rank = self._gloo_group.get_rank(self._writer)
        data = self._gloo_group.recv(recv_buf, writer_rank)
        # TODO (kevin85421): Currently, this PoC assumes that the data is a
        # float32 numpy array.
        assert data.dtype == np.float32
        return data

    def close(self) -> None:
        # TODO (kevin85421): can't teardown successfully
        self._gloo_group.destroy()
        ctx = ChannelContext.get_current()
        del ctx.gloo_groups[self._group_name]
