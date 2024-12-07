from typing import Any, Optional, List, Tuple

from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.util.annotations import PublicAPI

import ray
import numpy as np


@PublicAPI(stability="alpha")
class GlooChannel(ChannelInterface):    
    def __init__(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
    ):
        self._writer = writer
        self._reader_and_node_list = reader_and_node_list

        ctx = ChannelContext.get_current()
        self._gloo_group = ctx.gloo_groups["default_gloo_group"]
        self._shape = None

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        pass

    def __reduce__(self):
        return GlooChannel, (self._writer, self._reader_and_node_list)

    def write(self, value: Any, timeout: Optional[float] = None):
        print("GlooChannel.write", value)
        reader_rank = 1
        self._gloo_group.send(value, reader_rank)

    def read(self, timeout: Optional[float] = None, deserialize: bool = True) -> Any:
        writer_rank = 0
        # TODO: modify the shape
        data = self._gloo_group.recv((100,), np.float32, writer_rank)
        return data

    def close(self) -> None:
        self._gloo_group.destroy()
