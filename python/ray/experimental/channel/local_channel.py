import ray
from typing import Any, List, Optional
from ray.experimental.channel.common import ChannelInterface
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LocalChannel(ChannelInterface):
    def __init__(
        self,
        readers: List[Optional[ray.actor.ActorHandle]],
    ):
        self._readers = readers

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        pass

    def __reduce__(self):
        return LocalChannel, (
            self._readers,
        )

    def write(self, value: Any):
        self.data = value

    def begin_read(self) -> Any:
        return self.data

    def end_read(self):
        pass

    def close(self) -> None:
        pass
