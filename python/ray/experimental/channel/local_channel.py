from typing import Any

import ray
from ray.experimental.channel.common import ChannelInterface
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LocalChannel(ChannelInterface):
    def __init__(
        self,
        actor_handle: ray.actor.ActorHandle,
    ):
        # TODO (kevin85421): Currently, if we don't pass `actor_handle` to
        # `LocalChannel`, the actor will die due to the reference count of
        # `actor_handle` is 0. We should fix this issue in the future.
        self._actor_handle = actor_handle

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        pass

    def __reduce__(self):
        return LocalChannel, (self._actor_handle,)

    def write(self, value: Any):
        self.data = value

    def begin_read(self) -> Any:
        return self.data

    def end_read(self):
        pass

    def close(self) -> None:
        pass
