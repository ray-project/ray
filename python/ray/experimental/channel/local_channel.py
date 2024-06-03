import uuid
from typing import Any

import ray
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class IntraProcessChannel(ChannelInterface):
    def __init__(
        self,
        actor_handle: ray.actor.ActorHandle,
    ):
        """
        IntraProcessChannel is a channel for communication between two tasks in the same
        worker process. It writes data directly to the worker's serialization context
        and reads data from the serialization context to avoid the serialization
        overhead and the need for reading/writing from shared memory.
        """

        # TODO (kevin85421): Currently, if we don't pass `actor_handle` to
        # `IntraProcessChannel`, the actor will die due to the reference count of
        # `actor_handle` is 0. We should fix this issue in the future.
        self._actor_handle = actor_handle
        # Generate a unique ID for the channel. The writer and reader will use
        # this ID to store and retrieve data from the serialization context.
        self.channel_id = str(uuid.uuid4())

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        pass

    def __reduce__(self):
        return IntraProcessChannel, (self._actor_handle,)

    def write(self, value: Any):
        # Because both the reader and writer are in the same worker process,
        # we can directly store the data in the context instead of storing
        # it in the channel object. This reduces the serialization overhead of `value`.
        ctx = ChannelContext.get_current().serialization_context
        ctx.set_data(self.channel_id, value)

    def begin_read(self) -> Any:
        ctx = ChannelContext.get_current().serialization_context
        return ctx.get_data(self.channel_id)

    def end_read(self):
        pass

    def close(self) -> None:
        ctx = ChannelContext.get_current().serialization_context
        ctx.reset_data(self.channel_id)
