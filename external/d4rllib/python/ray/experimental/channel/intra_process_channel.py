import uuid
from typing import Any, Optional

from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class IntraProcessChannel(ChannelInterface):
    """
    IntraProcessChannel is a channel for communication between two tasks in the same
    worker process. It writes data directly to the worker's _SerializationContext
    and reads data from the _SerializationContext to avoid the serialization
    overhead and the need for reading/writing from shared memory. Note that if the
    readers may mutate the data, users should deep copy the data themselves to avoid
    side effects.

    Args:
        num_readers: The number of readers that will read from this channel. Readers
            can be the same method of the same actor.
    """

    def __init__(
        self,
        num_readers,
        _channel_id: Optional[str] = None,
    ):
        # Generate a unique ID for the channel. The writer and reader will use
        # this ID to store and retrieve data from the _SerializationContext.
        self._channel_id = _channel_id
        self._num_readers = num_readers
        if self._channel_id is None:
            self._channel_id = str(uuid.uuid4())

    def ensure_registered_as_writer(self) -> None:
        pass

    def ensure_registered_as_reader(self) -> None:
        pass

    def __reduce__(self):
        return IntraProcessChannel, (
            self._num_readers,
            self._channel_id,
        )

    def __str__(self) -> str:
        return f"IntraProcessChannel(channel_id={self._channel_id})"

    def write(self, value: Any, timeout: Optional[float] = None):
        self.ensure_registered_as_writer()
        # No need to check timeout as the operation is non-blocking.

        # Because both the reader and writer are in the same worker process,
        # we can directly store the data in the context instead of storing
        # it in the channel object. This removes the serialization overhead of `value`.
        ctx = ChannelContext.get_current().serialization_context
        ctx.set_data(self._channel_id, value, self._num_readers)

    def read(self, timeout: Optional[float] = None, deserialize: bool = True) -> Any:
        self.ensure_registered_as_reader()
        assert deserialize, "Data passed from the actor to itself is never serialized"
        # No need to check timeout as the operation is non-blocking.
        ctx = ChannelContext.get_current().serialization_context
        return ctx.get_data(self._channel_id)

    def close(self) -> None:
        ctx = ChannelContext.get_current().serialization_context
        ctx.reset_data(self._channel_id)
