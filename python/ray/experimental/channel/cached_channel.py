import uuid
from typing import Any, Optional

from ray.experimental.channel.common import ChannelInterface


class CachedChannel(ChannelInterface):
    """
    CachedChannel wraps an inner channel and caches the data read from it until
    `num_reads` reads have completed. If inner channel is None, the data
    is written to serialization context and retrieved from there. This is useful
    when passing data within the same actor and a shared memory channel can be
    avoided.

    Args:
        num_reads: The number of reads from this channel that must happen before
            writing again. Readers must be methods of the same actor.
        inner_channel: The inner channel to cache data from. If None, the data is
            read from the serialization context.
        _channel_id: The unique ID for the channel. If None, a new ID is generated.
    """

    def __init__(
        self,
        num_reads: int,
        inner_channel: Optional[ChannelInterface] = None,
        _channel_id: Optional[str] = None,
    ):
        assert num_reads > 0, "num_reads must be greater than 0."
        self._num_reads = num_reads
        self._inner_channel = inner_channel
        # Generate a unique ID for the channel. The writer and reader will use
        # this ID to store and retrieve data from the _SerializationContext.
        self._channel_id = _channel_id
        if self._channel_id is None:
            self._channel_id = str(uuid.uuid4())

    def ensure_registered_as_writer(self) -> None:
        if self._inner_channel is not None:
            self._inner_channel.ensure_registered_as_writer()

    def ensure_registered_as_reader(self) -> None:
        if self._inner_channel is not None:
            self._inner_channel.ensure_registered_as_reader()

    def __reduce__(self):
        return CachedChannel, (
            self._num_reads,
            self._inner_channel,
            self._channel_id,
        )

    def __str__(self) -> str:
        return (
            f"CachedChannel(channel_id={self._channel_id}, "
            f"num_reads={self._num_reads}), "
            f"inner_channel={self._inner_channel})"
        )

    def write(self, value: Any, timeout: Optional[float] = None):
        # TODO: beter organize the imports
        from ray.experimental.channel import ChannelContext

        if self._inner_channel is not None:
            self._inner_channel.write(value, timeout)
            return

        # Otherwise no need to check timeout as the operation is non-blocking.

        # Because both the reader and writer are in the same worker process,
        # we can directly store the data in the context instead of storing
        # it in the channel object. This removes the serialization overhead of `value`.
        ctx = ChannelContext.get_current().serialization_context
        ctx.set_data(self._channel_id, value, self._num_reads)

    def read(self, timeout: Optional[float] = None) -> Any:
        # TODO: beter organize the imports
        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current().serialization_context
        if ctx.has_data(self._channel_id):
            # No need to check timeout as the operation is non-blocking.
            return ctx.get_data(self._channel_id)

        assert (
            self._inner_channel is not None
        ), "Cannot read from the serialization context while inner channel is None."
        value = self._inner_channel.read(timeout)
        ctx.set_data(self._channel_id, value, self._num_reads)
        # NOTE: Currently we make a contract with aDAG users that the
        # channel results should not be mutated by the actor methods.
        # When the user needs to modify the channel results, they should
        # make a copy of the channel results and modify the copy.
        # This is the same contract as used in IntraProcessChannel.
        # This contract is NOT enforced right now in either case.
        # TODO(rui): introduce a flag to control the behavior:
        # for example, by default we make a deep copy of the channel
        # result, but the user can turn off the deep copy for performance
        # improvements.
        # https://github.com/ray-project/ray/issues/47409
        return ctx.get_data(self._channel_id)

    def close(self) -> None:
        from ray.experimental.channel import ChannelContext

        if self._inner_channel is not None:
            self._inner_channel.close()
        ctx = ChannelContext.get_current().serialization_context
        ctx.reset_data(self._channel_id)
