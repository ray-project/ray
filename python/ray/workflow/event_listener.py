import asyncio
from ray.util.annotations import PublicAPI
from ray.workflow.common import Event
import time
from typing import Callable

EventListenerType = Callable[[], "EventListener"]


@PublicAPI(stability="alpha")
class EventListener:
    """Defining a custom event listener. Event listeners provide an efficient way
    to listen for a custom event.

    Event listeners should be stateless. They will be instantiated from a
    coordinator actor.

    Example definition
    ==================

    ```
    class CustomEventListener:

        def __init__(self):
            self.event_provider = ...

        async def poll_for_event(self, topic, partition):
            return await self.event_provider.poll(topic, partition)

        async def event_checkpointed(self, event: Event):
            self.event_provider.commit(event.offset)
    ```

    Example Usage
    =============
    >>> from ray import workflow
    >>> CustomEventListener = ... # doctest: +SKIP
    >>> event_task = workflow.wait_for_event( # doctest: +SKIP
    ...     CustomEventListener, "topic1", "partition2")
    >>> handle_event = ... # doctest: +SKIP
    >>> workflow.run(handle_event.task(event_task)) # doctest: +SKIP

    """

    def __init__(self):
        """Optional constructor. Only the constructor with now arguments will be
        called."""
        pass

    async def poll_for_event(self, *args, **kwargs) -> Event:
        """Should return only when the event is received."""
        raise NotImplementedError

    async def event_checkpointed(self, event: Event) -> None:
        """Optional. Called after an event has been checkpointed and a transaction can
        be safely committed."""
        pass


@PublicAPI(stability="alpha")
class TimerListener(EventListener):
    """
    A listener that produces an event at a given timestamp.
    """

    async def poll_for_event(self, timestamp):
        await asyncio.sleep(timestamp - time.time())
