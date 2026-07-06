import abc
from typing import Callable

from ray.core.generated.events_base_event_pb2 import RayEvent


class PlatformEventProvider(abc.ABC):
    """
    Base interface for all platform-specific event providers (e.g. Kubernetes).
    """

    def __init__(self, callback: Callable[[RayEvent], None]):
        """
        Initialize the provider.
        Implementations of run() may invoke the callback from background worker
        threads, so consumers are responsible for passing a callback that is
        fully thread-safe.

        Args:
            callback: A callback function to deliver new or updated RayEvent
                protobuf objects to the central manager.
        """
        self._callback = callback

    @abc.abstractmethod
    async def run(self) -> None:
        """
        Start the provider. This method should run the event watch/poll loop.
        """
        pass

    @abc.abstractmethod
    async def cleanup(self) -> None:
        """
        Stop the provider and release all resources gracefully.
        """
        pass
