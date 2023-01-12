from typing import Callable, Any

from ray.air.execution.actor_spec import TrackedActor


class TrackedFutures:
    """Collection of futures tracked by an actor manager.

    This container class is used to define callbacks to be invoked when
    the futures resolve, error, or timeout.

    Example:

        .. code-block:: python

            tracked_futures = actor_manager.schedule_tasks(
                actor_manager.live_actors,
                "foo")
            tracked_futures.on_result(lambda actor, result: print(result))

    """

    def on_result(self, callback: Callable[[TrackedActor, Any], None]):
        """Define callback to handle successful future resolution.

        The callback should accept two arguments: The actor for which the
        future resolved, and the result received from the remote call.

        Args:
            callback: Callback to invoke when a future resolved.
        """
        raise NotImplementedError

    def on_error(self, callback: Callable[[TrackedActor, Exception], None]):
        """Define callback to handle any errors on future resolution.

        The callback should accept two arguments: The actor for which the
        future threw an error, and the exception.

        Args:
            callback: Callback to invoke when a future errorred.
        """
        raise NotImplementedError

    def on_timeout(self, callback: Callable[[TrackedActor], None]):
        """Define callback to handle timed out futures.

        The callback should accept one argument: The actor for which the
        future resolution timed out.

        Args:
            callback: Callback to invoke when a future timed out.
        """
        raise NotImplementedError
