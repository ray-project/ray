from typing import Callable, Any

from ray.air.execution.tracked_actor import TrackedActor


class TrackedTask:
    """Task tracked by a Ray event manager.

    This container class is used to define callbacks to be invoked when
    the task resolves, errors, or times out.

    Example:

        .. code-block:: python

            tracked_task = event_manager.schedule_task(remote_fn)
            tracked_task.on_result(lambda result: print(result))

    """

    def on_result(self, callback: Callable[[Any], None]):
        """Specify callback to handle successful task resolution.

        The callback should accept one argument:
        The result received from the remote call.

        Args:
            callback: Callback to invoke when the task resolves.

        """
        raise NotImplementedError

    def on_error(self, callback: Callable[[TrackedActor, Exception], None]):
        """Specify callback to handle any errors on task resolution.

        The callback should accept one argument:
        The exception received for the remote call.

        Args:
            callback: Callback to invoke when the task errors.

        """
        raise NotImplementedError

    def on_timeout(self, callback: Callable[[], None]):
        """Specify callback to handle task time out.

        The callback should not expect any arguments.

        Args:
            callback: Callback to invoke when the task times out.

        """
        raise NotImplementedError
