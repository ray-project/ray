from typing import Callable, Any

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class TrackedTask:
    """Task tracked by a Ray event manager.

    This container class is used to define callbacks to be invoked when
    the task resolves, errors, or times out.

    Example:

        .. code-block:: python

            tracked_task = event_manager.schedule_task(remote_fn)
            tracked_task.on_result(lambda result: print(result))

    """

    def on_result(self, callback: Callable[[Any], None]) -> "TrackedTask":
        """Specify callback to handle successful task resolution.

        The callback should accept one argument:
        The result received from the remote call.

        Args:
            callback: Callback to invoke when the task resolves.

        """
        raise NotImplementedError

    def on_error(self, callback: Callable[[Exception], None]) -> "TrackedTask":
        """Specify callback to handle any errors on task resolution.

        The callback should accept one argument:
        The exception received for the remote call.

        Args:
            callback: Callback to invoke when the task errors.

        """
        raise NotImplementedError
