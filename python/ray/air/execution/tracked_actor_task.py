from typing import Callable, Any

from ray.air.execution.tracked_actor import TrackedActor


class TrackedActorTask:
    """Actor task tracked by a Ray event manager.

    This container class is used to define callbacks to be invoked when
    the task resolves, errors, or times out.

    Example:

        .. code-block:: python

            tracked_futures = event_manager.schedule_actor_tasks(
                actor_manager.live_actors,
                "foo")
            tracked_futures.on_result(lambda actor, result: print(result))

    """

    def on_result(self, callback: Callable[[TrackedActor, Any], None]):
        """Specify callback to handle successful task resolution.

        The callback should accept two arguments: The actor for which the
        task resolved, and the result received from the remote call.

        Args:
            callback: Callback to invoke when the task resolves.
        """
        raise NotImplementedError

    def on_error(self, callback: Callable[[TrackedActor, Exception], None]):
        """Specify callback to handle any errors on task resolution.

        The callback should accept two arguments: The actor for which the
        task threw an error, and the exception.

        Args:
            callback: Callback to invoke when the task errors.
        """
        raise NotImplementedError


class TrackedActorTaskCollection:
    """Collection of actor tasks tracked by a Ray event manager.

    This container class contains one or more :ref:`TrackedActorTask`s.

    Callbacks specified for this collection will be propagated to each single
    actor task.

    Example:

        .. code-block:: python

            tracked_actor_tasks = event_manager.schedule_actor_tasks(
                event_manager.live_actors,
                "foo")
            tracked_actor_tasks.on_result(lambda actor, result: print(result))

    """

    def on_result(self, callback: Callable[[TrackedActor, Any], None]):
        """Specify callback to handle successful task resolution.

        The callback should accept two arguments: The actor for which the
        task resolved, and the result received from the remote call.

        Args:
            callback: Callback to invoke when a task resolves.
        """
        raise NotImplementedError

    def on_error(self, callback: Callable[[TrackedActor, Exception], None]):
        """Specify callback to handle any errors on future resolution.

        The callback should accept two arguments: The actor for which the
        task threw an error, and the exception.

        Args:
            callback: Callback to invoke when a task errors.
        """
        raise NotImplementedError
