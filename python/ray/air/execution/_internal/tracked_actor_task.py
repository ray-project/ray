from typing import Callable, Any, List

from ray.air.execution._internal.tracked_actor import TrackedActor


class TrackedActorTask:
    """Actor task tracked by a Ray event manager.

    This container class is used to define callbacks to be invoked when
    the task resolves, errors, or times out.

    Note:
        Objects of this class are returned by the :class:`RayEventManager`.
        This class should not be instantiated manually.

    Example:

        .. code-block:: python

            tracked_futures = event_manager.schedule_actor_tasks(
                actor_manager.live_actors,
                "foo")
            tracked_futures.on_result(lambda actor, result: print(result))

    """

    def __init__(self, tracked_actor: TrackedActor):
        self._tracked_actor = tracked_actor

        self._on_result = None
        self._on_error = None

    def on_result(
        self, callback: Callable[[TrackedActor, Any], None]
    ) -> "TrackedActorTask":
        """Specify callback to handle successful task resolution.

        The callback should accept two arguments: The actor for which the
        task resolved, and the result received from the remote call.

        Args:
            callback: Callback to invoke when the task resolves.
        """
        self._on_result = callback
        return self

    def on_error(
        self, callback: Callable[[TrackedActor, Exception], None]
    ) -> "TrackedActorTask":
        """Specify callback to handle any errors on task resolution.

        The callback should accept two arguments: The actor for which the
        task threw an error, and the exception.

        Args:
            callback: Callback to invoke when the task errors.
        """
        self._on_error = callback
        return self


class TrackedActorTaskCollection:
    """Collection of actor tasks tracked by a Ray event manager.

    This container class contains one or more :ref:`TrackedActorTask`s.

    Callbacks specified for this collection will be propagated to each single
    actor task.

    Note:
        Objects of this class are returned by the :class:`RayEventManager`.
        This class should not be instantiated manually.

    Example:

        .. code-block:: python

            tracked_actor_tasks = event_manager.schedule_actor_tasks(
                event_manager.live_actors,
                "foo")
            tracked_actor_tasks.on_result(lambda actor, result: print(result))

    """

    def __init__(self, actor_tasks: List[TrackedActorTask]):
        self._actors_tasks = actor_tasks

    def on_result(
        self, callback: Callable[[TrackedActor, Any], None]
    ) -> "TrackedActorTaskCollection":
        """Specify callback to handle successful task resolution.

        The callback should accept two arguments: The actor for which the
        task resolved, and the result received from the remote call.

        Args:
            callback: Callback to invoke when a task resolves.
        """
        for actor_task in self._actors_tasks:
            actor_task.on_result(callback=callback)
        return self

    def on_error(
        self, callback: Callable[[TrackedActor, Exception], None]
    ) -> "TrackedActorTaskCollection":
        """Specify callback to handle any errors on future resolution.

        The callback should accept two arguments: The actor for which the
        task threw an error, and the exception.

        Args:
            callback: Callback to invoke when a task errors.
        """
        for actor_task in self._actors_tasks:
            actor_task.on_result(callback=callback)
        return self
