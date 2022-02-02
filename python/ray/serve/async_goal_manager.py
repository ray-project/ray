import asyncio
from dataclasses import dataclass
import time
from typing import Any, Coroutine, Dict, Optional
from uuid import uuid4

from ray.serve.common import GoalId
from ray.serve.utils import logger


@dataclass
class AsyncGoal:
    """
    Wrapper dataclass to represent an async goal, usually created by user's
    deploy / delete deployment API calls with built-in synchronization object
    and optional result / exception fields.

    A trade-off between providing asyncio.Future-like functionalities without
    introducing its complexities of testing without clear value added.
    """

    goal_id: GoalId
    event: asyncio.Event
    result: Optional[Any]
    exception: Optional[Exception]

    def __init__(
        self,
        goal_id: GoalId,
        event: asyncio.Event,
        result: Optional[Any] = None,
        exception: Optional[Exception] = None,
    ):
        self.goal_id = goal_id
        self.event = event
        self.result = result
        self.exception = exception

    # TODO: (jiaodong) Setup proper result and exception handling flow later
    def set_result(self, result: Any) -> None:
        assert not self.done(), "Can only set result once with unset Event"
        assert (
            self.result is None and self.exception is None
        ), "Can only set result or exception once on the same AsyncGoal."

        self.result = result
        self.event.set()

    def set_exception(self, exception: Exception) -> None:
        assert not self.done(), "Can only set result once with unset Event"
        assert (
            self.result is None and self.exception is None
        ), "Can only set result or exception once on the same AsyncGoal."

        self.exception = exception
        self.event.set()

    def done(self) -> bool:
        return self.event.is_set()

    def wait(self) -> Coroutine:
        return self.event.wait()


class AsyncGoalManager:
    """
    Helper class to facilitate ServeController's async goal creation, tracking
    and termination by maintaining a dictionary of <goal_id, AsyncGoal>.

    Ray serve external api calls can be assigned with a goal_id for tracking,
    and can be either awaited as blocking call or delegate to run
    asynchronously.

    Ex:
        deploy() -> goal_id = AsyncGoal(
                                UUID('1b985345-f0ae-4cb7-8349-aa2ea881bf89')
                              )
            if blocking:
                # Block until deploy() action is done
                wait_for_goal(goal_id)
            else:
                # Immediately return and let controller to deploy async
                return goal_id

    As of now, each external api call as well as goal states are also
    checkpointed in case of serve controller failure.
    """

    def __init__(self):
        self._pending_goals: Dict[GoalId, AsyncGoal] = dict()

    def num_pending_goals(self) -> int:
        return len(self._pending_goals)

    def create_goal(self, goal_id: Optional[GoalId] = None) -> GoalId:
        """
        Create a new AsyncGoal as goal and return its id. If an id
        is already given, assign a future to it and return same id.
        """
        if goal_id is None:
            goal_id = uuid4()

        self._pending_goals[goal_id] = AsyncGoal(goal_id, asyncio.Event())

        return goal_id

    def get_goal(self, goal_id: GoalId) -> Optional[AsyncGoal]:
        """
        Retrieve reference to the underlying AsyncGoal object with given
        goal_id. Note the object might be not available anymore if the
        goal is already completed or popped from pending_goals.

        But if the goal is still pending while this function is called, we
        can use this AsyncGoal object for testing or tracking purposes.
        """
        return self._pending_goals.get(goal_id)

    def complete_goal(
        self, goal_id: GoalId, exception: Optional[Exception] = None
    ) -> None:
        """
        Mark given goal as completed, if an exception object is provided,
        set exception instead.
        """
        logger.debug(f"Completing goal {goal_id}")
        async_goal = self._pending_goals.pop(goal_id, None)
        if async_goal:
            if exception:
                async_goal.set_exception(exception)
            else:
                async_goal.set_result(goal_id)

    def check_complete(self, goal_id: GoalId) -> bool:
        """
        Check if given goal is completed by checking underlying asyncio.Event
        """
        if goal_id not in self._pending_goals:
            return True
        else:
            async_goal = self._pending_goals[goal_id]
            return async_goal.done()

    async def wait_for_goal(self, goal_id: GoalId) -> Optional[Exception]:
        """
        Wait for given goal_id to complete by external code calling
        complete_goal(goal_id), either result or exception could be set.

        Args:
            goal_id (GoalId): Target goal_id to wait on
        Returns:
            goal_id (GoalId): If goal_id finished successfully
        Raises:
            exception (Exception): If set by caller of complete_goal() with
                an exception object
        """
        start = time.time()
        if goal_id not in self._pending_goals:
            logger.debug(f"Goal {goal_id} not found")
            return None

        async_goal = self._pending_goals[goal_id]
        await async_goal.wait()
        logger.debug(f"Waiting for goal {goal_id} took {time.time() - start} seconds")

        if async_goal.exception is not None:
            return async_goal.exception
