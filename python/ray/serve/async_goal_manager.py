import asyncio
import time
from typing import Dict, Optional
from uuid import uuid4

from ray.serve.common import GoalId
from ray.serve.utils import logger


class AsyncGoalManager:
    """
    Helper class to facilitate ServeController's async goal creation, tracking
    and termination by maintaining a dictionary of <goal_id, asyncio.Future>.

    Ray serve external api calls can be assigned with a goal_id for tracking,
    and can be either awaited as blocking call or delegate to run
    asynchronously.

    Ex:
        deploy() -> goal_id = UUID('1b985345-f0ae-4cb7-8349-aa2ea881bf89')
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
        self._pending_goals: Dict[GoalId, asyncio.Future] = dict()

    def num_pending_goals(self) -> int:
        return len(self._pending_goals)

    def create_goal(self, goal_id: Optional[GoalId] = None) -> GoalId:
        """
        Create a new asyncio.Future as goal and return its id. If an id
        is already given, assign a future to it and return same id.
        """
        if goal_id is None:
            goal_id = uuid4()

        self._pending_goals[
            goal_id] = asyncio.get_running_loop().create_future()

        return goal_id

    def complete_goal(self,
                      goal_id: GoalId,
                      exception: Optional[Exception] = None) -> None:
        """
        Mark given goal as completed, if an exception object is provided,
        set exception instead.
        """
        logger.debug(f"Completing goal {goal_id}")
        future = self._pending_goals.pop(goal_id, None)
        if future:
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(goal_id)

    def check_complete(self, goal_id: GoalId) -> bool:
        """
        Check if given goal is completed.
        """
        if goal_id not in self._pending_goals:
            return True
        else:
            fut = self._pending_goals[goal_id]
            return fut.done()

    async def wait_for_goal(self, goal_id: GoalId) -> GoalId:
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
            return

        fut = self._pending_goals[goal_id]
        await fut
        logger.debug(
            f"Waiting for goal {goal_id} took {time.time() - start} seconds")

        return fut.result()
