import asyncio
import time
from typing import Dict, Optional
from uuid import uuid4

from ray.serve.common import GoalId
from ray.serve.utils import logger


class AsyncGoalManager:
    """
    Essentially a dictionary of <goal_id, asyncio.Future> within
    ServeController's event loop with APIs to facilitate common calls.
    """

    def __init__(self):
        self._pending_goals: Dict[GoalId, asyncio.Future] = dict()

    def num_pending_goals(self) -> int:
        return len(self._pending_goals)

    def create_goal(self, goal_id: Optional[GoalId] = None) -> GoalId:
        if goal_id is None:
            goal_id = uuid4()

        self._pending_goals[
            goal_id] = asyncio.get_running_loop().create_future()
        return goal_id

    def complete_goal(self, goal_id: GoalId) -> None:
        logger.debug(f"Completing goal {goal_id}")
        future = self._pending_goals.pop(goal_id, None)
        if future:
            future.set_result(goal_id)

    def check_complete(self, goal_id: GoalId) -> bool:
        # TODO: Maybe we can switch this to fut.done() instead ?
        return goal_id not in self._pending_goals

    def fail_goal(self, goal_id) -> None:
        future = self._pending_goals.pop(goal_id, None)
        future.set_exception(Exception(f"Goal {goal_id} failed to complete."))

    async def wait_for_goal(self, goal_id: GoalId) -> None:
        start = time.time()
        if goal_id not in self._pending_goals:
            logger.debug(f"Goal {goal_id} not found")
            return

        fut = self._pending_goals[goal_id]
        try:
            await fut
        except Exception as e:
            # Exception thrown won't stop controller main loop, only
            # handled by goal_id's final status
            fut.set_exception(e)

        logger.debug(
            f"Waiting for goal {goal_id} took {time.time() - start} seconds")
