import asyncio
import time
from typing import Dict, Optional
from uuid import uuid4

from ray.serve.common import GoalId
from ray.serve.utils import logger


class AsyncGoalManager:
    def __init__(self):
        self._pending_goals: Dict[GoalId, asyncio.Event] = dict()

    def num_pending_goals(self) -> int:
        return len(self._pending_goals)

    def create_goal(self, goal_id: Optional[GoalId] = None) -> GoalId:
        if goal_id is None:
            goal_id = uuid4()
        self._pending_goals[goal_id] = asyncio.Event()
        return goal_id

    def complete_goal(self, goal_id: GoalId) -> None:
        logger.debug(f"Completing goal {goal_id}")
        event = self._pending_goals.pop(goal_id, None)
        if event:
            event.set()

    def check_complete(self, goal_id: GoalId) -> bool:
        return goal_id not in self._pending_goals

    async def wait_for_goal(self, goal_id: GoalId) -> None:
        start = time.time()
        if goal_id not in self._pending_goals:
            logger.debug(f"Goal {goal_id} not found")
            return

        event = self._pending_goals[goal_id]
        await event.wait()
        logger.debug(
            f"Waiting for goal {goal_id} took {time.time() - start} seconds")
