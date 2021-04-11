import asyncio
import pytest

from ray.serve.async_goal_manager import AsyncGoalManager


@pytest.mark.asyncio
async def test_wait_for_goals():
    manager = AsyncGoalManager()

    # Check empty goal
    await manager.wait_for_goal(None)

    goal_id = manager.create_goal()
    loop = asyncio.get_event_loop()
    waiting = loop.create_task(manager.wait_for_goal(goal_id))

    assert not waiting.done(), "Unfinished task should not be done"
    manager.complete_goal(goal_id)

    await waiting

    # Test double waiting is okay
    await manager.wait_for_goal(goal_id)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
