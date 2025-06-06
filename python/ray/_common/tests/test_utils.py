"""Tests for Ray utility functions.

This module contains pytest-based tests for utility functions in ray._common.utils.
Test utility classes (SignalActor, Semaphore) are in ray._common.test_utils to
ensure they're included in the Ray package distribution.
"""

import asyncio
import warnings
import sys

import pytest

from ray._common.utils import (
    get_or_create_event_loop,
    run_background_task,
    _BACKGROUND_TASKS,
)


class TestGetOrCreateEventLoop:
    """Tests for the get_or_create_event_loop utility function."""

    def test_existing_event_loop(self):
        # With running event loop
        expect_loop = asyncio.new_event_loop()
        expect_loop.set_debug(True)
        asyncio.set_event_loop(expect_loop)
        with warnings.catch_warnings():
            # Assert no deprecating warnings raised for python>=3.10.
            warnings.simplefilter("error")
            actual_loop = get_or_create_event_loop()

            assert actual_loop == expect_loop, "Loop should not be recreated."

    def test_new_event_loop(self):
        with warnings.catch_warnings():
            # Assert no deprecating warnings raised for python>=3.10.
            warnings.simplefilter("error")
            loop = get_or_create_event_loop()
            assert loop is not None, "new event loop should be created."


@pytest.mark.asyncio
async def test_run_background_task():
    """Test the run_background_task utility function."""
    result = {}

    async def co():
        result["start"] = 1
        await asyncio.sleep(0)
        result["end"] = 1

    run_background_task(co())

    # Background task is running.
    assert len(_BACKGROUND_TASKS) == 1
    # co executed.
    await asyncio.sleep(0)
    # await asyncio.sleep(0) from co is reached.
    await asyncio.sleep(0)
    # co finished and callback called.
    await asyncio.sleep(0)
    # The task should be removed from the set once it finishes.
    assert len(_BACKGROUND_TASKS) == 0

    assert result.get("start") == 1
    assert result.get("end") == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
