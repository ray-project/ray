import asyncio
import warnings
import sys

import pytest

from ray._common.utils import (
    get_or_create_event_loop,
)


def test_get_or_create_event_loop_existing_event_loop():
    # With running event loop
    expect_loop = asyncio.new_event_loop()
    expect_loop.set_debug(True)
    asyncio.set_event_loop(expect_loop)
    with warnings.catch_warnings():
        # Assert no deprecating warnings raised for python>=3.10.
        warnings.simplefilter("error")
        actual_loop = get_or_create_event_loop()

        assert actual_loop == expect_loop, "Loop should not be recreated."


def test_get_or_create_event_loop_new_event_loop():
    with warnings.catch_warnings():
        # Assert no deprecating warnings raised for python>=3.10.
        warnings.simplefilter("error")
        loop = get_or_create_event_loop()
        assert loop is not None, "new event loop should be created."

if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
