# coding: utf-8
"""
Unit/Integration Testing for python/_private/utils.py

This currently expects to work for minimal installs.
"""

import pytest
import logging
from ray._private.utils import get_or_create_event_loop

logger = logging.getLogger(__name__)


def test_get_or_create_event_loop_existing_event_loop():
    import asyncio
    import warnings

    # With running event loop
    expect_loop = asyncio.new_event_loop()
    expect_loop.set_debug(True)
    asyncio.set_event_loop(expect_loop)
    with warnings.catch_warnings():
        # Assert no deprecating warnings raised for python>=3.10
        warnings.simplefilter("error")
        actual_loop = get_or_create_event_loop()

        assert actual_loop == expect_loop, "Loop should not be recreated."


def test_get_or_create_event_loop_new_event_loop():
    import warnings

    with warnings.catch_warnings():
        # Assert no deprecating warnings raised for python>=3.10
        warnings.simplefilter("error")
        loop = get_or_create_event_loop()
        assert loop is not None, "new event loop should be created."


if __name__ == "__main__":
    import os
    import sys

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
