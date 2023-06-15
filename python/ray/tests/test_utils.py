# coding: utf-8
"""
Unit/Integration Testing for python/_private/utils.py

This currently expects to work for minimal installs.
"""

import pytest
import unittest
import logging
from ray._private.utils import get_or_create_event_loop, try_import_each_module
from unittest.mock import patch
import sys

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


def test_try_import_each_module():
    mock_sys_modules = sys.modules.copy()
    modules_to_import = ["json", "logging", "re"]
    fake_module = "fake_import_does_not_exist"

    for lib in modules_to_import:
        if lib in mock_sys_modules:
            del mock_sys_modules[lib]

    with patch("ray._private.utils.logger.exception") as mocked_log_exception:
        with patch.dict(sys.modules, mock_sys_modules):
            patched_sys_modules = sys.modules

            try_import_each_module(
                modules_to_import[:1] + [fake_module] + modules_to_import[1:]
            )

            # Verify modules are imported.
            for lib in modules_to_import:
                assert (
                    lib in patched_sys_modules
                ), f"lib {lib} not in {patched_sys_modules}"

            # Verify import error is printed.
            found = False
            for args, _ in mocked_log_exception.call_args_list:
                found = any(fake_module in arg for arg in args)
                if found:
                    break

            assert found, (
                "Did not find print call with import "
                f"error {mocked_log_exception.call_args_list}"
            )


class TestImportAttr(unittest.TestCase):
    def test_none_full_path(self):
        """Test when full_path is None.

        When `full_path` is None, import_attr should raise a TypeError.
        """
        from ray._private.utils import import_attr

        with self.assertRaises(TypeError):
            import_attr(full_path=None)

    def test_multiple_colons_in_full_path(self):
        """Test when full_path has multiple colons.

        When `full_path` has multiple colons, import_attr should raise a ValueError.
        """
        from ray._private.utils import import_attr

        with self.assertRaises(ValueError):
            import_attr(full_path="resources:foo:bar")

    def test_valid_no_colons_full_path(self):
        """Test when full_path is valid and has no colons.

        When `full_path` is valid and as no colons, import_attr should succeed.
        `sys.path` should have dashboard ordered in the end.
        """
        from ray._private.utils import import_attr

        dashboard_path = "/ray/dashboard"
        expected_path = sys.path.copy()
        expected_path.append(dashboard_path)

        # Test sys.path have dashboard ordered in the end.
        sys.path.append(dashboard_path)
        import_attr(full_path="resources.foo.bar")
        assert sys.path == expected_path

    def test_valid_one_colons_full_path(self):
        """Test when full_path is valid and has one colon.

        When `full_path` is valid and as one colon, import_attr should succeed.
        `sys.path` should have dashboard ordered in the end.
        """
        from ray._private.utils import import_attr

        dashboard_path = "/ray/dashboard"
        expected_path = sys.path.copy()
        expected_path.append(dashboard_path)

        # Test sys.path have dashboard ordered in the end.
        sys.path.append(dashboard_path)
        import_attr(full_path="resources.foo:bar")
        assert sys.path == expected_path


if __name__ == "__main__":
    import os

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
