# coding: utf-8
"""
Unit/Integration Testing for python/_private/utils.py

This currently expects to work for minimal installs.
"""

import pytest
import logging
from ray._private.utils import (
    get_or_create_event_loop,
    pasre_pg_formatted_resources_to_original,
    try_import_each_module,
    get_current_node_cpu_model_name,
)
from unittest.mock import patch, mock_open
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


def test_pasre_pg_formatted_resources():
    out = pasre_pg_formatted_resources_to_original(
        {"CPU_group_e765be422c439de2cd263c5d9d1701000000": 1, "memory": 100}
    )
    assert out == {"CPU": 1, "memory": 100}

    out = pasre_pg_formatted_resources_to_original(
        {
            "memory_group_4da1c24ac25bec85bc817b258b5201000000": 100.0,
            "memory_group_0_4da1c24ac25bec85bc817b258b5201000000": 100.0,
            "CPU_group_0_4da1c24ac25bec85bc817b258b5201000000": 1.0,
            "CPU_group_4da1c24ac25bec85bc817b258b5201000000": 1.0,
        }
    )
    assert out == {"CPU": 1, "memory": 100}


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Doesn't support non-linux"
)
def test_get_current_node_cpu_model_name():
    with patch(
        "builtins.open", mock_open(read_data="processor: 0\nmodel name: Intel Xeon")
    ):
        assert get_current_node_cpu_model_name() == "Intel Xeon"


if __name__ == "__main__":
    import os

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
