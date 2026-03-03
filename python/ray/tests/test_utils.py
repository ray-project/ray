# coding: utf-8
"""
Unit/Integration Testing for python/_private/utils.py

This currently expects to work for minimal installs.
"""
import logging
import sys
from unittest.mock import mock_open, patch

import pytest

from ray._private import ray_constants
from ray._private.utils import (
    get_current_node_cpu_model_name,
    parse_pg_formatted_resources_to_original,
    resolve_object_store_memory,
    try_import_each_module,
)

logger = logging.getLogger(__name__)


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


def test_parse_pg_formatted_resources():
    out = parse_pg_formatted_resources_to_original(
        {"CPU_group_e765be422c439de2cd263c5d9d1701000000": 1, "memory": 100}
    )
    assert out == {"CPU": 1, "memory": 100}

    out = parse_pg_formatted_resources_to_original(
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


def test_object_store_memory_resolve_to_specified_memory():
    """
    Test object store memory resolves to user specified memory
    when provided.
    """
    available_memory_bytes = 16 * (1024**3)  # 16 GB
    explicit_memory = 4 * (1024**3)  # 4 GB

    result = resolve_object_store_memory(available_memory_bytes, explicit_memory)
    assert result == explicit_memory
    assert (
        result
        != available_memory_bytes * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
    )


def test_object_store_memory_resolve_to_default_memory():
    """
    Test object store memory resolves to default memory when no user
    specified object store memory is provided.
    """
    available_memory_bytes = 4 * (1024**3)  # 4 GB
    # Use large shm so it doesn't cap the result
    mock_shm_bytes = 100 * (1024**3)  # 100 GB
    expected = int(
        available_memory_bytes * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
    )

    with patch("ray._private.utils.sys.platform", "linux"):
        with patch(
            "ray._private.utils.get_shared_memory_bytes",
            return_value=mock_shm_bytes,
        ):
            result = resolve_object_store_memory(available_memory_bytes, None)
            assert result == expected


def test_object_store_memory_cap_to_max_memory_bytes():
    """Test that object store memory is capped to DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES."""
    # Use very large available memory that would exceed the cap
    available_memory_bytes = 1000 * (1024**3)  # 1000 GB
    # Use large shm so shm doesn't cap before the object store memory cap
    mock_shm_bytes = 500 * (1024**3)  # 500 GB
    cap = ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES

    with patch("ray._private.utils.sys.platform", "linux"):
        with patch(
            "ray._private.utils.get_shared_memory_bytes",
            return_value=mock_shm_bytes,
        ):
            result = resolve_object_store_memory(available_memory_bytes, None)
            assert result == cap


def test_object_store_memory_cap_to_linux_shm_cap():
    """Test that Linux respects shared memory cap."""
    available_memory_bytes = 100 * (1024**3)  # 100 GB
    mock_shm_bytes = 20 * (1024**3)  # 20 GB

    with patch("ray._private.utils.sys.platform", "linux"):
        with patch(
            "ray._private.utils.get_shared_memory_bytes", return_value=mock_shm_bytes
        ):
            result = resolve_object_store_memory(available_memory_bytes, None)
            expected_shm_cap = int(mock_shm_bytes * 0.95)
            assert result <= expected_shm_cap


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
