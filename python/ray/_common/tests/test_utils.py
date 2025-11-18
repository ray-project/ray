"""Tests for Ray utility functions.

This module contains pytest-based tests for utility functions in ray._common.utils.
Test utility classes (SignalActor, Semaphore) are in ray._common.test_utils to
ensure they're included in the Ray package distribution.
"""

import asyncio
import os
import sys
import tempfile
import warnings

import pytest

from ray._common.utils import (
    _BACKGROUND_TASKS,
    get_or_create_event_loop,
    get_system_memory,
    load_class,
    run_background_task,
    try_to_create_directory,
)

# Optional imports for testing
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


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


class TestTryToCreateDirectory:
    """Tests for the try_to_create_directory utility function."""

    def test_create_new_directory(self):
        """Test creating a new directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = os.path.join(temp_dir, "test_dir")
            try_to_create_directory(test_dir)
            assert os.path.exists(test_dir), "Directory should be created"
            assert os.path.isdir(test_dir), "Path should be a directory"

    def test_existing_directory(self):
        """Test creating a directory that already exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = os.path.join(temp_dir, "existing_dir")
            # Create directory first
            os.makedirs(test_dir)
            # Should work without error
            try_to_create_directory(test_dir)
            assert os.path.exists(test_dir), "Directory should still exist"

    def test_nested_directory_creation(self):
        """Test creating nested directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_dir = os.path.join(temp_dir, "nested", "deep", "structure")
            try_to_create_directory(nested_dir)
            assert os.path.exists(nested_dir), "Nested directory should be created"

    def test_tilde_expansion(self):
        """Test directory creation with tilde expansion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_home = os.path.join(temp_dir, "fake_home")
            os.makedirs(fake_home, exist_ok=True)

            # Mock the expanduser for this test
            original_expanduser = os.path.expanduser
            os.path.expanduser = (
                lambda path: path.replace("~", fake_home)
                if path.startswith("~")
                else path
            )

            try:
                tilde_dir = "~/test_tilde_dir"
                try_to_create_directory(tilde_dir)
                expected_path = os.path.join(fake_home, "test_tilde_dir")
                assert os.path.exists(
                    expected_path
                ), "Tilde-expanded directory should be created"
            finally:
                # Restore original expanduser
                os.path.expanduser = original_expanduser


class TestLoadClass:
    """Tests for the load_class utility function."""

    def test_load_builtin_class(self):
        """Test loading a builtin class."""
        list_class = load_class("builtins.list")
        assert list_class is list, "Should load the builtin list class"

    def test_load_module(self):
        """Test loading a module."""
        path_module = load_class("os.path")
        import os.path

        assert path_module is os.path, "Should load os.path module"

    def test_load_function(self):
        """Test loading a function from a module."""
        makedirs_func = load_class("os.makedirs")
        assert makedirs_func is os.makedirs, "Should load os.makedirs function"

    def test_load_standard_library_class(self):
        """Test loading a standard library class."""
        temp_dir_class = load_class("tempfile.TemporaryDirectory")
        assert (
            temp_dir_class is tempfile.TemporaryDirectory
        ), "Should load TemporaryDirectory class"

    def test_load_nested_module_class(self):
        """Test loading a class from a nested module."""
        datetime_class = load_class("datetime.datetime")
        import datetime

        assert (
            datetime_class is datetime.datetime
        ), "Should load datetime.datetime class"

    def test_invalid_path_error(self):
        """Test error handling for invalid paths."""
        with pytest.raises(ValueError, match="valid path like mymodule.provider_class"):
            load_class("invalid")

    def test_nonexistent_module_error(self):
        """Test error handling for nonexistent modules."""
        with pytest.raises((ImportError, ModuleNotFoundError)):
            load_class("nonexistent_module.SomeClass")

    def test_nonexistent_attribute_error(self):
        """Test error handling for nonexistent attributes."""
        with pytest.raises(AttributeError):
            load_class("os.NonexistentClass")


class TestGetSystemMemory:
    """Tests for the get_system_memory utility function."""

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v1_with_low_limit(self):
        """Test cgroups v1 with low memory limit."""
        with tempfile.NamedTemporaryFile("w") as memory_limit_file:
            memory_limit_file.write("1073741824")  # 1GB
            memory_limit_file.flush()
            memory = get_system_memory(
                memory_limit_filename=memory_limit_file.name,
                memory_limit_filename_v2="__does_not_exist__",
            )
            assert memory == 1073741824, "Should return cgroup limit when low"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v1_with_high_limit(self):
        """Test cgroups v1 with high memory limit (should fallback to psutil)."""
        with tempfile.NamedTemporaryFile("w") as memory_limit_file:
            memory_limit_file.write(str(2**63))  # Very high limit
            memory_limit_file.flush()
            psutil_memory = psutil.virtual_memory().total
            memory = get_system_memory(
                memory_limit_filename=memory_limit_file.name,
                memory_limit_filename_v2="__does_not_exist__",
            )
            assert (
                memory == psutil_memory
            ), "Should fallback to psutil when cgroup limit is very high"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v2_with_limit(self):
        """Test cgroups v2 with memory limit set."""
        with tempfile.NamedTemporaryFile("w") as memory_max_file:
            memory_max_file.write("2147483648\n")  # 2GB with newline
            memory_max_file.flush()
            memory = get_system_memory(
                memory_limit_filename="__does_not_exist__",
                memory_limit_filename_v2=memory_max_file.name,
            )
            assert memory == 2147483648, "Should return cgroups v2 limit"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v2_unlimited(self):
        """Test cgroups v2 with unlimited memory (max)."""
        with tempfile.NamedTemporaryFile("w") as memory_max_file:
            memory_max_file.write("max")
            memory_max_file.flush()
            psutil_memory = psutil.virtual_memory().total
            memory = get_system_memory(
                memory_limit_filename="__does_not_exist__",
                memory_limit_filename_v2=memory_max_file.name,
            )
            assert (
                memory == psutil_memory
            ), "Should fallback to psutil when cgroups v2 is unlimited"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_no_cgroup_files(self):
        """Test fallback to psutil when no cgroup files exist."""
        psutil_memory = psutil.virtual_memory().total
        memory = get_system_memory(
            memory_limit_filename="__does_not_exist__",
            memory_limit_filename_v2="__also_does_not_exist__",
        )
        assert memory == psutil_memory, "Should use psutil when no cgroup files exist"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
