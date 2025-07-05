"""Tests for Ray utility functions.

This module contains pytest-based tests for utility functions in ray._common.utils.
Test utility classes (SignalActor, Semaphore) are in ray._common.test_utils to
ensure they're included in the Ray package distribution.
"""

import asyncio
import warnings
import sys
import os
import tempfile
from unittest.mock import Mock, patch

import pytest

from ray._common.utils import (
    get_or_create_event_loop,
    run_background_task,
    _BACKGROUND_TASKS,
    try_to_create_directory,
    load_class,
    get_system_memory,
)
from ray._common.test_utils import (
    TelemetryCallsite,
    check_library_usage_telemetry,
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


class TestCheckLibraryUsageTelemetry:
    """Tests for the check_library_usage_telemetry function."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.mock_gcs_client = Mock()
        self.mock_usage_lib = Mock()

        # Mock the telemetry functions
        self.get_library_usages_patch = patch(
            "ray._common.test_utils._get_library_usages", return_value=set()
        )
        self.get_extra_usage_tags_patch = patch(
            "ray._common.test_utils._get_extra_usage_tags", return_value={}
        )

        self.mock_get_library_usages = self.get_library_usages_patch.start()
        self.mock_get_extra_usage_tags = self.get_extra_usage_tags_patch.start()

    def teardown_method(self):
        """Clean up after each test method."""
        self.get_library_usages_patch.stop()
        self.get_extra_usage_tags_patch.stop()

    def test_check_library_usage_telemetry_driver_callsite(self):
        """Test telemetry checking with DRIVER callsite."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]

        # Expected library usages
        expected_library_usages = [{"test_library"}]

        check_library_usage_telemetry(
            use_lib_fn=mock_use_lib_fn,
            callsite=TelemetryCallsite.DRIVER,
            expected_library_usages=expected_library_usages,
        )

        mock_use_lib_fn.assert_called_once()

    @patch("ray._common.test_utils.ray")
    def test_check_library_usage_telemetry_actor_callsite(self, mock_ray):
        """Test telemetry checking with ACTOR callsite."""
        mock_use_lib_fn = Mock()

        # Set up mock ray functions
        # Create a mock actor class that returns a mock actor instance
        mock_actor_class = Mock()
        mock_actor_instance = Mock()
        mock_ready_ref = Mock()

        # Set up the chain: ray.remote -> actor_class -> actor_instance
        mock_ray.remote.return_value = mock_actor_class
        mock_actor_class.remote.return_value = mock_actor_instance

        # Set up the __ray_ready__ method chain
        mock_actor_instance.__ray_ready__ = Mock()
        mock_actor_instance.__ray_ready__.remote.return_value = mock_ready_ref

        # ray.get should return True (actor is ready)
        mock_ray.get.return_value = True

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]

        expected_library_usages = [{"test_library"}]

        check_library_usage_telemetry(
            use_lib_fn=mock_use_lib_fn,
            callsite=TelemetryCallsite.ACTOR,
            expected_library_usages=expected_library_usages,
        )

        mock_ray.remote.assert_called_once()
        mock_ray.get.assert_called_once()

    @patch("ray._common.test_utils.ray")
    def test_check_library_usage_telemetry_task_callsite(self, mock_ray):
        """Test telemetry checking with TASK callsite."""
        mock_use_lib_fn = Mock()

        # Set up mock ray functions
        mock_task_ref = Mock()
        mock_ray.remote.return_value = mock_task_ref
        mock_ray.get.return_value = None

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]

        expected_library_usages = [{"test_library"}]

        check_library_usage_telemetry(
            use_lib_fn=mock_use_lib_fn,
            callsite=TelemetryCallsite.TASK,
            expected_library_usages=expected_library_usages,
        )

        mock_ray.remote.assert_called_once()
        mock_ray.get.assert_called_once()

    def test_check_library_usage_telemetry_with_extra_tags(self):
        """Test telemetry checking with extra usage tags."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]
        self.mock_get_extra_usage_tags.return_value = {"test_tag": "test_value"}

        expected_library_usages = [{"test_library"}]
        expected_extra_usage_tags = {"test_tag": "test_value"}

        check_library_usage_telemetry(
            use_lib_fn=mock_use_lib_fn,
            callsite=TelemetryCallsite.DRIVER,
            expected_library_usages=expected_library_usages,
            expected_extra_usage_tags=expected_extra_usage_tags,
        )

    def test_check_library_usage_telemetry_assertion_error_extra_tags(self):
        """Test that assertion error is raised for incorrect extra tags."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]
        self.mock_get_extra_usage_tags.return_value = {"test_tag": "unexpected_value"}

        expected_library_usages = [{"test_library"}]
        expected_extra_usage_tags = {"test_tag": "expected_value"}

        with pytest.raises(AssertionError):
            check_library_usage_telemetry(
                use_lib_fn=mock_use_lib_fn,
                callsite=TelemetryCallsite.DRIVER,
                expected_library_usages=expected_library_usages,
                expected_extra_usage_tags=expected_extra_usage_tags,
            )

    def test_check_library_usage_telemetry_assertion_error_library_usage(self):
        """Test that assertion error is raised for incorrect library usage."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"unexpected_library"},  # After use_lib_fn is called
        ]

        expected_library_usages = [{"expected_library"}]

        with pytest.raises(AssertionError):
            check_library_usage_telemetry(
                use_lib_fn=mock_use_lib_fn,
                callsite=TelemetryCallsite.DRIVER,
                expected_library_usages=expected_library_usages,
            )

    def test_check_library_usage_telemetry_empty_expected_usages(self):
        """Test telemetry checking with empty expected library usages."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            set(),  # After use_lib_fn is called (still empty)
        ]

        expected_library_usages = [set()]  # Expect empty set

        check_library_usage_telemetry(
            use_lib_fn=mock_use_lib_fn,
            callsite=TelemetryCallsite.DRIVER,
            expected_library_usages=expected_library_usages,
        )

    def test_check_library_usage_telemetry_initial_usage_not_empty(self):
        """Test that assertion error is raised if initial library usage is not empty."""
        mock_use_lib_fn = Mock()

        # Set up initial non-empty library usage
        self.mock_get_library_usages.return_value = {"existing_library"}

        expected_library_usages = [{"test_library"}]

        # Should raise AssertionError due to non-empty initial usage
        with pytest.raises(AssertionError):
            check_library_usage_telemetry(
                use_lib_fn=mock_use_lib_fn,
                callsite=TelemetryCallsite.DRIVER,
                expected_library_usages=expected_library_usages,
            )

    def test_check_library_usage_telemetry_multiple_expected_usages(self):
        """Test telemetry checking with multiple expected library usages."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]

        # Multiple expected usage patterns
        expected_library_usages = [
            {"test_library"},
            {"test_library", "another_library"},
        ]

        # Should not raise an exception since the actual usage matches one of the expected patterns
        check_library_usage_telemetry(
            use_lib_fn=mock_use_lib_fn,
            callsite=TelemetryCallsite.DRIVER,
            expected_library_usages=expected_library_usages,
        )

    def test_check_library_usage_telemetry_invalid_callsite(self):
        """Test that assertion error is raised for invalid callsite."""
        mock_use_lib_fn = Mock()

        # Set up expected returns
        self.mock_get_library_usages.side_effect = [
            set(),  # Initial empty check
            {"test_library"},  # After use_lib_fn is called
        ]

        expected_library_usages = [{"test_library"}]

        # Use an invalid callsite value by bypassing type checking
        with pytest.raises(AssertionError, match="Unrecognized callsite"):
            check_library_usage_telemetry(
                use_lib_fn=mock_use_lib_fn,
                callsite="invalid_callsite",  # type: ignore
                expected_library_usages=expected_library_usages,
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
