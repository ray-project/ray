"""Integration test for the module filtering functionality in DashboardHead."""

import os
import sys
import tempfile
from unittest.mock import patch

import pytest

from ray.dashboard.head import DashboardHead
from ray.dashboard.subprocesses.tests.utils import TestModule, TestModule1


@pytest.fixture(autouse=True)
def clean_env_var():
    """Clean up environment variables before and after each test."""
    # Remove any existing environment variables that might affect the tests
    if "RAY_DASHBOARD_DISABLED_MODULES" in os.environ:
        del os.environ["RAY_DASHBOARD_DISABLED_MODULES"]
    yield
    # Clean up after test
    if "RAY_DASHBOARD_DISABLED_MODULES" in os.environ:
        del os.environ["RAY_DASHBOARD_DISABLED_MODULES"]


def test_dashboard_head_filters_modules_based_on_env_var():
    """Test that DashboardHead properly filters modules based on environment variable."""
    # Set up a mock subprocess module for testing
    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        # Create mock modules that inherit from SubprocessModule
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Set environment variable to disable TestModule
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule"

        # Create DashboardHead instance with minimal required parameters
        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,  # INFO
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
            )

            # Load subprocess modules to trigger filtering
            modules = dashboard_head._load_subprocess_module_handles()

            # Verify that only TestModule1 is loaded (TestModule should be filtered out)
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            # TestModule should be filtered out due to environment variable
            assert "TestModule" not in loaded_module_names
            # TestModule1 should still be loaded
            assert "TestModule1" in loaded_module_names


def test_dashboard_head_no_filtering_when_env_var_not_set():
    """Test that all modules are loaded when environment variable is not set."""
    # Ensure environment variable is not set
    if "RAY_DASHBOARD_DISABLED_MODULES" in os.environ:
        del os.environ["RAY_DASHBOARD_DISABLED_MODULES"]

    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        # Create mock modules that inherit from SubprocessModule
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Create DashboardHead instance with minimal required parameters
        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,  # INFO
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
            )

            # Load subprocess modules
            modules = dashboard_head._load_subprocess_module_handles()

            # Verify that both modules are loaded
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            assert "TestModule" in loaded_module_names
            assert "TestModule1" in loaded_module_names
            assert len(loaded_module_names) == 2


def test_dashboard_head_multiple_modules_filtered():
    """Test that multiple modules can be filtered out."""
    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        # Create mock modules that inherit from SubprocessModule
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Set environment variable to disable both modules
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule,TestModule1"

        # Create DashboardHead instance with minimal required parameters
        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,  # INFO
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
            )

            # Load subprocess modules
            modules = dashboard_head._load_subprocess_module_handles()

            # Verify that no modules are loaded (both should be filtered out)
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            assert len(loaded_module_names) == 0


def test_dashboard_head_with_specific_modules_to_load():
    """Test filtering works correctly when specific modules are requested."""
    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        # Create mock modules that inherit from SubprocessModule
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Set environment variable to disable TestModule
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule"

        # Create DashboardHead instance requesting specific modules
        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,  # INFO
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
                modules_to_load={"TestModule", "TestModule1"},  # Request both
            )

            # Load subprocess modules
            modules = dashboard_head._load_subprocess_module_handles(
                modules_to_load={"TestModule", "TestModule1"}
            )

            # Verify that only TestModule1 is loaded (TestModule should be filtered out by env var)
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            assert "TestModule" not in loaded_module_names
            assert "TestModule1" in loaded_module_names
            assert len(loaded_module_names) == 1


def test_dashboard_head_minimal_mode_ignores_env_var():
    """Test that in minimal mode, subprocess modules are not loaded regardless of env var."""
    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Set environment variable to disable modules
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule"

        # Create DashboardHead instance in minimal mode
        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,  # INFO
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=True,  # Minimal mode
                serve_frontend=False,
            )

            # Load subprocess modules
            modules = dashboard_head._load_subprocess_module_handles()

            # In minimal mode, no subprocess modules should be loaded
            assert len(modules) == 0


def test_dashboard_head_whitespace_in_env_var():
    """Test that whitespace in environment variable is properly handled."""
    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Set environment variable with whitespace
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = " TestModule , TestModule1 "

        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
            )

            modules = dashboard_head._load_subprocess_module_handles()
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            # Both modules should be filtered out (whitespace should be trimmed)
            assert len(loaded_module_names) == 0


def test_dashboard_head_empty_env_var():
    """Test that empty environment variable behaves like it's not set."""
    # Set environment variable to empty string
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ""

    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        mock_get_modules.return_value = [TestModule, TestModule1]

        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
            )

            modules = dashboard_head._load_subprocess_module_handles()
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            # All modules should be loaded when env var is empty
            assert "TestModule" in loaded_module_names
            assert "TestModule1" in loaded_module_names
            assert len(loaded_module_names) == 2


def test_dashboard_head_case_sensitive_matching():
    """Test that module name matching is case-sensitive."""
    with patch("ray.dashboard.utils.get_all_modules") as mock_get_modules:
        mock_get_modules.return_value = [TestModule, TestModule1]

        # Set environment variable with different case
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "testmodule"  # lowercase

        with tempfile.TemporaryDirectory() as temp_dir:
            dashboard_head = DashboardHead(
                http_host="127.0.0.1",
                http_port=8265,
                http_port_retries=5,
                gcs_address="127.0.0.1:6379",
                cluster_id_hex="test_cluster",
                node_ip_address="127.0.0.1",
                log_dir=temp_dir,
                logging_level=20,
                logging_format="%(message)s",
                logging_filename="test_dashboard.log",
                logging_rotate_bytes=1000000,
                logging_rotate_backup_count=5,
                temp_dir=temp_dir,
                session_dir=temp_dir,
                minimal=False,
                serve_frontend=False,
            )

            modules = dashboard_head._load_subprocess_module_handles()
            loaded_module_names = [handle.module_cls.__name__ for handle in modules]

            # Case-sensitive matching: "testmodule" != "TestModule", so both should load
            assert "TestModule" in loaded_module_names
            assert "TestModule1" in loaded_module_names
            assert len(loaded_module_names) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
