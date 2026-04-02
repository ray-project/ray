"""Test cases for the module enabling/disabling functionality via environment variables."""

import os
import sys

import pytest

from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.tests.utils import BaseTestModule


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


def test_default_behavior_all_modules_enabled():
    """Test that by default, all modules are enabled when no environment variable is set."""
    # Confirm no environment variable is set
    assert "RAY_DASHBOARD_DISABLED_MODULES" not in os.environ

    # Test that a sample module name returns True
    assert SubprocessModule.is_enabled("TestModule")
    assert SubprocessModule.is_enabled("AnotherModule")
    assert SubprocessModule.is_enabled("YetAnotherModule")


def test_module_disabled_via_environment_variable():
    """Test that modules can be disabled via environment variable."""
    # Set environment variable to disable specific modules
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule1"

    # Test that the disabled module returns False
    assert not SubprocessModule.is_enabled("TestModule1")
    # Test that other modules still return True
    assert SubprocessModule.is_enabled("TestModule2")
    assert SubprocessModule.is_enabled("AnotherModule")


def test_multiple_modules_disabled_via_environment_variable():
    """Test that multiple modules can be disabled via environment variable."""
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule1,TestModule2"

    # Test that the disabled modules return False
    assert not SubprocessModule.is_enabled("TestModule1")
    assert not SubprocessModule.is_enabled("TestModule2")
    # Test that other modules still return True
    assert SubprocessModule.is_enabled("TestModule3")
    assert SubprocessModule.is_enabled("AnotherModule")


def test_whitespace_handling_in_environment_variable():
    """Test that whitespace is properly handled in the environment variable."""
    os.environ[
        "RAY_DASHBOARD_DISABLED_MODULES"
    ] = " TestModule1 , TestModule2 , TestModule3 "

    # Test that the disabled modules return False (whitespace should be trimmed)
    assert not SubprocessModule.is_enabled("TestModule1")
    assert not SubprocessModule.is_enabled("TestModule2")
    assert not SubprocessModule.is_enabled("TestModule3")
    # Test that other modules still return True
    assert SubprocessModule.is_enabled("TestModule4")


def test_empty_strings_in_environment_variable():
    """Test that empty strings in environment variable are handled correctly."""
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ",TestModule1,,TestModule2,"

    # Empty strings should be ignored, TestModule1 and TestModule2 should be disabled
    assert not SubprocessModule.is_enabled("TestModule1")
    assert not SubprocessModule.is_enabled("TestModule2")
    # Other modules should be enabled
    assert SubprocessModule.is_enabled("TestModule3")


def test_case_sensitive_matching():
    """Test that module name matching is case-sensitive."""
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule"

    # Exact match should be disabled
    assert not SubprocessModule.is_enabled("TestModule")
    # Case variations should still be enabled
    assert SubprocessModule.is_enabled("testmodule")
    assert SubprocessModule.is_enabled("TESTMODULE")
    assert SubprocessModule.is_enabled("Testmodule")


def test_inheritance_works_correctly():
    """Test that subclasses inherit the is_enabled functionality correctly."""
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "BaseTestModule"

    # Since BaseTestModule inherits from SubprocessModule, it should use the same logic
    assert not BaseTestModule.is_enabled("BaseTestModule")
    assert BaseTestModule.is_enabled("OtherModule")


def test_none_module_name():
    """Test behavior when None is passed as module name (though this shouldn't normally happen)."""
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "None"

    # This tests the edge case where "None" string is in the disabled list
    assert not SubprocessModule.is_enabled("None")
    assert SubprocessModule.is_enabled("SomeOtherModule")


def test_empty_environment_variable():
    """Test behavior when environment variable is set but empty."""
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ""

    # An empty environment variable should behave like it's not set (all enabled)
    assert SubprocessModule.is_enabled("AnyModule")
    assert SubprocessModule.is_enabled("AnotherModule")


def test_module_name_none_behavior():
    """Test behavior when None is passed as module_name parameter."""
    # When module_name is None, it should not crash
    # In practice, DashboardHead always passes the module name explicitly
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "SubprocessModule"

    # When None is passed, the method should still work
    # None is not in the disabled list, so it should return True
    result = SubprocessModule.is_enabled(None)
    assert result

    # Test with empty disabled list
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ""
    result = SubprocessModule.is_enabled(None)
    assert result


def test_real_module_names():
    """Test with real module names that might be used in production."""
    # Test with some common module names that might exist
    os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "ServeHead,JobHead"

    # These should be disabled
    assert not SubprocessModule.is_enabled("ServeHead")
    assert not SubprocessModule.is_enabled("JobHead")

    # These should still be enabled
    assert SubprocessModule.is_enabled("DataHead")
    assert SubprocessModule.is_enabled("StateHead")
    assert SubprocessModule.is_enabled("MetricsHead")


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
