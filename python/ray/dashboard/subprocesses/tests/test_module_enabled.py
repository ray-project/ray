"""Test cases for the module enabling/disabling functionality via environment variables."""

import os
import unittest

from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.tests.utils import BaseTestModule


class TestModuleEnabled(unittest.TestCase):
    """Test cases for SubprocessModule.is_enabled functionality."""

    def setUp(self):
        """Clean up environment variables before each test."""
        # Remove any existing environment variables that might affect the tests
        if "RAY_DASHBOARD_DISABLED_MODULES" in os.environ:
            del os.environ["RAY_DASHBOARD_DISABLED_MODULES"]

    def test_default_behavior_all_modules_enabled(self):
        """Test that by default, all modules are enabled when no environment variable is set."""
        # Confirm no environment variable is set
        self.assertNotIn("RAY_DASHBOARD_DISABLED_MODULES", os.environ)
        
        # Test that a sample module name returns True
        self.assertTrue(SubprocessModule.is_enabled("TestModule"))
        self.assertTrue(SubprocessModule.is_enabled("AnotherModule"))
        self.assertTrue(SubprocessModule.is_enabled("YetAnotherModule"))

    def test_module_disabled_via_environment_variable(self):
        """Test that modules can be disabled via environment variable."""
        # Set environment variable to disable specific modules
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule1"
        
        # Test that the disabled module returns False
        self.assertFalse(SubprocessModule.is_enabled("TestModule1"))
        # Test that other modules still return True
        self.assertTrue(SubprocessModule.is_enabled("TestModule2"))
        self.assertTrue(SubprocessModule.is_enabled("AnotherModule"))

    def test_multiple_modules_disabled_via_environment_variable(self):
        """Test that multiple modules can be disabled via environment variable."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule1,TestModule2"
        
        # Test that the disabled modules return False
        self.assertFalse(SubprocessModule.is_enabled("TestModule1"))
        self.assertFalse(SubprocessModule.is_enabled("TestModule2"))
        # Test that other modules still return True
        self.assertTrue(SubprocessModule.is_enabled("TestModule3"))
        self.assertTrue(SubprocessModule.is_enabled("AnotherModule"))

    def test_whitespace_handling_in_environment_variable(self):
        """Test that whitespace is properly handled in the environment variable."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = " TestModule1 , TestModule2 , TestModule3 "
        
        # Test that the disabled modules return False (whitespace should be trimmed)
        self.assertFalse(SubprocessModule.is_enabled("TestModule1"))
        self.assertFalse(SubprocessModule.is_enabled("TestModule2"))
        self.assertFalse(SubprocessModule.is_enabled("TestModule3"))
        # Test that other modules still return True
        self.assertTrue(SubprocessModule.is_enabled("TestModule4"))

    def test_empty_strings_in_environment_variable(self):
        """Test that empty strings in environment variable are handled correctly."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ",TestModule1,,TestModule2,"
        
        # Empty strings should be ignored, TestModule1 and TestModule2 should be disabled
        self.assertFalse(SubprocessModule.is_enabled("TestModule1"))
        self.assertFalse(SubprocessModule.is_enabled("TestModule2"))
        # Other modules should be enabled
        self.assertTrue(SubprocessModule.is_enabled("TestModule3"))

    def test_case_sensitive_matching(self):
        """Test that module name matching is case-sensitive."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "TestModule"
        
        # Exact match should be disabled
        self.assertFalse(SubprocessModule.is_enabled("TestModule"))
        # Case variations should still be enabled
        self.assertTrue(SubprocessModule.is_enabled("testmodule"))
        self.assertTrue(SubprocessModule.is_enabled("TESTMODULE"))
        self.assertTrue(SubprocessModule.is_enabled("Testmodule"))

    def test_inheritance_works_correctly(self):
        """Test that subclasses inherit the is_enabled functionality correctly."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "BaseTestModule"
        
        # Since BaseTestModule inherits from SubprocessModule, it should use the same logic
        self.assertFalse(BaseTestModule.is_enabled("BaseTestModule"))
        self.assertTrue(BaseTestModule.is_enabled("OtherModule"))

    def test_none_module_name(self):
        """Test behavior when None is passed as module name (though this shouldn't normally happen)."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "None"
        
        # This tests the edge case where "None" string is in the disabled list
        self.assertFalse(SubprocessModule.is_enabled("None"))
        self.assertTrue(SubprocessModule.is_enabled("SomeOtherModule"))

    def test_empty_environment_variable(self):
        """Test behavior when environment variable is set but empty."""
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ""
        
        # An empty environment variable should behave like it's not set (all enabled)
        self.assertTrue(SubprocessModule.is_enabled("AnyModule"))
        self.assertTrue(SubprocessModule.is_enabled("AnotherModule"))

    def test_module_name_none_behavior(self):
        """Test behavior when None is passed as module_name parameter."""
        # When module_name is None, it should not crash
        # In practice, DashboardHead always passes the module name explicitly
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "SubprocessModule"
        
        # When None is passed, the method should still work
        # None is not in the disabled list, so it should return True
        result = SubprocessModule.is_enabled(None)
        self.assertTrue(result)
        
        # Test with empty disabled list
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = ""
        result = SubprocessModule.is_enabled(None)
        self.assertTrue(result)

    def test_real_module_names(self):
        """Test with real module names that might be used in production."""
        # Test with some common module names that might exist
        os.environ["RAY_DASHBOARD_DISABLED_MODULES"] = "ServeHead,JobHead"
        
        # These should be disabled
        self.assertFalse(SubprocessModule.is_enabled("ServeHead"))
        self.assertFalse(SubprocessModule.is_enabled("JobHead"))
        
        # These should still be enabled
        self.assertTrue(SubprocessModule.is_enabled("DataHead"))
        self.assertTrue(SubprocessModule.is_enabled("StateHead"))
        self.assertTrue(SubprocessModule.is_enabled("MetricsHead"))


if __name__ == "__main__":
    unittest.main()