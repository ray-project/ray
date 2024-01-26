import unittest
import subprocess
import os
from unittest import mock
from ray_release.test import Test, TestState
from ci.ray_ci.automation.filter_tests import obtain_existing_test_names_by_state

class RayCIAutomationFilterTest(unittest.TestCase):
    
    def setUp(self):
        pass
    
    @mock.patch("ray_release.test.Test.get_tests")
    def test_script(self, mock_get_tests):
        mock_get_tests.side_effect = [
            Test(
                {
                    "name": "darwin://test_1",
                    "team": "core",
                    "state": TestState.FLAKY,
                }
            ),
            Test(
                {
                    "name": "darwin://test_2",
                    "team": "ci",
                    "state": TestState.FLAKY,
                }
            ),
        ],
        flaky_test_names = obtain_existing_test_names_by_state(prefix_on=False, test_state="flaky")
        self.assertEqual(flaky_test_names, ["//test_1", "//test_2"])

    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()