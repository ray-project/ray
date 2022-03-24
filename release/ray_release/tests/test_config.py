import os
import sys
import unittest

from ray_release.config import (
    read_and_validate_release_test_collection,
    Test,
    validate_release_test_collection,
)
from ray_release.exception import ReleaseTestConfigError


class ConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_collection_file = os.path.join(
            os.path.dirname(__file__), "..", "..", "release_tests.yaml"
        )

        self.valid_test = Test(
            **{
                "name": "validation_test",
                "group": "validation_group",
                "working_dir": "validation_dir",
                "legacy": {
                    "test_name": "validation_test",
                    "test_suite": "validation_suite",
                },
                "frequency": "nightly",
                "team": "release",
                "cluster": {
                    "cluster_env": "app_config.yaml",
                    "cluster_compute": "tpl_cpu_small.yaml",
                    "autosuspend_mins": 10,
                },
                "run": {
                    "timeout": 100,
                    "script": "python validate.py",
                    "wait_for_nodes": {"num_nodes": 2, "timeout": 100},
                    "type": "client",
                },
                "smoke_test": {"run": {"timeout": 20}, "frequency": "multi"},
                "alert": "default",
            }
        )

    def testSchemaValidation(self):
        test = self.valid_test.copy()

        validate_release_test_collection([test])

        # Remove some optional arguments
        del test["alert"]
        del test["run"]["wait_for_nodes"]
        del test["cluster"]["autosuspend_mins"]

        validate_release_test_collection([test])

        # Add some faulty arguments

        # Faulty frequency
        invalid_test = test.copy()
        invalid_test["frequency"] = "invalid"
        with self.assertRaises(ReleaseTestConfigError):
            validate_release_test_collection([invalid_test])

        # Faulty job type
        invalid_test = test.copy()
        invalid_test["run"]["type"] = "invalid"
        with self.assertRaises(ReleaseTestConfigError):
            validate_release_test_collection([invalid_test])

        # Faulty file manager type
        invalid_test = test.copy()
        invalid_test["run"]["file_manager"] = "invalid"
        with self.assertRaises(ReleaseTestConfigError):
            validate_release_test_collection([invalid_test])

        # Faulty smoke test
        invalid_test = test.copy()
        del invalid_test["smoke_test"]["frequency"]
        with self.assertRaises(ReleaseTestConfigError):
            validate_release_test_collection([invalid_test])

    def testLoadAndValidateTestCollectionFile(self):
        read_and_validate_release_test_collection(self.test_collection_file)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
