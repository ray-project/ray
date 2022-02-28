import os
import unittest
from typing import Dict
from unittest.mock import patch

from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import (
    split_ray_repo_str,
    get_default_settings,
    update_settings_from_environment,
    Frequency,
    update_settings_from_buildkite,
)
from ray_release.buildkite.step import get_step
from ray_release.config import Test
from ray_release.exception import ReleaseTestConfigError
from ray_release.wheels import (
    DEFAULT_BRANCH,
)


class MockBuildkite:
    def __init__(self, return_dict: Dict):
        self.return_dict = return_dict

    def __call__(self, key: str):
        return self.return_dict.get(key, None)


class BuildkiteSettingsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.buildkite = {}
        self.buildkite_mock = MockBuildkite(self.buildkite)

    def testSplitRayRepoStr(self):
        url, branch = split_ray_repo_str("https://github.com/ray-project/ray.git")
        self.assertEqual(url, "https://github.com/ray-project/ray.git")
        self.assertEqual(branch, DEFAULT_BRANCH)

        url, branch = split_ray_repo_str(
            "https://github.com/ray-project/ray/tree/branch/sub"
        )
        self.assertEqual(url, "https://github.com/ray-project/ray.git")
        self.assertEqual(branch, "branch/sub")

        url, branch = split_ray_repo_str("https://github.com/user/ray/tree/branch/sub")
        self.assertEqual(url, "https://github.com/user/ray.git")
        self.assertEqual(branch, "branch/sub")

        url, branch = split_ray_repo_str("ray-project:branch/sub")
        self.assertEqual(url, "https://github.com/ray-project/ray.git")
        self.assertEqual(branch, "branch/sub")

        url, branch = split_ray_repo_str("user:branch/sub")
        self.assertEqual(url, "https://github.com/user/ray.git")
        self.assertEqual(branch, "branch/sub")

        url, branch = split_ray_repo_str("user")
        self.assertEqual(url, "https://github.com/user/ray.git")
        self.assertEqual(branch, DEFAULT_BRANCH)

    def testSettingsOverrideEnv(self):
        settings = get_default_settings()

        # With no environment variables, default settings shouldn't be updated
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertDictEqual(settings, updated_settings)

        # Invalid frequency
        os.environ["RELEASE_FREQUENCY"] = "invalid"
        updated_settings = settings.copy()
        with self.assertRaises(ReleaseTestConfigError):
            update_settings_from_environment(updated_settings)

        os.environ["RELEASE_FREQUENCY"] = "nightly"
        os.environ["RAY_TEST_REPO"] = "https://github.com/user/ray.git"
        os.environ["RAY_TEST_BRANCH"] = "sub/branch"
        os.environ["RAY_WHEELS"] = "custom-wheels"
        os.environ["TEST_NAME"] = "name_filter"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertDictEqual(
            updated_settings,
            {
                "frequency": Frequency.NIGHTLY,
                "test_name_filter": "name_filter",
                "ray_wheels": "custom-wheels",
                "ray_test_repo": "https://github.com/user/ray.git",
                "ray_test_branch": "sub/branch",
            },
        )

    def testSettingsOverrideBuildkite(self):
        settings = get_default_settings()

        with patch(
            "ray_release.buildkite.settings.get_buildkite_prompt_value",
            self.buildkite_mock,
        ):

            # With no buildkite variables, default settings shouldn't be updated
            updated_settings = settings.copy()
            update_settings_from_buildkite(updated_settings)

            self.assertDictEqual(settings, updated_settings)

            # Invalid frequency
            self.buildkite["release-frequency"] = "invalid"
            updated_settings = settings.copy()
            with self.assertRaises(ReleaseTestConfigError):
                update_settings_from_buildkite(updated_settings)

            self.buildkite["release-frequency"] = "nightly"
            self.buildkite["release-ray-test-repo-branch"] = "user:sub/branch"
            self.buildkite["release-ray-wheels"] = "custom-wheels"
            self.buildkite["release-test-name"] = "name_filter"
            updated_settings = settings.copy()
            update_settings_from_buildkite(updated_settings)

            self.assertDictEqual(
                updated_settings,
                {
                    "frequency": Frequency.NIGHTLY,
                    "test_name_filter": "name_filter",
                    "ray_wheels": "custom-wheels",
                    "ray_test_repo": "https://github.com/user/ray.git",
                    "ray_test_branch": "sub/branch",
                },
            )

    def _filter_names_smoke(self, *args, **kwargs):
        filtered = filter_tests(*args, **kwargs)
        return [(t[0]["name"], t[1]) for t in filtered]

    def testFilterTests(self):
        tests = [
            Test(
                {
                    "name": "test_1",
                    "frequency": "nightly",
                    "smoke_test": {"frequency": "nightly"},
                }
            ),
            Test(
                {
                    "name": "test_2",
                    "frequency": "weekly",
                    "smoke_test": {"frequency": "nightly"},
                }
            ),
            Test({"name": "other_1", "frequency": "weekly"}),
            Test(
                {
                    "name": "other_2",
                    "frequency": "nightly",
                    "smoke_test": {"frequency": "multi"},
                }
            ),
            Test({"name": "other_3", "frequency": "disabled"}),
            Test({"name": "test_3", "frequency": "nightly"}),
        ]

        filtered = self._filter_names_smoke(tests, frequency=Frequency.ANY)
        self.assertSequenceEqual(
            filtered,
            [
                ("test_1", False),
                ("test_2", False),
                ("other_1", False),
                ("other_2", False),
                ("test_3", False),
            ],
        )

        filtered = self._filter_names_smoke(tests, frequency=Frequency.NIGHTLY)
        self.assertSequenceEqual(
            filtered,
            [
                ("test_1", False),
                ("test_2", True),
                ("other_2", False),
                ("test_3", False),
            ],
        )

        filtered = self._filter_names_smoke(tests, frequency=Frequency.WEEKLY)
        self.assertSequenceEqual(filtered, [("test_2", False), ("other_1", False)])

        filtered = self._filter_names_smoke(
            tests, frequency=Frequency.NIGHTLY, test_name_filter="other"
        )
        self.assertSequenceEqual(
            filtered,
            [
                ("other_2", False),
            ],
        )

        filtered = self._filter_names_smoke(
            tests, frequency=Frequency.NIGHTLY, test_name_filter="test"
        )
        self.assertSequenceEqual(
            filtered, [("test_1", False), ("test_2", True), ("test_3", False)]
        )

    def testGroupTests(self):
        tests = [
            (Test(name="x1", group="x"), False),
            (Test(name="x2", group="x"), False),
            (Test(name="y1", group="y"), False),
            (Test(name="ungrouped"), False),
            (Test(name="x3", group="x"), False),
        ]

        grouped = group_tests(tests)
        self.assertEqual(len(grouped), 3)  # Three groups
        self.assertEqual(len(grouped["x"]), 3)
        self.assertSequenceEqual(
            [t["name"] for t, _ in grouped["x"]], ["x1", "x2", "x3"]
        )
        self.assertEqual(len(grouped["y"]), 1)

    def testGetStep(self):
        test = Test(
            {
                "name": "test",
                "frequency": "nightly",
                "run": {"script": "test_script.py"},
                "smoke_test": {"frequency": "multi"},
            }
        )

        step = get_step(test, smoke_test=False)
        self.assertNotIn("--smoke-test", step["command"])

        step = get_step(test, smoke_test=True)
        self.assertIn("--smoke-test", step["command"])
