import os
import sys
import tempfile
import unittest
from typing import Dict
from unittest.mock import patch

import yaml

from ray_release.buildkite.concurrency import (
    get_test_resources_from_cluster_compute,
    get_concurrency_group,
    CONCURRENY_GROUPS,
)
from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import (
    split_ray_repo_str,
    get_default_settings,
    update_settings_from_environment,
    Frequency,
    update_settings_from_buildkite,
    Priority,
    get_test_attr_regex_filters,
)
from ray_release.buildkite.step import (
    get_step,
    RELEASE_QUEUE_DEFAULT,
    RELEASE_QUEUE_CLIENT,
)
from ray_release.config import Test
from ray_release.exception import ReleaseTestConfigError
from ray_release.tests.test_glue import MockReturn
from ray_release.wheels import (
    DEFAULT_BRANCH,
)


class MockBuildkiteAgent:
    def __init__(self, return_dict: Dict):
        self.return_dict = return_dict

    def __call__(self, key: str):
        return self.return_dict.get(key, None)


class MockBuildkitePythonAPI(MockReturn):
    def builds(self):
        return self

    def artifacts(self):
        return self


class BuildkiteSettingsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.buildkite = {}
        self.buildkite_mock = MockBuildkiteAgent(self.buildkite)

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

    def testGetTestAttrRegexFilters(self):
        test_attr_regex_filters = get_test_attr_regex_filters("")
        self.assertDictEqual(test_attr_regex_filters, {})

        test_attr_regex_filters = get_test_attr_regex_filters("name:xxx")
        self.assertDictEqual(test_attr_regex_filters, {"name": "xxx"})

        test_attr_regex_filters = get_test_attr_regex_filters("name:xxx\n")
        self.assertDictEqual(test_attr_regex_filters, {"name": "xxx"})

        test_attr_regex_filters = get_test_attr_regex_filters("name:xxx\n\nteam:yyy")
        self.assertDictEqual(test_attr_regex_filters, {"name": "xxx", "team": "yyy"})

        test_attr_regex_filters = get_test_attr_regex_filters("name:xxx\n \nteam:yyy\n")
        self.assertDictEqual(test_attr_regex_filters, {"name": "xxx", "team": "yyy"})

        with self.assertRaises(ReleaseTestConfigError):
            get_test_attr_regex_filters("xxx")

    def testSettingsOverrideEnv(self):
        settings = get_default_settings()

        # With no environment variables, default settings shouldn't be updated
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertDictEqual(settings, updated_settings)

        environ = os.environ.copy()

        # Invalid frequency
        os.environ.clear()
        os.environ.update(environ)
        os.environ["RELEASE_FREQUENCY"] = "invalid"
        updated_settings = settings.copy()
        with self.assertRaises(ReleaseTestConfigError):
            update_settings_from_environment(updated_settings)

        # Invalid priority
        os.environ.clear()
        os.environ.update(environ)
        os.environ["RELEASE_PRIORITY"] = "invalid"
        updated_settings = settings.copy()
        with self.assertRaises(ReleaseTestConfigError):
            update_settings_from_environment(updated_settings)

        # Invalid test attr regex filters
        os.environ.clear()
        os.environ.update(environ)
        os.environ["TEST_ATTR_REGEX_FILTERS"] = "xxxx"
        updated_settings = settings.copy()
        with self.assertRaises(ReleaseTestConfigError):
            update_settings_from_environment(updated_settings)

        os.environ.clear()
        os.environ.update(environ)
        os.environ["TEST_ATTR_REGEX_FILTERS"] = "name:xxx\nteam:yyy\n"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)
        self.assertDictEqual(
            updated_settings["test_attr_regex_filters"],
            {
                "name": "xxx",
                "team": "yyy",
            },
        )

        os.environ.clear()
        os.environ.update(environ)
        os.environ["RELEASE_FREQUENCY"] = "nightly"
        os.environ["RAY_TEST_REPO"] = "https://github.com/user/ray.git"
        os.environ["RAY_TEST_BRANCH"] = "sub/branch"
        os.environ["RAY_WHEELS"] = "custom-wheels"
        os.environ["TEST_NAME"] = "name_filter"
        os.environ["RELEASE_PRIORITY"] = "manual"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertDictEqual(
            updated_settings,
            {
                "frequency": Frequency.NIGHTLY,
                "prefer_smoke_tests": False,
                "test_attr_regex_filters": {"name": "name_filter"},
                "ray_wheels": "custom-wheels",
                "ray_test_repo": "https://github.com/user/ray.git",
                "ray_test_branch": "sub/branch",
                "priority": Priority.MANUAL,
                "no_concurrency_limit": False,
            },
        )

        os.environ["RELEASE_FREQUENCY"] = "any-smoke"
        update_settings_from_environment(updated_settings)
        self.assertDictEqual(
            updated_settings,
            {
                "frequency": Frequency.ANY,
                "prefer_smoke_tests": True,
                "test_attr_regex_filters": {"name": "name_filter"},
                "ray_wheels": "custom-wheels",
                "ray_test_repo": "https://github.com/user/ray.git",
                "ray_test_branch": "sub/branch",
                "priority": Priority.MANUAL,
                "no_concurrency_limit": False,
            },
        )

        ###
        # Buildkite PR build settings

        # Default PR
        os.environ.clear()
        os.environ.update(environ)
        os.environ["BUILDKITE_REPO"] = "https://github.com/ray-project/ray.git"
        os.environ[
            "BUILDKITE_PULL_REQUEST_REPO"
        ] = "https://github.com/user/ray-fork.git"
        os.environ["BUILDKITE_BRANCH"] = "user:some_branch"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertEqual(
            updated_settings["ray_test_repo"], "https://github.com/user/ray-fork.git"
        )
        self.assertEqual(updated_settings["ray_test_branch"], "some_branch")

        # PR without prefix
        os.environ.clear()
        os.environ.update(environ)
        os.environ["BUILDKITE_REPO"] = "https://github.com/ray-project/ray.git"
        os.environ["BUILDKITE_PULL_REQUEST_REPO"] = "git://github.com/user/ray-fork.git"
        os.environ["BUILDKITE_BRANCH"] = "some_branch"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertEqual(
            updated_settings["ray_test_repo"], "https://github.com/user/ray-fork.git"
        )
        self.assertEqual(updated_settings["ray_test_branch"], "some_branch")

        # Branch build but pointing to fork
        os.environ.clear()
        os.environ.update(environ)
        os.environ["BUILDKITE_REPO"] = "https://github.com/ray-project/ray.git"
        os.environ["BUILDKITE_BRANCH"] = "user:some_branch"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertEqual(
            updated_settings["ray_test_repo"], "https://github.com/user/ray.git"
        )
        self.assertEqual(updated_settings["ray_test_branch"], "some_branch")

        # Branch build but pointing to main repo branch
        os.environ.clear()
        os.environ.update(environ)
        os.environ["BUILDKITE_REPO"] = "https://github.com/ray-project/ray.git"
        os.environ["BUILDKITE_BRANCH"] = "some_branch"
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertEqual(
            updated_settings["ray_test_repo"], "https://github.com/ray-project/ray.git"
        )
        self.assertEqual(updated_settings["ray_test_branch"], "some_branch")

        # Empty BUILDKITE_PULL_REQUEST_REPO
        os.environ.clear()
        os.environ.update(environ)
        os.environ["BUILDKITE_REPO"] = "https://github.com/ray-project/ray.git"
        os.environ["BUILDKITE_BRANCH"] = "some_branch"
        os.environ["BUILDKITE_PULL_REQUEST_REPO"] = ""
        updated_settings = settings.copy()
        update_settings_from_environment(updated_settings)

        self.assertEqual(
            updated_settings["ray_test_repo"], "https://github.com/ray-project/ray.git"
        )
        self.assertEqual(updated_settings["ray_test_branch"], "some_branch")

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

            buildkite = self.buildkite.copy()

            # Invalid frequency
            self.buildkite.clear()
            self.buildkite.update(buildkite)
            self.buildkite["release-frequency"] = "invalid"
            updated_settings = settings.copy()
            with self.assertRaises(ReleaseTestConfigError):
                update_settings_from_buildkite(updated_settings)

            # Invalid priority
            self.buildkite.clear()
            self.buildkite.update(buildkite)
            self.buildkite["release-priority"] = "invalid"
            updated_settings = settings.copy()
            with self.assertRaises(ReleaseTestConfigError):
                update_settings_from_buildkite(updated_settings)

            # Invalid test attr regex filters
            self.buildkite.clear()
            self.buildkite.update(buildkite)
            self.buildkite["release-test-attr-regex-filters"] = "xxxx"
            updated_settings = settings.copy()
            with self.assertRaises(ReleaseTestConfigError):
                update_settings_from_buildkite(updated_settings)

            self.buildkite.clear()
            self.buildkite.update(buildkite)
            self.buildkite["release-test-attr-regex-filters"] = "name:xxx\ngroup:yyy"
            updated_settings = settings.copy()
            update_settings_from_buildkite(updated_settings)
            self.assertDictEqual(
                updated_settings["test_attr_regex_filters"],
                {
                    "name": "xxx",
                    "group": "yyy",
                },
            )

            self.buildkite.clear()
            self.buildkite.update(buildkite)
            self.buildkite["release-frequency"] = "nightly"
            self.buildkite["release-ray-test-repo-branch"] = "user:sub/branch"
            self.buildkite["release-ray-wheels"] = "custom-wheels"
            self.buildkite["release-test-name"] = "name_filter"
            self.buildkite["release-priority"] = "manual"
            updated_settings = settings.copy()
            update_settings_from_buildkite(updated_settings)

            self.assertDictEqual(
                updated_settings,
                {
                    "frequency": Frequency.NIGHTLY,
                    "prefer_smoke_tests": False,
                    "test_attr_regex_filters": {"name": "name_filter"},
                    "ray_wheels": "custom-wheels",
                    "ray_test_repo": "https://github.com/user/ray.git",
                    "ray_test_branch": "sub/branch",
                    "priority": Priority.MANUAL,
                    "no_concurrency_limit": False,
                },
            )

            self.buildkite["release-frequency"] = "any-smoke"
            update_settings_from_buildkite(updated_settings)

            self.assertDictEqual(
                updated_settings,
                {
                    "frequency": Frequency.ANY,
                    "prefer_smoke_tests": True,
                    "test_attr_regex_filters": {"name": "name_filter"},
                    "ray_wheels": "custom-wheels",
                    "ray_test_repo": "https://github.com/user/ray.git",
                    "ray_test_branch": "sub/branch",
                    "priority": Priority.MANUAL,
                    "no_concurrency_limit": False,
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
                    "team": "team_1",
                    "run": {"type": "job"},
                }
            ),
            Test(
                {
                    "name": "test_2",
                    "frequency": "weekly",
                    "smoke_test": {"frequency": "nightly"},
                    "team": "team_2",
                    "run": {"type": "client"},
                }
            ),
            Test({"name": "other_1", "frequency": "weekly", "team": "team_2"}),
            Test(
                {
                    "name": "other_2",
                    "frequency": "nightly",
                    "smoke_test": {"frequency": "multi"},
                    "team": "team_2",
                    "run": {"type": "job"},
                }
            ),
            Test({"name": "other_3", "frequency": "disabled", "team": "team_2"}),
            Test({"name": "test_3", "frequency": "nightly", "team": "team_2"}),
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

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.ANY,
            prefer_smoke_tests=True,
        )
        self.assertSequenceEqual(
            filtered,
            [
                ("test_1", True),
                ("test_2", True),
                ("other_1", False),
                ("other_2", True),
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

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.NIGHTLY,
            prefer_smoke_tests=True,
        )
        self.assertSequenceEqual(
            filtered,
            [
                ("test_1", True),
                ("test_2", True),
                ("other_2", True),
                ("test_3", False),
            ],
        )

        filtered = self._filter_names_smoke(tests, frequency=Frequency.WEEKLY)
        self.assertSequenceEqual(filtered, [("test_2", False), ("other_1", False)])

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.NIGHTLY,
            test_attr_regex_filters={"name": "other.*"},
        )
        self.assertSequenceEqual(
            filtered,
            [
                ("other_2", False),
            ],
        )

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.NIGHTLY,
            test_attr_regex_filters={"name": "test.*"},
        )
        self.assertSequenceEqual(
            filtered, [("test_1", False), ("test_2", True), ("test_3", False)]
        )

        filtered = self._filter_names_smoke(
            tests, frequency=Frequency.NIGHTLY, test_attr_regex_filters={"name": "test"}
        )
        self.assertSequenceEqual(filtered, [])

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.NIGHTLY,
            test_attr_regex_filters={"name": "test.*", "team": "team_1"},
        )
        self.assertSequenceEqual(filtered, [("test_1", False)])

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.NIGHTLY,
            test_attr_regex_filters={"name": "test_1|test_2"},
        )
        self.assertSequenceEqual(filtered, [("test_1", False), ("test_2", True)])

        # Filter by nested properties
        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.ANY,
            test_attr_regex_filters={"run/type": "job"},
        )
        self.assertSequenceEqual(filtered, [("test_1", False), ("other_2", False)])

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.ANY,
            test_attr_regex_filters={"run/type": "client"},
        )
        self.assertSequenceEqual(filtered, [("test_2", False)])

        filtered = self._filter_names_smoke(
            tests,
            frequency=Frequency.ANY,
            test_attr_regex_filters={"run/invalid": "xxx"},
        )
        self.assertSequenceEqual(filtered, [])

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

        step = get_step(test, priority_val=20)
        self.assertEqual(step["priority"], 20)

    def testInstanceResources(self):
        # AWS instances
        cpus, gpus = get_test_resources_from_cluster_compute(
            {
                "head_node_type": {"instance_type": "m5.4xlarge"},  # 16 CPUs, 0 GPUs
                "worker_node_types": [
                    {
                        "instance_type": "m5.8xlarge",  # 32 CPUS, 0 GPUs
                        "max_workers": 4,
                    },
                    {
                        "instance_type": "g3.8xlarge",  # 32 CPUs, 2 GPUs
                        "min_workers": 8,
                    },
                ],
            }
        )
        self.assertEqual(cpus, 16 + 32 * 4 + 32 * 8)
        self.assertEqual(gpus, 2 * 8)

        cpus, gpus = get_test_resources_from_cluster_compute(
            {
                "head_node_type": {
                    "instance_type": "n1-standard-16"  # 16 CPUs, 0 GPUs
                },
                "worker_node_types": [
                    {
                        "instance_type": "random-str-xxx-32",  # 32 CPUS, 0 GPUs
                        "max_workers": 4,
                    },
                    {
                        "instance_type": "a2-highgpu-2g",  # 24 CPUs, 2 GPUs
                        "min_workers": 8,
                    },
                ],
            }
        )
        self.assertEqual(cpus, 16 + 32 * 4 + 24 * 8)
        self.assertEqual(gpus, 2 * 8)

    def testConcurrencyGroups(self):
        def _return(ret):
            def _inner(*args, **kwargs):
                return ret

            return _inner

        test = Test(
            {
                "name": "test_1",
            }
        )

        def test_concurrency(cpu, gpu, group):
            with patch(
                "ray_release.buildkite.concurrency.get_test_resources",
                _return((cpu, gpu)),
            ):
                group_name, limit = get_concurrency_group(test)
                self.assertEqual(group_name, group)
                self.assertEqual(limit, CONCURRENY_GROUPS[group_name])

        test_concurrency(12800, 9, "large-gpu")
        test_concurrency(12800, 8, "small-gpu")
        test_concurrency(12800, 1, "small-gpu")
        test_concurrency(12800, 0, "enormous")
        test_concurrency(1025, 0, "enormous")
        test_concurrency(1024, 0, "large")
        test_concurrency(513, 0, "large")
        test_concurrency(512, 0, "medium")
        test_concurrency(129, 0, "medium")
        test_concurrency(128, 0, "small")
        test_concurrency(1, 0, "tiny")
        test_concurrency(32, 0, "tiny")
        test_concurrency(33, 0, "small")

    def testConcurrencyGroupSmokeTest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cluster_config_full = {
                "head_node_type": {
                    "instance_type": "n1-standard-16"  # 16 CPUs, 0 GPUs
                },
                "worker_node_types": [
                    {
                        "instance_type": "random-str-xxx-32",  # 32 CPUS, 0 GPUs
                        "max_workers": 10,
                    },
                ],
            }

            cluster_config_smoke = {
                "head_node_type": {
                    "instance_type": "n1-standard-16"  # 16 CPUs, 0 GPUs
                },
                "worker_node_types": [
                    {
                        "instance_type": "random-str-xxx-32",  # 32 CPUS, 0 GPUs
                        "max_workers": 1,
                    },
                ],
            }

            cluster_config_full_path = os.path.join(tmpdir, "full.yaml")
            with open(cluster_config_full_path, "w") as fp:
                yaml.safe_dump(cluster_config_full, fp)

            cluster_config_smoke_path = os.path.join(tmpdir, "smoke.yaml")
            with open(cluster_config_smoke_path, "w") as fp:
                yaml.safe_dump(cluster_config_smoke, fp)

            test = Test(
                {
                    "name": "test_1",
                    "cluster": {"cluster_compute": cluster_config_full_path},
                    "smoke_test": {
                        "cluster": {"cluster_compute": cluster_config_smoke_path},
                    },
                }
            )
            step = get_step(test, smoke_test=False)
            self.assertEquals(step["concurrency_group"], "medium")

            step = get_step(test, smoke_test=True)
            self.assertEquals(step["concurrency_group"], "small")

    def testStepQueueClient(self):
        test_regular = Test(
            {
                "name": "test",
                "frequency": "nightly",
                "run": {"script": "test_script.py"},
            }
        )
        test_client = Test(
            {
                "name": "test",
                "frequency": "nightly",
                "run": {"script": "test_script.py", "type": "client"},
            }
        )

        step = get_step(test_regular)
        self.assertEqual(step["agents"]["queue"], str(RELEASE_QUEUE_DEFAULT))

        step = get_step(test_client)
        self.assertEqual(step["agents"]["queue"], str(RELEASE_QUEUE_CLIENT))


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
