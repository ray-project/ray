import os
import time
import unittest
from unittest.mock import patch

from freezegun import freeze_time

from ray_release.exception import RayWheelsNotFoundError, RayWheelsTimeoutError
from ray_release.tests.test_buildkite import MockBuildkitePythonAPI
from ray_release.wheels import (
    get_ray_version,
    DEFAULT_REPO,
    get_ray_wheels_url,
    find_ray_wheels_url,
    find_and_wait_for_ray_wheels_url,
    get_wheels_filename,
)


class WheelsFinderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.buildkite_api = MockBuildkitePythonAPI()

    def testGetRayVersion(self):
        init_file = os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "python", "ray", "__init__.py"
        )
        with open(init_file, "rt") as fp:
            content = [line.encode() for line in fp.readlines()]

        with patch("urllib.request.urlopen", lambda _: content):
            version = get_ray_version(DEFAULT_REPO, commit="fake")
            self.assertEqual(version, "2.0.0.dev0")

        with patch("urllib.request.urlopen", lambda _: []), self.assertRaises(
            RayWheelsNotFoundError
        ):
            get_ray_version(DEFAULT_REPO, commit="fake")

    def testGetRayWheelsURL(self):
        url = get_ray_wheels_url(
            repo_url="https://github.com/ray-project/ray.git",
            branch="master",
            commit="1234",
            ray_version="2.0.0.dev0",
        )
        self.assertEqual(
            url,
            "https://s3-us-west-2.amazonaws.com/ray-wheels/"
            "master/1234/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl",
        )

    @patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "2.0.0.dev0")
    def testFindRayWheelsBuildkite(self):
        repo = DEFAULT_REPO
        branch = "master"
        commit = "1234" * 10
        version = "2.0.0.dev0"

        os.environ["BUILDKITE_COMMIT"] = commit

        url = find_ray_wheels_url()

        self.assertEqual(url, get_ray_wheels_url(repo, branch, commit, version))

        branch = "branched"
        os.environ["BUILDKITE_BRANCH"] = branch

        url = find_ray_wheels_url()

        self.assertEqual(url, get_ray_wheels_url(repo, branch, commit, version))

    @patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "2.0.0.dev0")
    def testFindRayWheelsCommitOnly(self):
        os.environ.pop("BUILDKITE_BRANCH")

        repo = DEFAULT_REPO
        branch = "master"
        commit = "1234" * 10
        version = "2.0.0.dev0"

        search_str = commit

        url = find_ray_wheels_url(search_str)

        self.assertEqual(url, get_ray_wheels_url(repo, branch, commit, version))

    def _testFindRayWheelsCheckout(
        self, repo: str, branch: str, commit: str, version: str, search_str: str
    ):
        with patch(
            "ray_release.wheels.get_latest_commits", lambda *a, **kw: [commit]
        ), patch(
            "ray_release.wheels.url_exists", lambda *a, **kw: False
        ), self.assertRaises(
            RayWheelsNotFoundError
        ):
            # Fails because URL does not exist
            find_ray_wheels_url(search_str)

        with patch(
            "ray_release.wheels.get_latest_commits", lambda *a, **kw: [commit]
        ), patch("ray_release.wheels.url_exists", lambda *a, **kw: True):
            # Succeeds
            url = find_ray_wheels_url(search_str)

            self.assertEqual(url, get_ray_wheels_url(repo, branch, commit, version))

    @patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "2.0.0.dev0")
    def testFindRayWheelsBranch(self):
        repo = DEFAULT_REPO
        branch = "master"
        commit = "1234" * 10
        version = "2.0.0.dev0"

        self._testFindRayWheelsCheckout(
            repo, branch, commit, version, search_str="master"
        )

    @patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "2.0.0.dev0")
    def testFindRayWheelsRepoBranch(self):
        repo = DEFAULT_REPO
        branch = "master"
        commit = "1234" * 10
        version = "2.0.0.dev0"

        self._testFindRayWheelsCheckout(
            repo, branch, commit, version, search_str="ray-project:master"
        )

    @patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "2.0.0.dev0")
    def testFindRayWheelsPRRepoBranch(self):
        # Todo: Remove previous commit Buildkite token etc

        repo = "user"
        branch = "dev-branch"
        commit = "1234" * 10
        version = "2.0.0.dev0"

        self.buildkite_api.return_dict["list_all_for_pipeline"] = [
            {"commit": "9999" * 10, "number": 1},
            {"commit": commit, "number": 2},
        ]
        self.buildkite_api.return_dict["list_artifacts_for_build"] = [
            {"filename": "invalid", "download_url": "invalid"},
            {"filename": get_wheels_filename(version), "download_url": "invalid"},
        ]

        with patch(
            "ray_release.buildkite.wheels.get_buildkite", lambda: self.buildkite_api
        ):
            self._testFindRayWheelsCheckout(
                repo, branch, commit, version, search_str="user:dev-branch"
            )
            self._testFindRayWheelsCheckout(
                f"https://github.com/{repo}/ray-fork.git",
                branch,
                commit,
                version,
                search_str="user:dev-branch",
            )

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "2.0.0.dev0")
    def testFindAndWaitWheels(self):
        repo = DEFAULT_REPO
        branch = "master"
        commit = "1234" * 10
        version = "2.0.0.dev0"

        class TrueAfter:
            def __init__(self, after: float):
                self.available_at = time.monotonic() + after

            def __call__(self, *args, **kwargs):
                if time.monotonic() > self.available_at:
                    return True
                return False

        with freeze_time(auto_tick_seconds=10):
            with patch(
                "ray_release.wheels.url_exists", TrueAfter(400)
            ), self.assertRaises(RayWheelsTimeoutError):
                find_and_wait_for_ray_wheels_url(commit, timeout=300.0)

        with freeze_time(auto_tick_seconds=10):
            with patch("ray_release.wheels.url_exists", TrueAfter(200)):
                url = find_and_wait_for_ray_wheels_url(commit, timeout=300.0)

            self.assertEqual(url, get_ray_wheels_url(repo, branch, commit, version))
