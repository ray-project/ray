import os
import time
import unittest
from unittest.mock import patch

from freezegun import freeze_time

from ray_release.exception import RayWheelsNotFoundError, RayWheelsTimeoutError
from ray_release.wheels import (
    get_ray_version,
    DEFAULT_REPO,
    get_ray_wheels_url,
    find_ray_wheels_url,
    find_and_wait_for_ray_wheels_url,
)


class WheelsFinderTest(unittest.TestCase):
    def setUp(self) -> None:
        pass

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
        with self.assertRaises(RayWheelsNotFoundError):
            get_ray_wheels_url(
                repo_url="https://github.com/unknown/ray.git",
                branch="master",
                commit="1234",
                ray_version="2.0.0.dev0",
            )

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

        with self.assertRaises(RayWheelsNotFoundError):
            self._testFindRayWheelsCheckout(
                repo, branch, commit, version, search_str="remote:master"
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
