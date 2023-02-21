import os
import sys
import time
import pytest
from unittest.mock import patch

from freezegun import freeze_time

from ray_release.config import Test
from ray_release.template import load_test_cluster_env
from ray_release.exception import RayWheelsNotFoundError, RayWheelsTimeoutError
from ray_release.util import url_exists
from ray_release.wheels import (
    get_ray_version,
    DEFAULT_REPO,
    get_ray_wheels_url,
    find_ray_wheels_url,
    find_and_wait_for_ray_wheels_url,
    is_wheels_url_matching_ray_verison,
    get_wheels_filename,
    maybe_rewrite_wheels_url,
)


@pytest.fixture
def remove_buildkite_env():
    for key in os.environ:
        if key.startswith("BUILDKITE"):
            os.environ.pop(key)


def test_get_ray_version(remove_buildkite_env):
    init_file = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "python", "ray", "__init__.py"
    )
    with open(init_file, "rt") as fp:
        content = [line.encode() for line in fp.readlines()]

    with patch("urllib.request.urlopen", lambda _: content):
        version = get_ray_version(DEFAULT_REPO, commit="fake")
        assert version

    with patch("urllib.request.urlopen", lambda _: []), pytest.raises(
        RayWheelsNotFoundError
    ):
        get_ray_version(DEFAULT_REPO, commit="fake")


def test_get_ray_wheels_url(remove_buildkite_env):
    url = get_ray_wheels_url(
        repo_url="https://github.com/ray-project/ray.git",
        branch="master",
        commit="1234",
        ray_version="3.0.0.dev0",
    )
    assert (
        url == "https://s3-us-west-2.amazonaws.com/ray-wheels/master/1234/"
        "ray-3.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl"
    )


@patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "3.0.0.dev0")
def test_find_ray_wheels_buildkite(remove_buildkite_env):
    repo = DEFAULT_REPO
    branch = "master"
    commit = "1234" * 10
    version = "3.0.0.dev0"

    os.environ["BUILDKITE_COMMIT"] = commit

    url = find_ray_wheels_url()

    assert url == get_ray_wheels_url(repo, branch, commit, version)

    branch = "branched"
    os.environ["BUILDKITE_BRANCH"] = branch

    url = find_ray_wheels_url()

    assert url == get_ray_wheels_url(repo, branch, commit, version)


@patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "3.0.0.dev0")
def test_find_ray_wheels_commit_only(remove_buildkite_env):
    repo = DEFAULT_REPO
    branch = "master"
    commit = "1234" * 10
    version = "3.0.0.dev0"

    search_str = commit

    url = find_ray_wheels_url(search_str)

    assert url == get_ray_wheels_url(repo, branch, commit, version)


def _test_find_ray_wheels_checkout(
    repo: str, branch: str, commit: str, version: str, search_str: str
):
    with patch(
        "ray_release.wheels.get_latest_commits", lambda *a, **kw: [commit]
    ), patch("ray_release.wheels.url_exists", lambda *a, **kw: False), pytest.raises(
        RayWheelsNotFoundError
    ):
        # Fails because URL does not exist
        find_ray_wheels_url(search_str)

    with patch(
        "ray_release.wheels.get_latest_commits", lambda *a, **kw: [commit]
    ), patch("ray_release.wheels.url_exists", lambda *a, **kw: True):
        # Succeeds
        url = find_ray_wheels_url(search_str)

        assert url == get_ray_wheels_url(repo, branch, commit, version)


@patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "3.0.0.dev0")
def test_find_ray_wheels_branch(remove_buildkite_env):
    repo = DEFAULT_REPO
    branch = "master"
    commit = "1234" * 10
    version = "3.0.0.dev0"

    search_str = "master"

    _test_find_ray_wheels_checkout(repo, branch, commit, version, search_str)


@patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "3.0.0.dev0")
def test_find_ray_wheels_repo_branch(remove_buildkite_env):
    repo = DEFAULT_REPO
    branch = "master"
    commit = "1234" * 10
    version = "3.0.0.dev0"

    search_str = "ray-project:master"

    _test_find_ray_wheels_checkout(repo, branch, commit, version, search_str)


@patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "3.0.0.dev0")
def test_find_ray_wheels_pr_repo_branch(remove_buildkite_env):
    repo = "user"
    branch = "dev-branch"
    commit = "1234" * 10
    version = "3.0.0.dev0"

    search_str = "user:dev-branch"
    _test_find_ray_wheels_checkout(repo, branch, commit, version, search_str)

    search_str = "user:dev-branch"
    _test_find_ray_wheels_checkout(
        f"https://github.com/{repo}/ray-fork.git", branch, commit, version, search_str
    )


@patch("time.sleep", lambda *a, **kw: None)
@patch("ray_release.wheels.get_ray_version", lambda *a, **kw: "3.0.0.dev0")
def test_find_and_wait_wheels(remove_buildkite_env):
    repo = DEFAULT_REPO
    branch = "master"
    commit = "1234" * 10
    version = "3.0.0.dev0"

    class TrueAfter:
        def __init__(self, after: float):
            self.available_at = time.monotonic() + after

        def __call__(self, *args, **kwargs):
            if time.monotonic() > self.available_at:
                return True
            return False

    with freeze_time(auto_tick_seconds=10):
        with patch("ray_release.wheels.url_exists", TrueAfter(400)):
            with pytest.raises(RayWheelsTimeoutError):
                find_and_wait_for_ray_wheels_url(commit, timeout=300.0)

    with freeze_time(auto_tick_seconds=10):
        with patch("ray_release.wheels.url_exists", TrueAfter(200)):
            url = find_and_wait_for_ray_wheels_url(commit, timeout=300.0)

        assert url == get_ray_wheels_url(repo, branch, commit, version)


def test_matching_ray_wheels_url():
    assert not is_wheels_url_matching_ray_verison(
        f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 8))}", (3, 7)
    )

    assert not is_wheels_url_matching_ray_verison(
        f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 7))}", (3, 8)
    )

    assert is_wheels_url_matching_ray_verison(
        f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 7))}", (3, 7)
    )


@patch("ray_release.wheels.resolve_url", lambda url: url)
def test_rewrite_wheels_url(remove_buildkite_env):
    # Do not rewrite if versions match
    assert (
        maybe_rewrite_wheels_url(
            f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 7))}",
            (3, 7),
        )
        == f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 7))}"
    )

    # Do not rewrite if version can't be parsed
    assert (
        maybe_rewrite_wheels_url("http://some/location/unknown.whl", (3, 7))
        == "http://some/location/unknown.whl"
    )

    # Rewrite when version can be parsed
    assert (
        maybe_rewrite_wheels_url(
            f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 8))}",
            (3, 7),
        )
        == f"http://some/location/{get_wheels_filename('3.0.0dev0', (3, 7))}"
    )


def test_wheels_sanity_string(remove_buildkite_env):
    this_env = {"env": None}

    def override_env(path, env):
        this_env["env"] = env

    with patch(
        "ray_release.template.load_and_render_yaml_template", override_env
    ), patch("ray_release.template.get_test_environment", lambda: {}):
        load_test_cluster_env(
            Test(cluster=dict(cluster_env="invalid")),
            ray_wheels_url="https://no-commit-url",
        )
        assert "No commit sanity check" in this_env["env"]["RAY_WHEELS_SANITY_CHECK"]

        sha = "abcdef1234abcdef1234abcdef1234abcdef1234"
        load_test_cluster_env(
            Test(cluster=dict(cluster_env="invalid")),
            ray_wheels_url=f"https://some/{sha}/binary.whl",
        )
        assert sha in this_env["env"]["RAY_WHEELS_SANITY_CHECK"]


def test_url_exist():
    assert url_exists("https://github.com/")
    assert not url_exists("invalid://somewhere")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
