import os
import sys
import pytest
from unittest.mock import patch

from ray_release.bazel import bazel_runfile
from ray_release.exception import RayWheelsNotFoundError
from ray_release.util import url_exists
from ray_release.wheels import (
    get_ray_version,
    DEFAULT_REPO,
    get_ray_wheels_url,
    is_wheels_url_matching_ray_verison,
    get_wheels_filename,
    maybe_rewrite_wheels_url,
    parse_commit_from_wheel_url,
)


@pytest.fixture
def remove_buildkite_env():
    for key in os.environ:
        if key.startswith("BUILDKITE"):
            os.environ.pop(key)


def test_get_ray_version(remove_buildkite_env):
    init_file = bazel_runfile("python/ray/__init__.py")
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
        "ray-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl"
    )


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


def test_url_exist():
    assert url_exists("https://github.com/")
    assert not url_exists("invalid://somewhere")


def test_parse_commit_from_wheel_url():
    url = (
        "https://s3-us-west-2.amazonaws.com/ray-wheels/master/"
        "0e0c15065507f01e8bfe78e49b0d0de063f81164/"
        "ray-3.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl"
    )
    expected_commit = "0e0c15065507f01e8bfe78e49b0d0de063f81164"
    assert parse_commit_from_wheel_url(url) == expected_commit


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
