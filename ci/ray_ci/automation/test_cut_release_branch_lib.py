import sys

import pytest

from ci.ray_ci.automation.cut_release_branch_lib import (
    get_commit_message,
    get_release_branch_name,
    get_update_version_command,
    validate_version,
)


@pytest.mark.parametrize("version", ["2.59.0", "3.0.0", "10.20.30"])
def test_validate_version_accepts_release_versions(version):
    validate_version(version)


@pytest.mark.parametrize(
    "version",
    ["2.59", "2.59.0rc0", "2.59.0.dev0", "v2.59.0", "", "releases/2.59.0"],
)
def test_validate_version_rejects_non_release_versions(version):
    with pytest.raises(ValueError):
        validate_version(version)


def test_get_release_branch_name():
    assert get_release_branch_name("2.59.0") == "releases/2.59.0"


def test_get_release_branch_name_validates():
    with pytest.raises(ValueError):
        get_release_branch_name("2.59.0rc0")


def test_get_update_version_command():
    assert get_update_version_command("2.59.0") == [
        "bazel",
        "run",
        "//ci/ray_ci/automation:update_version",
        "--",
        "--new_version=2.59.0",
    ]


def test_get_commit_message():
    assert (
        get_commit_message("2.59.0", "abc123")
        == "[release] Cut releases/2.59.0 from abc123"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
