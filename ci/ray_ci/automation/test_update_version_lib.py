import os
import sys
import tempfile
from unittest import mock

import pytest

from ci.ray_ci.automation.update_version_lib import (
    get_current_version,
    update_file_version,
)


@mock.patch("ci.ray_ci.automation.update_version_lib.get_check_output")
def test_get_current_version_from_master_branch_version(mock_check_output):
    mock_check_output.return_value = (
        "3.0.0.dev0 a123456dc1d2egd345a6789f1e23d45b678c90ed"
    )
    assert get_current_version(tempfile.gettempdir()) == "3.0.0.dev0"


@mock.patch("ci.ray_ci.automation.update_version_lib.get_check_output")
def test_get_current_version_from_changed_version(mock_check_output):
    mock_check_output.return_value = "2.2.0 a123456dc1d2egd345a6789f1e23d45b678c90ed"

    assert get_current_version(tempfile.gettempdir()) == "2.2.0"


def _prepare_file(file_path, version: str):
    """
    Print to a file with the given version.
    """
    with open(file_path, "w") as f:
        f.write(f"version: {version}")
        f.flush()


def _make_tmp_directories(tmp_dir):
    directories = [
        "ci",
        "ci/ray_ci",
        "python",
        "python/ray",
        "src",
        "src/ray",
        "src/ray/common",
    ]
    for dir in directories:
        full_dir_path = os.path.join(tmp_dir, dir)
        os.mkdir(full_dir_path)


@pytest.mark.parametrize(
    ("main_version", "new_version"),
    [
        ("3.0.0.dev0", "2.3.3"),
        ("2.3.2", "2.3.3"),
    ],
)
def test_update_file_version(main_version, new_version):
    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_tmp_directories(tmp_dir)
        file_paths = [
            "ci/ray_ci/utils.py",
            "python/ray/_version.py",
            "rayci.env",
            "src/ray/common/constants.h",
        ]
        for file_path in file_paths:
            _prepare_file(os.path.join(tmp_dir, file_path), version=main_version)

        update_file_version(
            main_version=main_version,
            new_version=new_version,
            root_dir=tmp_dir,
        )

        for file_path in file_paths:
            with open(os.path.join(tmp_dir, file_path), "r") as f:
                assert f.read() == f"version: {new_version}"


def test_update_file_version_fail_no_file():
    """
    Test for failure when there's no file to be found.
    """
    main_version = "3.0.0.dev0"
    new_version = "2.3.3"

    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_tmp_directories(tmp_dir)
        with pytest.raises(ValueError):
            update_file_version(
                main_version=main_version,
                new_version=new_version,
                root_dir=tmp_dir,
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
