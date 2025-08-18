import os
import sys
import tempfile
from unittest import mock

import pytest

from ci.ray_ci.automation.update_version_lib import (
    get_current_version,
    list_java_files,
    update_file_version,
)


def test_list_java_files():
    with tempfile.TemporaryDirectory() as tmp_dir:
        os.mkdir(os.path.join(tmp_dir, "subdir_0"))
        os.mkdir(os.path.join(tmp_dir, "subdir_1"))
        os.mkdir(os.path.join(tmp_dir, "subdir_0/subdir_0_0"))
        select_file_paths = [
            "pom_template.xml",
            "subdir_0/pom.xml",
            "subdir_0/pom_template.xml",
            "subdir_1/pom.xml",
            "subdir_1/pom_template.xml",
            "subdir_0/subdir_0_0/pom.xml",
        ]
        non_select_file_paths = [
            "not_pom.xml",
            "subdir_1/not_pom_template.xml",
            "subdir_0/subdir_0_0/not_pom_template.xml",
            "subdir_0/subdir_0_0/not_pom.xml",
        ]
        for file_path in select_file_paths + non_select_file_paths:
            with open(os.path.join(tmp_dir, file_path), "w") as f:
                f.write("")

        assert list_java_files(tmp_dir) == sorted(
            [os.path.join(tmp_dir, file_path) for file_path in select_file_paths]
        )


@mock.patch("ci.ray_ci.automation.update_version_lib.get_check_output")
def test_get_current_version_from_master_branch_version(mock_check_output):
    mock_check_output.return_value = (
        "3.0.0.dev0 a123456dc1d2egd345a6789f1e23d45b678c90ed"
    )
    assert get_current_version(tempfile.gettempdir()) == (
        "3.0.0.dev0",
        "2.0.0-SNAPSHOT",
    )


@mock.patch("ci.ray_ci.automation.update_version_lib.get_check_output")
def test_get_current_version_from_changed_version(mock_check_output):
    mock_check_output.return_value = "2.2.0 a123456dc1d2egd345a6789f1e23d45b678c90ed"

    assert get_current_version(tempfile.gettempdir()) == (
        "2.2.0",
        "2.2.0",
    )


def _prepare_file(file_path, version: str, java: bool = False):
    """
    Print to a file with version in the format of Java or non-Java files.
    """
    with open(file_path, "w") as f:
        if java:
            f.write(f"<version>{version}</version>")
        else:
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
        "subdir_0",
        "subdir_1",
        "subdir_0/subdir_0_0",
    ]
    for dir in directories:
        full_dir_path = os.path.join(tmp_dir, dir)
        os.mkdir(full_dir_path)


@pytest.mark.parametrize(
    ("main_version", "java_version", "new_version"),
    [
        ("3.0.0.dev0", "2.0.0-SNAPSHOT", "2.3.3"),
        ("2.3.2", "2.3.2", "2.3.3"),
    ],
)
def test_update_file_version(main_version, java_version, new_version):
    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_tmp_directories(tmp_dir)
        non_java_file_paths = [
            "ci/ray_ci/utils.py",
            "python/ray/_version.py",
            "src/ray/common/constants.h",
        ]
        select_java_file_paths = [
            "pom_template.xml",
            "subdir_0/pom.xml",
            "subdir_0/pom_template.xml",
            "subdir_1/pom.xml",
            "subdir_1/pom_template.xml",
            "subdir_0/subdir_0_0/pom.xml",
        ]
        non_select_java_file_paths = [
            "not_pom.xml",
            "subdir_1/not_pom_template.xml",
            "subdir_0/subdir_0_0/not_pom_template.xml",
            "subdir_0/subdir_0_0/not_pom.xml",
        ]
        for file_path in select_java_file_paths + non_select_java_file_paths:
            _prepare_file(
                os.path.join(tmp_dir, file_path), version=java_version, java=True
            )
        for file_path in non_java_file_paths:
            _prepare_file(
                os.path.join(tmp_dir, file_path), version=main_version, java=False
            )

        update_file_version(
            main_version=main_version,
            java_version=java_version,
            new_version=new_version,
            root_dir=tmp_dir,
        )

        for file_path in non_java_file_paths:
            with open(os.path.join(tmp_dir, file_path), "r") as f:
                assert f.read() == f"version: {new_version}"

        for file_path in select_java_file_paths:
            with open(os.path.join(tmp_dir, file_path), "r") as f:
                assert f.read() == f"<version>{new_version}</version>"

        for file_path in non_select_java_file_paths:
            with open(os.path.join(tmp_dir, file_path), "r") as f:
                assert f.read() == f"<version>{java_version}</version>"


def test_update_file_version_fail_no_non_java_file():
    """
    Test for failure when there's no file to be found.
    """
    main_version = "3.0.0.dev0"
    java_version = "2.0.0-SNAPSHOT"
    new_version = "2.3.3"

    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_tmp_directories(tmp_dir)
        select_java_file_paths = [
            "pom_template.xml",
            "subdir_0/pom.xml",
            "subdir_0/pom_template.xml",
            "subdir_1/pom.xml",
            "subdir_1/pom_template.xml",
            "subdir_0/subdir_0_0/pom.xml",
        ]
        for file_path in select_java_file_paths:
            _prepare_file(
                os.path.join(tmp_dir, file_path), version=java_version, java=True
            )
        with pytest.raises(ValueError):
            update_file_version(
                main_version=main_version,
                java_version=java_version,
                new_version=new_version,
                root_dir=tmp_dir,
            )


def test_update_file_version_fail_no_java_file():
    """
    Test for failure when there's no java file to be found.
    """
    main_version = "3.0.0.dev0"
    java_version = "2.0.0-SNAPSHOT"
    new_version = "2.3.3"

    with tempfile.TemporaryDirectory() as tmp_dir:
        _make_tmp_directories(tmp_dir)
        non_java_file_paths = [
            "ci/ray_ci/utils.py",
            "python/ray/_version.py",
            "src/ray/common/constants.h",
        ]
        for file_path in non_java_file_paths:
            _prepare_file(
                os.path.join(tmp_dir, file_path), version=main_version, java=False
            )

        with pytest.raises(AssertionError):
            update_file_version(
                main_version=main_version,
                java_version=java_version,
                new_version=new_version,
                root_dir=tmp_dir,
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
