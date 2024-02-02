from unittest import mock
import sys
import pytest
import tempfile
import os

from ci.ray_ci.upgrade_version import (
    list_java_files,
    get_current_version,
    upgrade_file_version,
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


@mock.patch("ci.ray_ci.upgrade_version.get_check_output")
def test_get_current_version(mock_check_output):
    mock_check_output.side_effect = [
        "3.0.0.dev0 a123456dc1d2egd345a6789f1e23d45b678c90ed",
        "2.2.0 a123456dc1d2egd345a6789f1e23d45b678c90ed",
    ]
    # Test when version is default
    assert get_current_version(tempfile.gettempdir()) == (
        "3.0.0.dev0",
        "2.0.0-SNAPSHOT",
    )
    # Test when version is different
    assert get_current_version(tempfile.gettempdir()) == (
        "2.2.0",
        "2.2.0",
    )


def _prepare_tmp_file(java: bool = False):
    file = tempfile.NamedTemporaryFile()
    with open(file.name, "w") as f:
        if java:
            f.write("<version>2.0.0-SNAPSHOT</version>")
        else:
            f.write("version: 2.2.0.dev0")
        f.flush()
    return file


def test_upgrade_file_version():
    main_version = "2.2.0.dev0"
    java_version = "2.0.0-SNAPSHOT"
    new_version = "1.1.1"

    # Create temporary files with default version.
    non_java_file_1 = _prepare_tmp_file()
    non_java_file_2 = _prepare_tmp_file()
    java_file_1 = _prepare_tmp_file(java=True)
    java_file_2 = _prepare_tmp_file(java=True)

    non_java_files = [non_java_file_1.name, non_java_file_2.name]
    java_files = [java_file_1.name, java_file_2.name]
    non_java_files.sort()
    java_files.sort()
    upgrade_file_version(
        non_java_files=non_java_files,
        java_files=java_files,
        main_version=main_version,
        java_version=java_version,
        new_version=new_version,
        root_dir=tempfile.gettempdir(),
    )

    for file in non_java_files:
        with open(file, "r") as f:
            assert f.read() == f"version: {new_version}"

    for file in java_files:
        with open(file, "r") as f:
            assert f.read() == f"<version>{new_version}</version>"


def test_upgrade_file_version_fail():
    main_version = "2.2.0.dev0"
    java_version = "2.0.0-SNAPSHOT"
    new_version = "1.1.1"

    non_java_files = []
    java_files = []
    non_java_files.sort()
    java_files.sort()

    with pytest.raises(AssertionError):
        upgrade_file_version(
            non_java_files=non_java_files,
            java_files=java_files,
            main_version=main_version,
            java_version=java_version,
            new_version=new_version,
            root_dir=tempfile.gettempdir(),
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
