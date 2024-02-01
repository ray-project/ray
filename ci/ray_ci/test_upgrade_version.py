from unittest import mock
import sys
import pytest
import tempfile

from ci.ray_ci.upgrade_version import list_java_files, get_current_version, upgrade_file_version
from ci.ray_ci.utils import logger

@mock.patch("os.walk")
def test_list_java_files(mock_os_walk):
    mock_os_walk.return_value = [
        ("root/dir", ["subdir_1", "subdir_2", "subdir_3"], ["pom_template.xml", "not_pom.xml"]),
        ("root/dir/subdir_1", [], ["pom.xml", "not_pom.xml", "not_pom_template.xml"]),
        ("root/dir/subdir_2", [], ["pom_template.xml", "pom.xml", "not_pom.py"]),
        ("root/dir/subdir_3", [], ["pom.xml", "pom_template.xml", "not_pom_template.py"]),
    ]
    assert list_java_files() == set([
        "root/dir/pom_template.xml",
        "root/dir/subdir_1/pom.xml",
        "root/dir/subdir_2/pom_template.xml",
        "root/dir/subdir_2/pom.xml",
        "root/dir/subdir_3/pom.xml",
        "root/dir/subdir_3/pom_template.xml",
    ])

@mock.patch("subprocess.run")
def test_get_current_version(mock_subprocess_run):
    mock_subprocess_run.return_value = mock.MagicMock(
        returncode=0,
        stdout="1.1.1.default",
    )
    # Test when version is default
    assert get_current_version("1.1.1.default", "2.0.0-SNAPSHOT") == ("1.1.1.default", "2.0.0-SNAPSHOT")
    # Test when version is different
    assert get_current_version("1.1.1.dev1", "2.0.0-SNAPSHOT") == ("1.1.1.default", "1.1.1.default")
    
def _prepare_tmp_file(java: bool = False):
    file = tempfile.NamedTemporaryFile()
    with open(file.name, "w") as f:
        if java:
            f.write("<version>2.0.0-SNAPSHOT</version>")
        else:
            f.write("version: 1.1.1.default")
        f.flush()
    return file

def test_upgrade_file_version():
    non_java_version = "1.1.1.default"
    java_version = "2.0.0-SNAPSHOT"
    new_version = "1.1.1.new"

    # Create temporary files with default version.
    non_java_file_1 = _prepare_tmp_file()
    non_java_file_2 = _prepare_tmp_file()
    java_file_1 = _prepare_tmp_file(java=True)
    java_file_2 = _prepare_tmp_file(java=True)

    non_java_files = set([non_java_file_1.name, non_java_file_2.name])
    java_files = set([java_file_1.name, java_file_2.name])
    upgrade_file_version(
        non_java_files=non_java_files, 
        java_files=java_files, 
        non_java_version=non_java_version, 
        java_version=java_version, 
        new_version=new_version
    )

    for file in non_java_files:
        with open(file, "r") as f:
            assert f.read() == f"version: {new_version}"
    
    for file in java_files:
        with open(file, "r") as f:
            assert f.read() == f"<version>{new_version}</version>"

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))