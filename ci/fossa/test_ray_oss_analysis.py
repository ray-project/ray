import logging
import os
import sys
from unittest.mock import mock_open, patch

import pytest

from ci.fossa import ray_oss_analysis


@pytest.fixture(autouse=True)
def reset_logger():
    """Reset logger level before each test."""
    ray_oss_analysis.logger.setLevel(logging.INFO)


@patch("ci.fossa.ray_oss_analysis.logging.FileHandler")
def test_setup_logger(mock_file_handler) -> None:
    # Configure mock to have a valid level so logging check works
    mock_file_handler.return_value.level = logging.NOTSET

    # Test default setup
    ray_oss_analysis._setup_logger()
    assert ray_oss_analysis.logger.level == logging.INFO

    # Test with debug and log file
    ray_oss_analysis._setup_logger(log_file="test.log", enable_debug=True)
    assert ray_oss_analysis.logger.level == logging.DEBUG
    mock_file_handler.assert_called_with("test.log", mode="w")


def test_is_excluded_kind() -> None:
    assert ray_oss_analysis._is_excluded_kind("config_setting rule")
    assert ray_oss_analysis._is_excluded_kind("py_library rule")
    assert not ray_oss_analysis._is_excluded_kind("cc_library rule")
    assert not ray_oss_analysis._is_excluded_kind("unknown_kind rule")


def test_is_build_tool() -> None:
    assert ray_oss_analysis._is_build_tool("@bazel_tools//src:windows")
    assert ray_oss_analysis._is_build_tool(
        "@local_config_cc//:builtin_include_directory_paths"
    )
    assert not ray_oss_analysis._is_build_tool("//src/ray/util:subreaper.h")
    assert not ray_oss_analysis._is_build_tool(
        "@upb//upbc:stage0/google/protobuf/compiler/plugin.upb.c"
    )


@patch("ci.fossa.ray_oss_analysis.os.getcwd")
def test_is_own_code(mock_getcwd) -> None:
    mock_getcwd.return_value = "/repo/root"
    assert ray_oss_analysis._is_own_code("/repo/root/file.py")
    assert not ray_oss_analysis._is_own_code("/other/root/file.py")
    assert not ray_oss_analysis._is_own_code(None)


def test_is_cpp_code() -> None:
    assert ray_oss_analysis._is_cpp_code("file.cc")
    assert ray_oss_analysis._is_cpp_code("file.h")
    assert not ray_oss_analysis._is_cpp_code("file.py")
    assert not ray_oss_analysis._is_cpp_code("file.java")


def test_get_dependency_info() -> None:
    # SOURCE_FILE
    info = ray_oss_analysis._get_dependency_info(
        {"type": "SOURCE_FILE", "sourceFile": {"name": "name", "location": "loc"}}
    )
    assert info == ("SOURCE_FILE", "SOURCE_FILE", "name", "loc")

    # RULE
    info = ray_oss_analysis._get_dependency_info(
        {
            "type": "RULE",
            "rule": {"ruleClass": "cc_library", "name": "name", "location": "loc"},
        }
    )
    assert info == ("RULE", "cc_library", "name", "loc")

    # UNKNOWN
    info = ray_oss_analysis._get_dependency_info({"type": "UNKNOWN"})
    assert info == ("UNKNOWN", "UNKNOWN", "unknown", "unknown")


def test_clean_path() -> None:
    assert ray_oss_analysis._clean_path("/path/to/file:10:20") == "/path/to/file"
    assert ray_oss_analysis._clean_path("/path/to/file") == "/path/to/file"


def test_get_package_name() -> None:
    # Test extraction logic
    assert ray_oss_analysis._get_package_name("@repo//pkg:target") == "repo"
    assert ray_oss_analysis._get_package_name("@repo//:target") == "repo"
    # Should be None for local targets if regex matches but group 1 is empty
    assert ray_oss_analysis._get_package_name("//pkg:target") is None
    assert ray_oss_analysis._get_package_name("@//:target") is None


@patch("ci.fossa.ray_oss_analysis.subprocess.check_output")
def test_get_bazel_dependencies(mock_check_output) -> None:
    # Mock bazel query output
    mock_output = "\n".join(
        [
            '{"type": "SOURCE_FILE", "sourceFile": {"name": "//:file.cc", "location": "/abs/file.cc:1:1"}}',
            '{"type": "SOURCE_FILE", "sourceFile": {"name": "@dep//:lib.h", "location": "/external/dep/lib.h:1:1"}}',
            '{"type": "RULE", "rule": {"ruleClass": "py_library", "name": "//:py_lib", "location": "/abs/lib.py:1:1"}}',
        ]
    )
    mock_check_output.return_value = mock_output

    # Mock _is_own_code to exclude local files
    with patch("ci.fossa.ray_oss_analysis._is_own_code") as mock_is_own:
        # First file is own code, second is external
        mock_is_own.side_effect = [True, False, True]

        package_names, file_paths = ray_oss_analysis._get_bazel_dependencies(
            "//:target", "bazel"
        )

        assert "dep" in package_names
        assert "/external/dep/lib.h" in file_paths
        assert "/abs/file.cc" not in file_paths  # Own code


@patch("ci.fossa.ray_oss_analysis.shutil.copy")
@patch("ci.fossa.ray_oss_analysis.os.makedirs")
def test_copy_single_file(mock_makedirs, mock_copy) -> None:
    ray_oss_analysis._copy_single_file("src", "dst")
    mock_makedirs.assert_called_with(os.path.dirname("dst"), exist_ok=True)
    mock_copy.assert_called_with("src", "dst")


@patch("ci.fossa.ray_oss_analysis.glob.glob")
def test_expand_license_files(mock_glob) -> None:
    mock_glob.side_effect = [
        ["/path/LICENSE"],  # Match LICENSE
        [],
        [],
        [],
        [],  # No match for others
    ]
    with patch("os.path.isfile", return_value=True):
        paths = ray_oss_analysis._expand_license_files("/path")
        assert paths == ["/path/LICENSE"]


@patch("ci.fossa.ray_oss_analysis.subprocess.check_output")
def test_askalono_crawl(mock_check_output) -> None:
    mock_check_output.return_value = (
        '{"path": "p", "result": {"score": 1.0, "license": {"name": "MIT"}}}\n'
    )
    results = ray_oss_analysis._askalono_crawl("/path")
    assert len(results) == 1
    assert results[0]["result"]["license"]["name"] == "MIT"


@patch("ci.fossa.ray_oss_analysis.yaml.dump")
@patch("builtins.open", new_callable=mock_open)
def test_generate_fossa_deps_file(mock_file, mock_yaml_dump) -> None:
    askalono_results = [
        {"dependency": "dep1", "license": "MIT", "path": "external/dep1/LICENSE"}
    ]

    ray_oss_analysis._generate_fossa_deps_file(askalono_results, "output")

    mock_file.assert_called_with(os.path.join("output", "fossa_deps.yaml"), "w")
    # Check structure of dumped yaml
    args, _ = mock_yaml_dump.call_args
    data = args[0]
    assert "custom-dependencies" in data
    assert data["custom-dependencies"][0]["name"] == "dep1"
    assert data["custom-dependencies"][0]["license"] == "MIT"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
