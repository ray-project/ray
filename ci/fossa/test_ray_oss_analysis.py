import logging
import os
import unittest
from unittest.mock import mock_open, patch

from ci.fossa import ray_oss_analysis


class TestRayOssAnalysis(unittest.TestCase):
    def setUp(self):
        # Reset logger level for each test
        ray_oss_analysis.logger.setLevel(logging.INFO)

    @patch("ci.fossa.ray_oss_analysis.logging.FileHandler")
    def test_setup_logger(self, mock_file_handler):
        # Configure mock to have a valid level so logging check works
        mock_file_handler.return_value.level = logging.NOTSET

        # Test default setup
        ray_oss_analysis._setup_logger()
        self.assertEqual(ray_oss_analysis.logger.level, logging.INFO)

        # Test with debug and log file
        ray_oss_analysis._setup_logger(log_file="test.log", enable_debug=True)
        self.assertEqual(ray_oss_analysis.logger.level, logging.DEBUG)
        mock_file_handler.assert_called_with("test.log", mode="w")

    def test_isExcludedKind(self):
        self.assertTrue(ray_oss_analysis._is_excluded_kind("config_setting rule"))
        self.assertTrue(ray_oss_analysis._is_excluded_kind("py_library rule"))
        self.assertFalse(ray_oss_analysis._is_excluded_kind("cc_library rule"))
        self.assertFalse(ray_oss_analysis._is_excluded_kind("unknown_kind rule"))

    def test_isBuildTool(self):
        self.assertTrue(ray_oss_analysis._is_build_tool("@bazel_tools//src:windows"))
        self.assertTrue(
            ray_oss_analysis._is_build_tool(
                "@local_config_cc//:builtin_include_directory_paths"
            )
        )
        self.assertFalse(ray_oss_analysis._is_build_tool("//src/ray/util:subreaper.h"))
        self.assertFalse(
            ray_oss_analysis._is_build_tool(
                "@upb//upbc:stage0/google/protobuf/compiler/plugin.upb.c"
            )
        )

    @patch("ci.fossa.ray_oss_analysis.os.getcwd")
    def test_isOwnCode(self, mock_getcwd):
        mock_getcwd.return_value = "/repo/root"
        self.assertTrue(ray_oss_analysis._is_own_code("/repo/root/file.py"))
        self.assertFalse(ray_oss_analysis._is_own_code("/other/root/file.py"))
        self.assertFalse(ray_oss_analysis._is_own_code(None))

    def test_isCppCode(self):
        self.assertTrue(ray_oss_analysis._is_cpp_code("file.cc"))
        self.assertTrue(ray_oss_analysis._is_cpp_code("file.h"))
        self.assertFalse(ray_oss_analysis._is_cpp_code("file.py"))
        self.assertFalse(ray_oss_analysis._is_cpp_code("file.java"))

    def test_get_dependency_info(self):
        # SOURCE_FILE
        info = ray_oss_analysis._get_dependency_info(
            {"type": "SOURCE_FILE", "sourceFile": {"name": "name", "location": "loc"}}
        )
        self.assertEqual(info, ("SOURCE_FILE", "SOURCE_FILE", "name", "loc"))

        # RULE
        info = ray_oss_analysis._get_dependency_info(
            {
                "type": "RULE",
                "rule": {"ruleClass": "cc_library", "name": "name", "location": "loc"},
            }
        )
        self.assertEqual(info, ("RULE", "cc_library", "name", "loc"))

        # UNKNOWN
        info = ray_oss_analysis._get_dependency_info({"type": "UNKNOWN"})
        self.assertEqual(info, ("UNKNOWN", "UNKNOWN", "unknown", "unknown"))

    def test_clean_path(self):
        self.assertEqual(
            ray_oss_analysis._clean_path("/path/to/file:10:20"), "/path/to/file"
        )
        self.assertEqual(ray_oss_analysis._clean_path("/path/to/file"), "/path/to/file")

    def test_get_package_name(self):
        # Test extraction logic
        self.assertEqual(
            ray_oss_analysis._get_package_name("@repo//pkg:target"), "repo"
        )
        self.assertEqual(ray_oss_analysis._get_package_name("@repo//:target"), "repo")
        # Should be None for local targets if regex matches but group 1 is empty
        self.assertIsNone(ray_oss_analysis._get_package_name("//pkg:target"))
        self.assertIsNone(ray_oss_analysis._get_package_name("@//:target"))

    @patch("ci.fossa.ray_oss_analysis.subprocess.check_output")
    def test_get_bazel_dependencies(self, mock_check_output):
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

            self.assertIn("dep", package_names)
            self.assertIn("/external/dep/lib.h", file_paths)
            self.assertNotIn("/abs/file.cc", file_paths)  # Own code

    @patch("ci.fossa.ray_oss_analysis.shutil.copy")
    @patch("ci.fossa.ray_oss_analysis.os.makedirs")
    def test_copy_single_file(self, mock_makedirs, mock_copy):
        ray_oss_analysis._copy_single_file("src", "dst")
        mock_makedirs.assert_called_with(os.path.dirname("dst"), exist_ok=True)
        mock_copy.assert_called_with("src", "dst")

    @patch("ci.fossa.ray_oss_analysis.glob.glob")
    def test_expand_license_files(self, mock_glob):
        mock_glob.side_effect = [
            ["/path/LICENSE"],  # Match LICENSE
            [],
            [],
            [],
            [],  # No match for others
        ]
        with patch("os.path.isfile", return_value=True):
            paths = ray_oss_analysis._expand_license_files("/path")
            self.assertEqual(paths, ["/path/LICENSE"])

    @patch("ci.fossa.ray_oss_analysis.subprocess.check_output")
    def test_askalono_crawl(self, mock_check_output):
        mock_check_output.return_value = (
            '{"path": "p", "result": {"score": 1.0, "license": {"name": "MIT"}}}\n'
        )
        results = ray_oss_analysis._askalono_crawl("/path")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["result"]["license"]["name"], "MIT")

    @patch("ci.fossa.ray_oss_analysis.yaml.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_fossa_deps_file(self, mock_file, mock_yaml_dump):
        askalono_results = [
            {"dependency": "dep1", "license": "MIT", "path": "external/dep1/LICENSE"}
        ]

        ray_oss_analysis._generate_fossa_deps_file(askalono_results, "output")

        mock_file.assert_called_with(os.path.join("output", "fossa_deps.yaml"), "w")
        # Check structure of dumped yaml
        args, _ = mock_yaml_dump.call_args
        data = args[0]
        self.assertIn("custom-dependencies", data)
        self.assertEqual(data["custom-dependencies"][0]["name"], "dep1")
        self.assertEqual(data["custom-dependencies"][0]["license"], "MIT")


if __name__ == "__main__":
    unittest.main()
