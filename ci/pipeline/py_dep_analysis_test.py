import os
import tempfile
import unittest

import py_dep_analysis as pda


class TestPyDepAnalysis(unittest.TestCase):
    def create_tmp_file(self, path: str, content: str):
        f = open(path, "w")
        f.write(content)
        f.close()

    def test_full_module_path(self):
        self.assertEqual(pda._full_module_path("aa.bb.cc", "__init__.py"), "aa.bb.cc")
        self.assertEqual(pda._full_module_path("aa.bb.cc", "dd.py"), "aa.bb.cc.dd")
        self.assertEqual(pda._full_module_path("", "dd.py"), "dd")

    def test_bazel_path_to_module_path(self):
        self.assertEqual(
            pda._bazel_path_to_module_path("//python/ray/rllib:xxx/yyy/dd"),
            "ray.rllib.xxx.yyy.dd",
        )
        self.assertEqual(
            pda._bazel_path_to_module_path("python:ray/rllib/xxx/yyy/dd"),
            "ray.rllib.xxx.yyy.dd",
        )
        self.assertEqual(
            pda._bazel_path_to_module_path("python/ray/rllib:xxx/yyy/dd"),
            "ray.rllib.xxx.yyy.dd",
        )

    def test_file_path_to_module_path(self):
        self.assertEqual(
            pda._file_path_to_module_path("python/ray/rllib/env/env.py"),
            "ray.rllib.env.env",
        )
        self.assertEqual(
            pda._file_path_to_module_path("python/ray/rllib/env/__init__.py"),
            "ray.rllib.env",
        )

    def test_import_line_continuation(self):
        graph = pda.DepGraph()
        graph.ids["ray"] = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = os.path.join(tmpdir, "continuation1.py")
            self.create_tmp_file(
                src_path,
                """
import ray.rllib.env.\\
    mock_env
b = 2
""",
            )
            pda._process_file(graph, src_path, "ray")

        self.assertEqual(len(graph.ids), 2)
        print(graph.ids)
        # Shoud pick up the full module name.
        self.assertEqual(graph.ids["ray.rllib.env.mock_env"], 1)
        self.assertEqual(graph.edges[0], {1: True})

    def test_import_line_continuation_parenthesis(self):
        graph = pda.DepGraph()
        graph.ids["ray"] = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = os.path.join(tmpdir, "continuation1.py")
            self.create_tmp_file(
                src_path,
                """
from ray.rllib.env import (ClassName,
    module1, module2)
b = 2
""",
            )
            pda._process_file(graph, src_path, "ray")

        self.assertEqual(len(graph.ids), 2)
        print(graph.ids)
        # Shoud pick up the full module name without trailing (.
        self.assertEqual(graph.ids["ray.rllib.env"], 1)
        self.assertEqual(graph.edges[0], {1: True})

    def test_from_import_file_module(self):
        graph = pda.DepGraph()
        graph.ids["ray"] = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = "multi_line_comment_3.py"
            self.create_tmp_file(
                os.path.join(tmpdir, src_path),
                """
from ray.rllib.env import mock_env
a = 1
b = 2
""",
            )
            # Touch ray/rllib/env/mock_env.py in tmpdir,
            # so that it looks like a module.
            module_dir = os.path.join(tmpdir, "python", "ray", "rllib", "env")
            os.makedirs(module_dir, exist_ok=True)
            f = open(os.path.join(module_dir, "mock_env.py"), "w")
            f.write("print('hello world!')")
            f.close

            pda._process_file(graph, src_path, "ray", _base_dir=tmpdir)

        self.assertEqual(len(graph.ids), 2)
        self.assertEqual(graph.ids["ray.rllib.env.mock_env"], 1)
        # Only 1 edge from ray to ray.rllib.env.mock_env
        # ray.tune.tune is ignored.
        self.assertEqual(graph.edges[0], {1: True})

    def test_from_import_class_object(self):
        graph = pda.DepGraph()
        graph.ids["ray"] = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = "multi_line_comment_3.py"
            self.create_tmp_file(
                os.path.join(tmpdir, src_path),
                """
from ray.rllib.env import MockEnv
a = 1
b = 2
""",
            )
            # Touch ray/rllib/env.py in tmpdir,
            # MockEnv is a class on env module.
            module_dir = os.path.join(tmpdir, "python", "ray", "rllib")
            os.makedirs(module_dir, exist_ok=True)
            f = open(os.path.join(module_dir, "env.py"), "w")
            f.write("print('hello world!')")
            f.close

            pda._process_file(graph, src_path, "ray", _base_dir=tmpdir)

        self.assertEqual(len(graph.ids), 2)
        # Should depend on env.py instead.
        self.assertEqual(graph.ids["ray.rllib.env"], 1)
        # Only 1 edge from ray to ray.rllib.env.mock_env
        # ray.tune.tune is ignored.
        self.assertEqual(graph.edges[0], {1: True})


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
