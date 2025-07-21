import pytest
import sys
import unittest
import tempfile
import runfiles
import shutil
from ci.raydepsets.cli import load, DependencySetManager, Workspace
from click.testing import CliRunner
from pathlib import Path
import os
from ci.raydepsets.cli import uv_binary
import subprocess
from networkx import topological_sort

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


class TestCli(unittest.TestCase):
    def test_workspace_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Workspace(tmpdir)
            assert workspace.dir is not None

    def test_cli_load_fail_no_config(self):
        result = CliRunner().invoke(
            load,
            [
                "fake_path/test.config.yaml",
                "--workspace-dir",
                "/ci/raydepsets/test_data",
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)
        assert "No such file or directory" in str(result.exception)

    def test_dependency_set_manager_init(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            assert manager is not None
            assert manager.workspace.dir == tmpdir
            assert manager.config.depsets[0].name == "ray_base_test_depset"
            assert manager.config.depsets[0].operation == "compile"
            assert manager.config.depsets[0].requirements == ["requirements_test.txt"]
            assert manager.config.depsets[0].constraints == [
                "requirement_constraints_test.txt"
            ]
            assert manager.config.depsets[0].output == "requirements_compiled.txt"

    def test_dependency_set_manager_get_depset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            with self.assertRaises(KeyError):
                manager.get_depset("fake_depset")

    def test_uv_binary_exists(self):
        assert uv_binary() is not None

    def test_uv_version(self):
        result = subprocess.run(
            [uv_binary(), "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert result.returncode == 0
        assert "uv 0.7.20" in result.stdout.decode("utf-8")
        assert result.stderr.decode("utf-8") == ""

    def test_compile(self):
        compiled_file = Path(
            _runfiles.Rlocation(
                f"{_REPO_NAME}/ci/raydepsets/test_data/requirements_compiled_test.txt"
            )
        )
        output_file = Path(
            _runfiles.Rlocation(
                f"{_REPO_NAME}/ci/raydepsets/test_data/requirements_compiled.txt"
            )
        )
        shutil.copy(compiled_file, output_file)

        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                args=["--no-annotate", "--no-header"],
                name="ray_base_test_depset",
                output="requirements_compiled.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_compile_update_package(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            compiled_file = Path(
                _runfiles.Rlocation(f"{tmpdir}/requirement_constraints_test.txt")
            )
            _replace_in_file(compiled_file, "emoji==2.10.0", "emoji==2.12.0")
            output_file = Path(
                _runfiles.Rlocation(f"{tmpdir}/requirements_compiled.txt")
            )
            shutil.copy(compiled_file, output_file)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt"],
                args=["--no-annotate", "--no-header"],
                name="ray_base_test_depset",
                output="requirements_compiled.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test_update.txt"
            output_text_valid = output_file_valid.read_text()
            assert output_text == output_text_valid

    def test_compile_by_depset_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            result = CliRunner().invoke(
                load,
                [
                    "test.config.yaml",
                    "--workspace-dir",
                    tmpdir,
                    "--name",
                    "ray_base_test_depset",
                ],
            )

            output_fp = Path(tmpdir) / "requirements_compiled.txt"
            assert output_fp.is_file()
            assert result.exit_code == 0

            assert (
                "Dependency set ray_base_test_depset compiled successfully"
                in result.output
            )

    def test_subset_by_depset_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            result = CliRunner().invoke(
                load,
                [
                    "test.config.yaml",
                    "--workspace-dir",
                    tmpdir,
                    "--name",
                    "general_depset",
                ],
            )

            output_fp = Path(tmpdir) / "requirements_compiled_general_py311.txt"
            assert result.exit_code == 0
            assert Path(output_fp).is_file()

            result = CliRunner().invoke(
                load,
                [
                    "test.config.yaml",
                    "--workspace-dir",
                    tmpdir,
                    "--name",
                    "subset_general_depset",
                ],
            )

            output_fp = Path(tmpdir) / "requirements_compiled_subset_general.txt"
            assert result.exit_code == 0
            assert Path(output_fp).is_file()
            assert (
                "Dependency set subset_general_depset compiled successfully"
                in result.output
            )

    def test_expand_by_depset_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            result = CliRunner().invoke(
                load,
                [
                    "test.config.yaml",
                    "--workspace-dir",
                    tmpdir,
                    "--name",
                    "general_depset",
                ],
            )

            output_fp = Path(tmpdir) / "requirements_compiled_general_py311.txt"
            assert result.exit_code == 0
            assert Path(output_fp).is_file()

            result = CliRunner().invoke(
                load,
                [
                    "test.config.yaml",
                    "--workspace-dir",
                    tmpdir,
                    "--name",
                    "expand_general_depset",
                ],
            )

            output_fp = Path(tmpdir) / "requirements_compiled_expand_general.txt"
            assert result.exit_code == 0
            assert Path(output_fp).is_file()
            assert (
                "Dependency set expand_general_depset compiled successfully"
                in result.output
            )

    def test_compile_bad_requirements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            with self.assertRaises(RuntimeError):
                manager.compile(
                    constraints=[],
                    requirements=["requirements_test_bad.txt"],
                    args=[],
                    name="general_depset",
                    output="requirements_compiled_general.txt",
                )

    def test_build_graph(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            assert manager.build_graph is not None
            assert len(manager.build_graph.nodes()) == 4
            assert len(manager.build_graph.edges()) == 2
            assert manager.build_graph.nodes["general_depset"]["operation"] == "compile"
            assert (
                manager.build_graph.nodes["subset_general_depset"]["operation"]
                == "subset"
            )
            assert (
                manager.build_graph.nodes["expand_general_depset"]["operation"]
                == "expand"
            )

            sorted_nodes = list(topological_sort(manager.build_graph))
            print(f"type of sorted_nodes: {type(sorted_nodes)}")
            print("sorted_nodes", sorted_nodes)
            print(f"first node: {sorted_nodes[0]}")
            assert "general_depset" in sorted_nodes[:2]
            assert sorted_nodes[2] == "subset_general_depset"
            assert sorted_nodes[3] == "expand_general_depset"

    def test_build_graph_bad_operation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            with open(Path(tmpdir) / "test.config.yaml", "w") as f:
                f.write(
                    """
depsets:
    - name: invalid_op_depset
      operation: invalid_op
      requirements:
          - requirements_test.txt
      output: requirements_compiled_invalid_op.txt
                """
                )
            with self.assertRaises(ValueError):
                DependencySetManager(
                    config_path="test.config.yaml",
                    workspace_dir=tmpdir,
                )

    def test_execute(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            manager.execute()
            assert Path(tmpdir) / "requirements_compiled_general.txt"
            assert Path(tmpdir) / "requirements_compiled_subset_general.txt"
            assert Path(tmpdir) / "requirements_compiled_expand_general.txt"
            assert Path(tmpdir) / "requirements_compiled_ray_test_py311_cpu.txt"

    def test_env_substitution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            output_fp = Path(tmpdir) / "test.config.yaml"
            _replace_in_file(
                output_fp,
                "requirements_compiled_general_py311.txt",
                "requirements_compiled_general_$PYTHON_CODE.txt",
            )
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            assert os.getenv("PYTHON_CODE") == "py311"
            assert (
                "requirements_compiled_general_py311.txt"
                == manager.get_depset("general_depset").output
            )

    def test_env_substitution_with_brackets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            output_fp = Path(tmpdir) / "test.config.yaml"
            _replace_in_file(
                output_fp,
                "requirements_compiled_general_py311.txt",
                "requirements_compiled_general_${PYTHON_CODE}.txt",
            )
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            assert os.getenv("PYTHON_CODE") == "py311"
            assert (
                "requirements_compiled_general_py311.txt"
                == manager.get_depset("general_depset").output
            )


def _copy_data_to_tmpdir(tmpdir):
    shutil.copytree(
        _runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/test_data"),
        tmpdir,
        dirs_exist_ok=True,
    )


def _replace_in_file(filepath, old, new):
    with open(filepath, "r") as f:
        contents = f.read()

    contents = contents.replace(old, new)

    with open(filepath, "w") as f:
        f.write(contents)


def _replace_in_file(filepath, old, new):
    with open(filepath, "r") as f:
        contents = f.read()

    contents = contents.replace(old, new)

    with open(filepath, "w") as f:
        f.write(contents)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
