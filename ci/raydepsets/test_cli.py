import pytest
import sys
import unittest
import tempfile
import subprocess
import shutil
import runfiles
from ci.raydepsets.cli import (
    load,
    DependencySetManager,
    uv_binary,
    Depset,
    DEFAULT_UV_FLAGS,
)
from ci.raydepsets.workspace import Workspace
from click.testing import CliRunner
from pathlib import Path

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
                args=["--no-annotate", "--no-header"] + DEFAULT_UV_FLAGS.copy(),
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
            _replace_in_file(compiled_file, "emoji==2.9.0", "emoji==2.10.0")
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
                args=["--no-annotate", "--no-header"] + DEFAULT_UV_FLAGS.copy(),
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

    def test_subset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            # Add six to requirements_test_subset.txt
            _save_packages_to_file(
                Path(tmpdir) / "requirements_test_subset.txt",
                ["six==1.16.0"],
            )
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            # Compile general_depset with requirements_test.txt and requirements_test_subset.txt
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt", "requirements_test_subset.txt"],
                args=["--no-annotate", "--no-header"] + DEFAULT_UV_FLAGS.copy(),
                name="general_depset",
                output="requirements_compiled_general.txt",
            )
            # Subset general_depset with requirements_test.txt (should lock emoji & pyperclip)
            manager.subset(
                source_depset="general_depset",
                requirements=["requirements_test.txt"],
                args=["--no-annotate", "--no-header"] + DEFAULT_UV_FLAGS.copy(),
                name="subset_general_depset",
                output="requirements_compiled_subset_general.txt",
            )
            output_file = Path(tmpdir) / "requirements_compiled_subset_general.txt"
            output_text = output_file.read_text()
            output_file_valid = Path(tmpdir) / "requirements_compiled_test.txt"
            output_text_valid = output_file_valid.read_text()

            assert output_text == output_text_valid

    def test_subset_does_not_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            # Add six to requirements_test_subset.txt
            _save_packages_to_file(
                Path(tmpdir) / "requirements_test_subset.txt",
                ["six==1.16.0"],
            )
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            manager.compile(
                constraints=["requirement_constraints_test.txt"],
                requirements=["requirements_test.txt", "requirements_test_subset.txt"],
                args=["--no-annotate", "--no-header"] + DEFAULT_UV_FLAGS.copy(),
                name="general_depset",
                output="requirements_compiled_general.txt",
            )

            with self.assertRaises(RuntimeError):
                manager.subset(
                    source_depset="general_depset",
                    requirements=["requirements_compiled_test.txt"],
                    args=["--no-annotate", "--no-header"] + DEFAULT_UV_FLAGS.copy(),
                    name="subset_general_depset",
                    output="requirements_compiled_subset_general.txt",
                )

    def test_check_if_subset_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            source_depset = Depset(
                name="general_depset",
                operation="compile",
                requirements=["requirements_1.txt", "requirements_2.txt"],
                constraints=["requirement_constraints_1.txt"],
                output="requirements_compiled_general.txt",
            )
            with self.assertRaises(RuntimeError):
                manager.check_subset_exists(
                    source_depset=source_depset,
                    requirements=["requirements_3.txt"],
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

    def test_get_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_data_to_tmpdir(tmpdir)
            manager = DependencySetManager(
                config_path="test.config.yaml",
                workspace_dir=tmpdir,
            )
            assert (
                manager.get_path("requirements_test.txt")
                == f"{tmpdir}/requirements_test.txt"
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


def _save_packages_to_file(filepath, packages):
    with open(filepath, "w") as f:
        for package in packages:
            f.write(package + "\n")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
