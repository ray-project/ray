import pytest
import sys
import unittest
import tempfile
import subprocess
import shutil
import runfiles
from ci.raydepsets.cli import load, DependencySetManager, uv_binary
from ci.raydepsets.workspace import Workspace
from click.testing import CliRunner

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
            assert manager.config.depsets[0].requirements == ["python/requirements.txt"]
            assert manager.config.depsets[0].constraints == [
                "python/requirements_compiled_ray_test_py311_cpu.txt"
            ]
            assert (
                manager.config.depsets[0].output
                == "tests/requirements_compiled_test.txt"
            )

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


def _copy_data_to_tmpdir(tmpdir):
    shutil.copytree(
        _runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/test_data"),
        tmpdir,
        dirs_exist_ok=True,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
