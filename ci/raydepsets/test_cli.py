import pytest
import sys
import unittest
import tempfile
import runfiles
import subprocess
import platform
from ci.raydepsets.cli import load, DependencySetManager
from ci.raydepsets.workspace import Workspace
from click.testing import CliRunner

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


class TestCli(unittest.TestCase):
    def test_workspace_init_happy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Workspace(tmpdir)
            assert workspace.dir is not None

    def test_cli_load_fail_no_config(self):
        result = CliRunner().invoke(
            load,
            [
                _runfiles.Rlocation(
                    f"{_REPO_NAME}/ci/raydepsets/test_data/fake_path/test.config.yaml"
                ),
                "--workspace-dir",
                _runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/"),
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)
        assert "No such file or directory" in str(result.exception)

    def test_dependency_set_manager_init_happy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = DependencySetManager(
                config_path=_runfiles.Rlocation(
                    f"{_REPO_NAME}/ci/raydepsets/test_data/test.config.yaml"
                ),
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

    def test_uv_binary_exists(self):
        assert _uv_binary() is not None

    def test_uv_version(self):
        result = subprocess.run(
            [_uv_binary(), "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert result.returncode == 0
        assert "uv 0.7.20" in result.stdout.decode("utf-8")
        assert result.stderr.decode("utf-8") == ""


def _uv_binary():
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise RuntimeError(
            f"Unsupported platform/processor: {system}/{platform.processor()}"
        )
    return _runfiles.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
