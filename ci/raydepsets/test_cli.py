import pytest
import sys
import unittest
import tempfile
import runfiles
from ci.raydepsets.cli import load, DependencySetManager
from ci.raydepsets.workspace import Workspace
from click.testing import CliRunner
import platform
import subprocess
from pathlib import Path

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


class TestCli(unittest.TestCase):
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
                _runfiles.Rlocation(f"{_REPO_NAME}"),
            ],
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)
        assert "No such file or directory" in str(result.exception)


    def test_compile_by_depset_name_happy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = CliRunner().invoke(
                load,
                [
                    _runfiles.Rlocation(
                        f"{_REPO_NAME}/ci/raydepsets/test_data/test.config.yaml"
                    ),
                    "--workspace-dir",
                    _runfiles.Rlocation(f"{_REPO_NAME}"),
                    "--name",
                    "ray_base_test_depset",
                ],
            )
            output_fp = _runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/test_data/requirements_compiled.txt")
            assert result.exit_code == 0
            assert Path(output_fp).is_file()
            assert "Dependency set ray_base_test_depset compiled successfully" in result.output


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
            assert manager.config.depsets[0].requirements == ["ci/raydepsets/test_data/requirements_test.txt"]
            assert manager.config.depsets[0].constraints == [
                "ci/raydepsets/test_data/requirement_constraints_test.txt"
            ]
            assert (
                manager.config.depsets[0].output
                == "ci/raydepsets/test_data/requirements_compiled.txt"
            )



def _uv_binary():
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise ValueError(f"Unsupported platform: {system}")
    return r.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
