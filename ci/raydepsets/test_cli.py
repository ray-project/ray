import pytest
import sys
import os
import unittest
import tempfile
import runfiles
from ci.raydepsets.cli import load, DependencySetManager
from ci.raydepsets.workspace import Workspace
from click.testing import CliRunner

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()
WORKSPACE_DIR = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
class TestCli(unittest.TestCase):
    def tearDown(self):
        os.environ["BUILD_WORKSPACE_DIRECTORY"] = WORKSPACE_DIR

    def test_cli_load_fail_no_config(self):
        result = CliRunner().invoke(load, ["/tmp/raydepsets/fake_path/test.config.yaml"])
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)

    def test_workspace_init_happy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Workspace(tmpdir)
            assert workspace.dir is not None

    def test_dependency_set_manager_init_happy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = DependencySetManager(
                config_path=_runfiles.Rlocation(f"{_REPO_NAME}/ci/raydepsets/test_data/test.config.yaml"),
                workspace_dir=tmpdir,
            )
            assert manager is not None
            assert manager.workspace.dir == tmpdir
            assert manager.config.depsets[0].name == "ray_base_test_depset"
            assert manager.config.depsets[0].operation == "compile"
            assert manager.config.depsets[0].requirements == [
                 "python/requirements.txt"
            ]
            assert manager.config.depsets[0].constraints == [
                "python/requirements_compiled_ray_test_py311_cpu.txt"
            ]
            assert manager.config.depsets[0].output == "tests/requirements_compiled_test.txt"

    def test_workspace_init_fail_no_dir(self):
        workspace_dir = os.environ.pop("BUILD_WORKSPACE_DIRECTORY")
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(Exception, match="BUILD_WORKSPACE_DIRECTORY is not set"):
                Workspace()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
