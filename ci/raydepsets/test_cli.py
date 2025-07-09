import pytest
import sys
import os
import unittest
from ci.raydepsets.cli import load, DependencySetManager
from ci.raydepsets.workspace import Workspace
from click.testing import CliRunner


class TestCli(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.makedirs("/tmp/raydepsets", exist_ok=True)
        os.system("cp -r ci/raydepsets/test_data/* /tmp/raydepsets")
        os.environ["BUILD_WORKSPACE_DIRECTORY"] = "/tmp/raydepsets"

    @classmethod
    def tearDownClass(cls):
        os.system("rm -rf /tmp/raydepsets")
        os.environ.pop("BUILD_WORKSPACE_DIRECTORY")

    def test_cli_load_fail_no_config(self):
        result = CliRunner().invoke(load, ["/tmp/raydepsets/fake_path/test.config.yaml"])
        assert result.exit_code == 1
        assert isinstance(result.exception, FileNotFoundError)

    def test_workspace_init_happy(self):
        os.environ["BUILD_WORKSPACE_DIRECTORY"] = "/tmp/raydepsets"
        workspace = Workspace()
        assert workspace.dir is not None

    def test_dependency_set_manager_init_happy(self):
        workspace = Workspace()
        manager = DependencySetManager(
            config_path="/tmp/raydepsets/test.config.yaml"
        )
        assert manager is not None
        assert manager.workspace.dir == "/tmp/raydepsets"
        assert manager.config.depsets[0].name == "ray_base_test_depset"
        assert manager.config.depsets[0].operation == "compile"
        assert manager.config.depsets[0].requirements == [
            os.path.join(workspace.dir, "python/requirements.txt")
        ]
        assert manager.config.depsets[0].constraints == [
            os.path.join(
                workspace.dir, "python/requirements_compiled_ray_test_py311_cpu.txt"
            )
        ]
        assert manager.config.depsets[0].output == os.path.join(
            workspace.dir, "tests/requirements_compiled_test.txt"
        )

    def test_workspace_init_fail_no_dir(self):
        os.environ.pop("BUILD_WORKSPACE_DIRECTORY")
        with pytest.raises(Exception, match="BUILD_WORKSPACE_DIRECTORY is not set"):
            Workspace()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
