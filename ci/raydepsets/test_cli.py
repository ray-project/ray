import pytest
import sys
import os
from ci.raydepsets.cli import load, DependencySetManager
from ci.raydepsets.config import get_current_directory
from click.testing import CliRunner
from unittest.mock import patch


def test_cli_load_fail_no_config():
    result = CliRunner().invoke(load, ["ci/raydepsets/test_data/test1.config.yaml"])
    assert result.exit_code == 1
    assert isinstance(result.exception, FileNotFoundError)


def test_depdenecy_set_manager_init_happy():
    manager = DependencySetManager(
        config_path="ci/raydepsets/test_data/test.config.yaml"
    )
    assert manager is not None
    assert manager.config.depsets[0].name == "ray_base_test_depset"
    assert manager.config.depsets[0].operation == "compile"
    assert manager.config.depsets[0].requirements == [
        os.path.join(get_current_directory(), "python/requirements.txt")
    ]
    assert manager.config.depsets[0].constraints == [
        os.path.join(
            get_current_directory(),
            "python/requirements_compiled_ray_test_py311_cpu.txt",
        )
    ]
    assert manager.config.depsets[0].output == os.path.join(
        get_current_directory(), "ci/raydepsets/tests/requirements_compiled_test.txt"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
