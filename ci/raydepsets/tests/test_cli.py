<<<<<<< HEAD
<<<<<<< HEAD
def test_cli():
    pass
=======
=======
>>>>>>> cleanup tests
import pytest
import sys
import os
from ci.raydepsets.lib.cli import load, DependencySetManager
from ci.raydepsets.lib.config import get_current_directory
from click.testing import CliRunner
from unittest.mock import patch


def test_cli_load_happy():
    with patch(
        "ci.raydepsets.lib.cli.DependencySetManager.execute_all"
    ) as execute_all_call:
        result = CliRunner().invoke(load, ["ci/raydepsets/tests/test.config.yaml"])
        execute_all_call.assert_called_once()


def test_cli_load_fail_no_config():
    result = CliRunner().invoke(load, ["ci/raydepsets/tests/test1.config.yaml"])
    assert result.exit_code == 1
    assert isinstance(result.exception, FileNotFoundError)


def test_cli_load_single_happy():
    manager = DependencySetManager(config_path="ci/raydepsets/tests/test.config.yaml")
    with patch(
        "ci.raydepsets.lib.cli.DependencySetManager.execute_single"
    ) as execute_single_call:
        manager.execute_single(manager.get_depset("ray_base_test_depset"))
        execute_single_call.assert_called_once_with(
            manager.get_depset("ray_base_test_depset")
        )


def test_cli_load_single_fail_no_name():
    result = CliRunner().invoke(
        load, ["ci/raydepsets/tests/test.config.yaml", "--name", "mock_depset"]
    )
    assert result.exit_code == 1
    assert "Dependency set mock_depset not found" in str(result.exception)


def test_depdenecy_set_manager_init_happy():
    manager = DependencySetManager(config_path="ci/raydepsets/tests/test.config.yaml")
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


def test_depdenecy_set_manager_compile_happy():
    with patch(
        "ci.raydepsets.lib.cli.DependencySetManager.exec_uv_cmd"
    ) as mock_exec_uv_cmd:
        manager = DependencySetManager(
            config_path="ci/raydepsets/tests/test.config.yaml"
        )
        manager.compile(
            constraints=[
                os.path.join(
                    get_current_directory(),
                    "python/requirements_compiled_ray_test_py311_cpu.txt",
                )
            ],
            requirements=[
                os.path.join(get_current_directory(), "python/requirements.txt")
            ],
            args=[],
            name="ray_base_test_depset",
            output=os.path.join(
                get_current_directory(),
                "ci/raydepsets/tests/requirements_compiled_test.txt",
            ),
        )
        mock_exec_uv_cmd.assert_called_once_with(
            "compile",
            [
                "-c",
                os.path.join(
                    get_current_directory(),
                    "python/requirements_compiled_ray_test_py311_cpu.txt",
                ),
                os.path.join(get_current_directory(), "python/requirements.txt"),
                "-o",
                os.path.join(
                    get_current_directory(),
                    "ci/raydepsets/tests/requirements_compiled_test.txt",
                ),
            ],
        )


def test_depdenecy_set_manager_get_depset_happy():
    manager = DependencySetManager(config_path="ci/raydepsets/tests/test.config.yaml")
    depset = manager.get_depset("ray_base_test_depset")
    assert depset is not None
    assert depset.name == "ray_base_test_depset"
    assert depset.operation == "compile"
    assert depset.requirements == [
        os.path.join(get_current_directory(), "python/requirements.txt")
    ]
    assert depset.constraints == [
        os.path.join(
            get_current_directory(),
            "python/requirements_compiled_ray_test_py311_cpu.txt",
        )
    ]
    assert depset.output == os.path.join(
        get_current_directory(), "ci/raydepsets/tests/requirements_compiled_test.txt"
    )


def test_depdenecy_set_manager_get_depset_fail_no_depset():
    manager = DependencySetManager(config_path="ci/raydepsets/tests/test.config.yaml")
    with pytest.raises(Exception) as e:
        manager.get_depset("mock_depset")
    assert "Dependency set mock_depset not found" in str(e.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
<<<<<<< HEAD
>>>>>>> raydepsets scaffolding (package management tool)  (#54265)
=======
>>>>>>> cleanup tests
