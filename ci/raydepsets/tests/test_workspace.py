import sys
import tempfile
import unittest
from pathlib import Path

import pytest

from ci.raydepsets.tests.utils import (
    copy_data_to_tmpdir,
    get_depset_by_name,
    write_to_config_file,
)
from ci.raydepsets.workspace import (
    BuildArgSet,
    Depset,
    Workspace,
    _substitute_build_args,
)


def test_workspace_init():
    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Workspace(tmpdir)
        assert workspace.dir is not None


def test_parse_build_arg_sets():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(config_path=Path(tmpdir) / "test.depsets.yaml")
        assert "general_depset__py311_cpu" in [depset.name for depset in config.depsets]
        assert "build_args_test_depset__py311_cpu" in [
            depset.name for depset in config.depsets
        ]
        assert "expanded_depset__py311_cpu" in [
            depset.name for depset in config.depsets
        ]


def test_substitute_build_args():
    build_arg_set = BuildArgSet(
        build_args={
            "PYTHON_VERSION": "py311",
            "CUDA_VERSION": "cu128",
        },
    )
    depset_dict = {
        "name": "test_depset_${PYTHON_VERSION}_${CUDA_VERSION}",
        "operation": "compile",
        "requirements": ["requirements_test.txt"],
        "output": "requirements_compiled_test_${PYTHON_VERSION}_${CUDA_VERSION}.txt",
    }
    substituted_depset = _substitute_build_args(depset_dict, build_arg_set)
    assert substituted_depset["output"] == "requirements_compiled_test_py311_cu128.txt"
    assert substituted_depset["name"] == "test_depset_py311_cu128"


def test_invalid_build_arg_set():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        with open(Path(tmpdir) / "test.depsets.yaml", "w") as f:
            f.write(
                """
depsets:
    - name: invalid_build_arg_set
      operation: compile
      requirements:
          - requirements_test.txt
      output: requirements_compiled_invalid_build_arg_set.txt
      build_arg_sets:
          - invalid_build_arg_set
            """
            )
        with pytest.raises(KeyError):
            workspace = Workspace(dir=tmpdir)
            workspace.load_config(config_path=Path(tmpdir) / "test.depsets.yaml")


def test_parse_pre_hooks():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(config_path=Path(tmpdir) / "test2.depsets.yaml")
        pre_hook_depset = get_depset_by_name(config.depsets, "pre_hook_test_depset")
        assert pre_hook_depset.pre_hooks == ["pre-hook-test.sh"]


def test_load_first_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(config_path=Path(tmpdir) / "test.depsets.yaml")
        assert config.depsets is not None
        assert len(config.depsets) == 7


def test_load_second_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(config_path=Path(tmpdir) / "test2.depsets.yaml")
        assert config.depsets is not None
        assert len(config.depsets) == 3


# load all configs should always load all depsets
def test_load_all_configs_first_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_configs(config_path=Path(tmpdir) / "test.depsets.yaml")
        assert config.depsets is not None
        assert len(config.depsets) == 10
    # load all configs should always load all depsets
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_configs(config_path=Path(tmpdir) / "test2.depsets.yaml")
        assert config.depsets is not None
        assert len(config.depsets) == 10


def test_merge_configs():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(config_path=Path(tmpdir) / "test.depsets.yaml")
        config2 = workspace.load_config(config_path=Path(tmpdir) / "test2.depsets.yaml")
        merged_config = workspace.merge_configs([config, config2])
        assert merged_config.depsets is not None
        assert len(merged_config.depsets) == 10


def test_get_configs_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        configs_dir = workspace.get_configs_dir(
            configs_path=Path(tmpdir) / "test.depsets.yaml"
        )
        assert len(configs_dir) == 2
        assert f"{tmpdir}/test.depsets.yaml" in configs_dir
        assert f"{tmpdir}/test2.depsets.yaml" in configs_dir


def test_load_configs_with_wildcard_config_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_configs(config_path=f"{tmpdir}/*.depsets.yaml")
        assert config.depsets is not None
        assert len(config.depsets) == 10


def test_invalid_build_arg_set_in_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        depset = Depset(
            name="invalid_build_arg_set",
            operation="compile",
            requirements=["requirements_test.txt"],
            output="requirements_compiled_invalid_build_arg_set.txt",
            config_name="test.depsets.yaml",
        )
        write_to_config_file(
            tmpdir,
            [depset],
            "test.depsets.yaml",
            build_arg_sets=["invalid_build_arg_set"],
        )
        workspace = Workspace(dir=tmpdir)
        with unittest.TestCase().assertRaises(KeyError) as e:
            workspace.load_config(config_path=Path(tmpdir) / "test.depsets.yaml")
        assert (
            "Build arg set invalid_build_arg_set not found in config test.depsets.yaml"
            in str(e.exception)
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
