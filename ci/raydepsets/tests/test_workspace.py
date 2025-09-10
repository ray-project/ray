import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.tests.utils import copy_data_to_tmpdir, get_depset_by_name
from ci.raydepsets.workspace import BuildArgSet, Workspace, _substitute_build_args


def test_workspace_init():
    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Workspace(tmpdir)
        assert workspace.dir is not None


def test_parse_build_arg_sets():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(path=Path(tmpdir) / "test.depsets.yaml")
        assert config.build_arg_sets["py311_cpu"].build_args == {
            "CUDA_VERSION": "cpu",
            "PYTHON_VERSION": "py311",
        }
        assert config.build_arg_sets["py311_cuda128"].build_args == {
            "CUDA_VERSION": 128,
            "PYTHON_VERSION": "py311",
        }


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
            workspace.load_config(path=Path(tmpdir) / "test.depsets.yaml")


def test_parse_pre_hooks():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(path=Path(tmpdir) / "test.depsets.yaml")
        pre_hook_depset = get_depset_by_name(config.depsets, "pre_hook_test_depset")
        assert pre_hook_depset.pre_hooks == ["pre-hook-test.sh"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
