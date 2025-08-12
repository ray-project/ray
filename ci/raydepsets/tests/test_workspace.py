import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.tests.utils import copy_data_to_tmpdir
from ci.raydepsets.workspace import (
    BuildArgSet,
    Workspace,
    _substitute_build_args,
    _expand_depsets_per_build_arg_set,
)


def test_workspace_init():
    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Workspace(tmpdir)
        assert workspace.dir is not None


def test_parse_build_arg_sets():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        workspace.build_depset_map(path=Path(tmpdir) / "test.depsets.yaml")
        assert workspace.build_arg_sets["py311_cpu"].name == "py311_cpu"
        assert workspace.build_arg_sets["py311_cpu"].build_args == {
            "CUDA_VERSION": "cpu",
            "PYTHON_VERSION": "py311",
        }
        assert workspace.build_arg_sets["py311_cuda128"].name == "py311_cuda128"
        assert workspace.build_arg_sets["py311_cuda128"].build_args == {
            "CUDA_VERSION": 128,
            "PYTHON_VERSION": "py311",
        }


def test_substitute_build_args():
    build_arg_set = BuildArgSet(
        name="py311_cpu",
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


def test_expand_depsets_per_build_arg_set():
    depset = {
        "name": "test_depset_${PYTHON_VERSION}_${CUDA_VERSION}",
        "operation": "compile",
        "requirements": ["requirements_test.txt"],
        "output": "requirements_compiled_test_${PYTHON_VERSION}_${CUDA_VERSION}.txt",
    }
    build_arg_set_matrix = ["py311_cpu", "py311_cuda128"]
    build_arg_sets = {
        "py311_cpu": BuildArgSet(
            name="py311_cpu",
            build_args={"PYTHON_VERSION": "py311", "CUDA_VERSION": "cpu"},
        ),
        "py311_cuda128": BuildArgSet(
            name="py311_cuda128",
            build_args={"PYTHON_VERSION": "py311", "CUDA_VERSION": 128},
        ),
    }
    expanded_depsets = _expand_depsets_per_build_arg_set(
        depset, build_arg_set_matrix, build_arg_sets
    )
    assert len(expanded_depsets) == 2
    assert expanded_depsets[0].name == "test_depset_py311_cpu"
    assert expanded_depsets[0].output == "requirements_compiled_test_py311_cpu.txt"
    assert expanded_depsets[1].name == "test_depset_py311_128"
    assert expanded_depsets[1].output == "requirements_compiled_test_py311_128.txt"


def test_expand_depsets_per_build_arg_set_bad_build_arg_set():
    depset = {
        "name": "test_depset_${PYTHON_VERSION}_${CUDA_VERSION}",
        "operation": "compile",
        "requirements": ["requirements_test.txt"],
        "output": "requirements_compiled_test_${PYTHON_VERSION}_${CUDA_VERSION}.txt",
    }
    build_arg_set_matrix = ["py311_cpu", "py311_cuda128"]
    build_arg_sets = {
        "py311_cpu": BuildArgSet(
            name="py311_cpu",
            build_args={"PYTHON_VERSION": "py311", "CUDA_VERSION": "cpu"},
        ),
    }
    print(f"type of build_arg_sets: {type(build_arg_sets)}")
    with pytest.raises(KeyError):
        _expand_depsets_per_build_arg_set(depset, build_arg_set_matrix, build_arg_sets)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
