import sys
import tempfile
from pathlib import Path

import pytest

from ci.raydepsets.tests.utils import copy_data_to_tmpdir
from ci.raydepsets.workspace import Workspace


def test_workspace_init():
    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Workspace(tmpdir)
        assert workspace.dir is not None


def test_parse_build_arg_sets():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        workspace = Workspace(dir=tmpdir)
        config = workspace.load_config(path=Path(tmpdir) / "test.depsets.yaml")
        assert config.build_arg_sets[0].name == "py311_cpu"
        assert config.build_arg_sets[0].build_args == {
            "CUDA_VERSION": "cpu",
            "PYTHON_VERSION": "py311",
        }
        assert config.build_arg_sets[1].name == "py311_cuda128"
        assert config.build_arg_sets[1].build_args == {
            "CUDA_VERSION": 128,
            "PYTHON_VERSION": "py311",
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
