import sys
import tempfile

import pytest

from ci.raydepsets.dependency_graph import DependencyGraph
from ci.raydepsets.parser import Dep
from ci.raydepsets.tests.utils import copy_data_to_tmpdir


def test_dependency_graph():
    with tempfile.TemporaryDirectory() as tmpdir:
        copy_data_to_tmpdir(tmpdir)
        deps = [
            Dep(name="aiohappyeyeballs", version="2.6.1", required_by=["aiohttp"]),
            Dep(name="aiohttp", version="3.11.16", required_by=["-r in.lock"]),
            Dep(name="attrs", version="", required_by=["aiohttp"]),
            Dep(name="aiohttp", version="3.11.16", required_by=[""]),
        ]
        dependency_graph = DependencyGraph()
        dependency_graph.build_dependency_graph(deps)
        assert len(dependency_graph.graph.nodes()) == 4
        assert len(dependency_graph.graph.edges()) == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
