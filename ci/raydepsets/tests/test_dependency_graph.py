import sys

import pytest

from ci.raydepsets.dependency_graph import DependencyGraph
from ci.raydepsets.models import Dep


def test_dependency_graph():
    deps = [
        Dep(name="aiohappyeyeballs", version="2.6.1", required_by=["aiohttp"]),
        Dep(name="aiohttp", version="3.11.16", required_by=[]),
        Dep(name="attrs", version="25.4.0", required_by=["aiohttp"]),
        Dep(name="multidict", version="6.7.0", required_by=["aiohttp", "yarl"]),
        Dep(name="yarl", version="1.22.0", required_by=["aiohttp"]),
    ]
    dependency_graph = DependencyGraph()
    dependency_graph.build_dependency_graph(deps)
    assert len(dependency_graph.graph.nodes()) == 5
    assert len(dependency_graph.graph.edges()) == 5


def test_dependency_graph_remove_dropped_dependencies():
    deps = [
        Dep(name="aiohappyeyeballs", version="2.6.1", required_by=["aiohttp"]),
        Dep(name="aiohttp", version="3.11.16", required_by=[]),
        Dep(name="attrs", version="25.4.0", required_by=["aiohttp"]),
        Dep(name="multidict", version="6.7.0", required_by=["aiohttp", "yarl"]),
        Dep(name="yarl", version="1.22.0", required_by=["aiohttp"]),
        Dep(name="anyio", version="3.8.1", required_by=[]),
        Dep(name="sniffio", version="1.3.1", required_by=["anyio"]),
    ]
    dependency_graph = DependencyGraph()
    dependency_graph.build_dependency_graph(deps)
    assert len(dependency_graph.graph.nodes()) == 7
    assert len(dependency_graph.graph.edges()) == 6
    dependency_graph.remove_dropped_dependencies(["anyio"])
    assert len(dependency_graph.graph.nodes()) == 5
    assert len(dependency_graph.graph.edges()) == 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
