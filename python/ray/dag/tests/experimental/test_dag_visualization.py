import sys
import ray
import pydot
import os
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa

import pytest


def test_visualize_basic(ray_start_regular):
    """
    Expect output or dot_source:
        MultiOutputNode" fillcolor=yellow shape=rectangle style=filled]
            0 -> 1 [label=SharedMemoryType]
            1 -> 2 [label=SharedMemoryType]
    """

    @ray.remote
    class Actor:
        def echo(self, x):
            return x

    actor = Actor.remote()

    with InputNode() as i:
        dag = actor.echo.bind(i)

    compiled_dag = dag.experimental_compile()

    # Call the visualize method
    dot_source = compiled_dag.visualize(return_dot=True)

    graphs = pydot.graph_from_dot_data(dot_source)
    graph = graphs[0]

    node_names = {node.get_name() for node in graph.get_nodes()}
    edge_pairs = {
        (edge.get_source(), edge.get_destination()) for edge in graph.get_edges()
    }

    expected_nodes = {"0", "1", "2"}
    assert expected_nodes.issubset(
        node_names
    ), f"Expected nodes {expected_nodes} not found."

    expected_edges = {("0", "1"), ("1", "2")}
    assert expected_edges.issubset(
        edge_pairs
    ), f"Expected edges {expected_edges} not found."

    compiled_dag.teardown()


def test_visualize_multi_return(ray_start_regular):
    """
    Expect output or dot_source:
        MultiOutputNode" fillcolor=yellow shape=rectangle style=filled]
            0 -> 1 [label=SharedMemoryType]
            1 -> 2 [label=SharedMemoryType]
            1 -> 3 [label=SharedMemoryType]
            2 -> 4 [label=SharedMemoryType]
            3 -> 4 [label=SharedMemoryType]
    """

    @ray.remote
    class Actor:
        @ray.method(num_returns=2)
        def return_two(self, x):
            return x, x + 1

    actor = Actor.remote()

    with InputNode() as i:
        o1, o2 = actor.return_two.bind(i)
        dag = MultiOutputNode([o1, o2])

    compiled_dag = dag.experimental_compile()

    # Get the DOT source
    dot_source = compiled_dag.visualize(return_dot=True)

    graphs = pydot.graph_from_dot_data(dot_source)
    graph = graphs[0]

    node_names = {node.get_name() for node in graph.get_nodes()}
    edge_pairs = {
        (edge.get_source(), edge.get_destination()) for edge in graph.get_edges()
    }

    expected_nodes = {"0", "1", "2", "3", "4"}
    assert expected_nodes.issubset(
        node_names
    ), f"Expected nodes {expected_nodes} not found."

    expected_edges = {("0", "1"), ("1", "2"), ("1", "3"), ("2", "4"), ("3", "4")}
    assert expected_edges.issubset(
        edge_pairs
    ), f"Expected edges {expected_edges} not found."

    compiled_dag.teardown()


def test_visualize_multi_return2(ray_start_regular):
    """
    Expect output or dot_source:
        MultiOutputNode" fillcolor=yellow shape=rectangle style=filled]
            0 -> 1 [label=SharedMemoryType]
            1 -> 2 [label=SharedMemoryType]
            1 -> 3 [label=SharedMemoryType]
            2 -> 4 [label=SharedMemoryType]
            3 -> 5 [label=SharedMemoryType]
            4 -> 6 [label=SharedMemoryType]
            5 -> 6 [label=SharedMemoryType]
    """

    @ray.remote
    class Actor:
        @ray.method(num_returns=2)
        def return_two(self, x):
            return x, x + 1

        def echo(self, x):
            return x

    a = Actor.remote()
    b = Actor.remote()
    with InputNode() as i:
        o1, o2 = a.return_two.bind(i)
        o3 = b.echo.bind(o1)
        o4 = b.echo.bind(o2)
        dag = MultiOutputNode([o3, o4])

    compiled_dag = dag.experimental_compile()

    # Get the DOT source
    dot_source = compiled_dag.visualize(return_dot=True)

    graphs = pydot.graph_from_dot_data(dot_source)
    graph = graphs[0]

    node_names = {node.get_name() for node in graph.get_nodes()}
    edge_pairs = {
        (edge.get_source(), edge.get_destination()) for edge in graph.get_edges()
    }

    expected_nodes = {"0", "1", "2", "3", "4", "5", "6"}
    assert expected_nodes.issubset(
        node_names
    ), f"Expected nodes {expected_nodes} not found."

    expected_edges = {
        ("0", "1"),
        ("1", "2"),
        ("1", "3"),
        ("2", "4"),
        ("3", "5"),
        ("4", "6"),
        ("5", "6"),
    }
    assert expected_edges.issubset(
        edge_pairs
    ), f"Expected edges {expected_edges} not found."

    compiled_dag.teardown()


def test_visualize_multi_input_nodes(ray_start_regular):
    """
    Expect output or dot_source:
        MultiOutputNode" fillcolor=yellow shape=rectangle style=filled]
            0 -> 1
            0 -> 2
            0 -> 3
            1 -> 4
            2 -> 5
            3 -> 6
            4 -> 7
            5 -> 7
            6 -> 7
    """

    @ray.remote
    class Actor:
        def echo(self, x):
            return x

    actor = Actor.remote()

    with InputNode() as inp:
        o1 = actor.echo.bind(inp.x)
        o2 = actor.echo.bind(inp.y)
        o3 = actor.echo.bind(inp.z)
        dag = MultiOutputNode([o1, o2, o3])

    compiled_dag = dag.experimental_compile()

    # Get the DOT source
    dot_source = compiled_dag.visualize(return_dot=True)

    graphs = pydot.graph_from_dot_data(dot_source)
    graph = graphs[0]

    node_names = {node.get_name() for node in graph.get_nodes()}
    edge_pairs = {
        (edge.get_source(), edge.get_destination()) for edge in graph.get_edges()
    }

    expected_nodes = {"0", "1", "2", "3", "4", "5", "6", "7"}
    assert expected_nodes.issubset(
        node_names
    ), f"Expected nodes {expected_nodes} not found."

    expected_edges = {
        ("0", "1"),
        ("0", "2"),
        ("0", "3"),
        ("1", "4"),
        ("2", "5"),
        ("3", "6"),
        ("4", "7"),
        ("5", "7"),
        ("6", "7"),
    }
    assert expected_edges.issubset(
        edge_pairs
    ), f"Expected edges {expected_edges} not found."

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
