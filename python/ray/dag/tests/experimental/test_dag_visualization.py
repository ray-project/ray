import os
import sys

import pydot
import pytest

import ray
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa


@pytest.fixture
def cleanup_files():
    """Clean up files generated during the test."""

    def _cleanup_files(filename: str):
        for ext in ["", ".png", ".pdf", ".jpeg", ".dot"]:
            file_path = filename + ext
            if os.path.exists(file_path):
                os.remove(file_path)

    return _cleanup_files


def test_visualize_basic(ray_start_regular, cleanup_files):
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
    dot_source = compiled_dag.visualize()

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

    cleanup_files("compiled_graph")


def test_visualize_multi_return(ray_start_regular, cleanup_files):
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
    dot_source = compiled_dag.visualize()

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
    cleanup_files("compiled_graph")


def test_visualize_multi_return2(ray_start_regular, cleanup_files):
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
    dot_source = compiled_dag.visualize()

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
    cleanup_files("compiled_graph")


def test_visualize_multi_input_nodes(ray_start_regular, cleanup_files):
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
    dot_source = compiled_dag.visualize()

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
    cleanup_files("compiled_graph")


class TestVisualizationAscii:

    """Tests for the visualize_ascii method of compiled DAGs."""

    @staticmethod
    def parse_ascii_visualization(ascii_visualization):
        """
        Parses the ASCII visualization output to extract node names and edge pairs.

        Args:
            ascii_visualization: The ASCII visualization
                output generated by the `visualize` function.

        Returns:
            tuple: A tuple containing:
                - node_names: A set of strings representing node names.
                - edge_pairs: A set of tuples representing edge
                pairs with type hints.
        """
        import re

        # Sets to store unique nodes and edges
        node_names = set()
        edge_pairs = set()

        # Extract nodes from "Nodes Information" section
        node_pattern = re.compile(r'^(\d+) \[label="Task \d+')
        edge_pattern = re.compile(r"^(\d+) (--->|\+\+\+>) (\d+)")

        lines = ascii_visualization.splitlines()
        in_nodes_section = False
        in_edges_section = False

        for line in lines:
            line = line.strip()

            # Check for nodes section
            if line.startswith("Nodes Information:"):
                in_nodes_section = True
                in_edges_section = False
                continue

            # Check for edges section
            if line.startswith("Edges Information:"):
                in_edges_section = True
                in_nodes_section = False
                continue

            # Collect nodes
            if in_nodes_section:
                node_match = node_pattern.match(line)
                if node_match:
                    node_id = node_match.group(1)
                    node_names.add(node_id)

            # Collect edges
            if in_edges_section:
                edge_match = edge_pattern.match(line)
                if edge_match:
                    from_node, _, to_node = edge_match.groups()
                    edge_pairs.add((from_node, to_node))

        return node_names, edge_pairs

    def test_visualize_ascii_basic(self, ray_start_regular):
        """
        Expect output:
            Nodes Information:
            0 [label="Task 0  InputNode"]
            1 [label="Task 1  Actor: d6c5c4... Method: echo"]
            2 [label="Task 2  MultiOutputNode"]

            Edges Information:
            0 ---> 1
            1 ---> 2

            Legend:
            +++> : Represents Nccl-type data channels
            ---> : Represents Shared Memory data channels

            Experimental Graph:
            0:InputNode
            |
            1:Actor_d6c5c4:echo
            |
            2:MultiOutputNode
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
        ascii_visualization = compiled_dag.visualize(format="ascii")

        node_names, edge_pairs = TestVisualizationAscii.parse_ascii_visualization(
            ascii_visualization
        )
        print(node_names, edge_pairs)
        expected_nodes = {"0", "1", "2"}
        assert expected_nodes.issubset(
            node_names
        ), f"Expected nodes {expected_nodes} not found."

        expected_edges = {("0", "1"), ("1", "2")}
        assert expected_edges.issubset(
            edge_pairs
        ), f"Expected edges {expected_edges} not found."

    def test_visualize_ascii_multi_return(self, ray_start_regular):
        """
        Expect output:
            Nodes Information:
            0 [label="Task 0  InputNode"]
            1 [label="Task 1  Actor: 885f1d... Method: return_two"]
            2 [label="Task 2  ClassMethodOutputNode[0]"]
            3 [label="Task 3  ClassMethodOutputNode[1]"]
            4 [label="Task 4  MultiOutputNode"]

            Edges Information:
            0 ---> 1
            1 ---> 2
            1 ---> 3
            2 ---> 4
            3 ---> 4

            Legend:
            +++> : Represents Nccl-type data channels
            ---> : Represents Shared Memory data channels

            Graph Built:
            0:InputNode
            |
            1:Actor_885f1d:return_two
            |---------------------------->|
            2:Output[0]                   3:Output[1]
            |<----------------------------|
            4:MultiOutputNode
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

        ascii_visualization = compiled_dag.visualize(format="ascii")

        node_names, edge_pairs = TestVisualizationAscii.parse_ascii_visualization(
            ascii_visualization
        )

        expected_nodes = {"0", "1", "2", "3", "4"}
        assert expected_nodes.issubset(
            node_names
        ), f"Expected nodes {expected_nodes} not found."

        expected_edges = {("0", "1"), ("1", "2"), ("1", "3"), ("2", "4"), ("3", "4")}
        assert expected_edges.issubset(
            edge_pairs
        ), f"Expected edges {expected_edges} not found."

    def test_visualize_ascii_multi_return2(self, ray_start_regular):
        """
        Expect output:
            Nodes Information:
            0 [label="Task 0  InputNode"]
            1 [label="Task 1  Actor: f3e919... Method: return_two"]
            2 [label="Task 2  ClassMethodOutputNode[0]"]
            3 [label="Task 3  ClassMethodOutputNode[1]"]
            4 [label="Task 4  Actor: 15ec69... Method: echo"]
            5 [label="Task 5  Actor: 15ec69... Method: echo"]
            6 [label="Task 6  MultiOutputNode"]

            Edges Information:
            0 ---> 1
            1 ---> 2
            1 ---> 3
            2 ---> 4
            3 ---> 5
            4 ---> 6
            5 ---> 6

            Legend:
            +++> : Represents Nccl-type data channels
            ---> : Represents Shared Memory data channels

            Graph Built:
            0:InputNode
            |
            1:Actor_f3e919:return_two
            |---------------------------->|
            2:Output[0]                   3:Output[1]
            |                             |
            4:Actor_15ec69:echo           5:Actor_15ec69:echo
            |<----------------------------|
            6:MultiOutputNode
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

        ascii_visualization = compiled_dag.visualize(format="ascii")

        node_names, edge_pairs = TestVisualizationAscii.parse_ascii_visualization(
            ascii_visualization
        )

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

    def test_visualize_ascii_complicate(self, ray_start_regular):
        """
        Expect output:
            Nodes Information:
            0 [label="Task 0  InputNode"]
            1 [label="Task 1  Actor: 54777d... Method: return_three"]
            2 [label="Task 2  ClassMethodOutputNode[0]"]
            3 [label="Task 3  ClassMethodOutputNode[1]"]
            4 [label="Task 4  ClassMethodOutputNode[2]"]
            5 [label="Task 5  Actor: c927c9... Method: echo"]
            6 [label="Task 6  Actor: c927c9... Method: echo"]
            7 [label="Task 7  Actor: c927c9... Method: return_two"]
            8 [label="Task 8  MultiOutputNode"]
            9 [label="Task 9  ClassMethodOutputNode[0]"]
            10 [label="Task 10  ClassMethodOutputNode[1]"]

            Edges Information:
            0 ---> 1
            1 ---> 2
            1 ---> 3
            1 ---> 4
            2 ---> 5
            3 ---> 6
            4 ---> 7
            5 ---> 8
            6 ---> 8
            9 ---> 8
            10 ---> 8
            7 ---> 9
            7 ---> 10

            Legend:
            +++> : Represents Nccl-type data channels
            ---> : Represents Shared Memory data channels

            Graph Built:
            0:InputNode
            |
            1:Actor_54777d:return_three
            |---------------------------->|---------------------------->|                                                  # noqa
            2:Output[0]                   3:Output[1]                   4:Output[2]                                        # noqa
            |                             |                             |                                                  # noqa
            5:Actor_c927c9:echo           6:Actor_c927c9:echo           7:Actor_c927c9:return_two                          # noqa
            |                             |                             |---------------------------->|                    # noqa
            |                             |                             9:Output[0]                   10:Output[1]         # noqa
            |<----------------------------|-----------------------------|-----------------------------|                    # noqa
            8:MultiOutputNode
        """

        @ray.remote
        class Actor:
            @ray.method(num_returns=3)
            def return_three(self, x):
                return x, x + 1, x + 2

            def echo(self, x):
                return x

            @ray.method(num_returns=2)
            def return_two(self, x):
                return x, x + 1

        a = Actor.remote()
        b = Actor.remote()
        with InputNode() as i:
            o1, o2, o3 = a.return_three.bind(i)
            o4 = b.echo.bind(o1)
            o5 = b.echo.bind(o2)
            o6, o7 = b.return_two.bind(o3)
            dag = MultiOutputNode([o4, o5, o6, o7])

        compiled_dag = dag.experimental_compile()

        ascii_visualization = compiled_dag.visualize(format="ascii")

        node_names, edge_pairs = TestVisualizationAscii.parse_ascii_visualization(
            ascii_visualization
        )

        expected_nodes = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
        assert expected_nodes.issubset(
            node_names
        ), f"Expected nodes {expected_nodes} not found."

        expected_edges = {
            ("0", "1"),
            ("1", "2"),
            ("1", "3"),
            ("1", "4"),
            ("2", "5"),
            ("3", "6"),
            ("4", "7"),
            ("5", "8"),
            ("6", "8"),
            ("9", "8"),
            ("10", "8"),
            ("7", "9"),
            ("7", "10"),
        }
        assert expected_edges.issubset(
            edge_pairs
        ), f"Expected edges {expected_edges} not found."

    def test_visualize_ascii_cross_line(self, ray_start_regular):
        """
        Expect output:
            Nodes Information:
            0 [label="Task 0  InputNode"]
            1 [label="Task 1  Actor: 84835a... Method: return_three"]
            2 [label="Task 2  ClassMethodOutputNode[0]"]
            3 [label="Task 3  ClassMethodOutputNode[1]"]
            4 [label="Task 4  ClassMethodOutputNode[2]"]
            5 [label="Task 5  Actor: 02a6a1... Method: echo"]
            6 [label="Task 6  Actor: 02a6a1... Method: return_two"]
            7 [label="Task 7  Actor: 02a6a1... Method: echo"]
            8 [label="Task 8  MultiOutputNode"]
            9 [label="Task 9  ClassMethodOutputNode[0]"]
            10 [label="Task 10  ClassMethodOutputNode[1]"]

            Edges Information:
            0 ---> 1
            1 ---> 2
            1 ---> 3
            1 ---> 4
            2 ---> 5
            3 ---> 6
            4 ---> 7
            5 ---> 8
            7 ---> 8
            9 ---> 8
            10 ---> 8
            6 ---> 9
            6 ---> 10

            Legend:
            +++> : Represents Nccl-type data channels
            ---> : Represents Shared Memory data channels

            Graph Built:
            0:InputNode
            |
            1:Actor_84835a:return_three
            |---------------------------->|---------------------------->|                            # noqa
            2:Output[0]                   3:Output[1]                   4:Output[2]                  # noqa
            |                             |                             |                            # noqa
            5:Actor_02a6a1:echo           6:Actor_02a6a1:return_two     7:Actor_02a6a1:echo          # noqa
            |                             |---------------------------->|                            # noqa
            |                             9:Output[0]                   10:Output[1]                 # noqa
            |<----------------------------------------------------------|                            # noqa
            8:MultiOutputNod
        """

        @ray.remote
        class Actor:
            @ray.method(num_returns=3)
            def return_three(self, x):
                return x, x + 1, x + 2

            def echo(self, x):
                return x

            @ray.method(num_returns=2)
            def return_two(self, x):
                return x, x + 1

        a = Actor.remote()
        b = Actor.remote()
        with InputNode() as i:
            o1, o2, o3 = a.return_three.bind(i)
            o4 = b.echo.bind(o1)
            o5 = b.echo.bind(o3)
            o6, o7 = b.return_two.bind(o2)
            dag = MultiOutputNode([o4, o5, o6, o7])

        compiled_dag = dag.experimental_compile()

        ascii_visualization = compiled_dag.visualize(format="ascii")

        node_names, edge_pairs = TestVisualizationAscii.parse_ascii_visualization(
            ascii_visualization
        )

        expected_nodes = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
        assert expected_nodes.issubset(
            node_names
        ), f"Expected nodes {expected_nodes} not found."

        expected_edges = {
            ("0", "1"),
            ("1", "2"),
            ("1", "3"),
            ("1", "4"),
            ("2", "5"),
            ("3", "6"),
            ("4", "7"),
            ("5", "8"),
            ("7", "8"),
            ("9", "8"),
            ("10", "8"),
            ("6", "9"),
            ("6", "10"),
        }
        assert expected_edges.issubset(
            edge_pairs
        ), f"Expected edges {expected_edges} not found."


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
