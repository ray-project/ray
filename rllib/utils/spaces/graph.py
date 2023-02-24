from gymnasium.spaces import Box, Dict

import numpy as np

import random

from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class Graph(Dict):
    """Graph obs space for GNNs (built upon Dict space)

    Example:
        node set "node_set" with "default" features (vector of length 10) and
        at most max_n_nodes = 30 (maximal number of nodes in this node set)
        => {"node_set": Repeated(Dict{"default": Box(shape=(10,))}, max_len=30)}

        edge set "edge_set" with "default" features (vector of length 5) and
        adjacency (tuples) between nodes from "node_set"
        => {"edge_set": Repeated(Dict{
            "features": Dict{"default": Box(shape=(5,))},
            "adjacency": Tuple((
                Box(0, source_node_set.max_len-1),
                Box(0, target_node_set.max_len-1)
            ))
        }, max_len=source_node_set.max_len * target_node_set.max_len)}

        context with "global_context" features (vector of length 20)
        => context = Dict{"global_context": Box(shape=(20,))}

        graph_space = Graph(graph_spec, graph_pieces) with
        graph_pieces = {
            "node_sets": {"node_set": ...},
            "edge_sets": {"edge_set": ...},
            "context": context
        }
    """

    def __init__(self, graph_spec, graph_spaces=None, seed=None, **spaces_kwargs):
        super().__init__(spaces=graph_spaces, seed=seed)

        self.graph_spec = graph_spec

        assert (
            "node_sets" in self.spaces
            and isinstance(self.spaces["node_sets"], Dict)
            and len(self.spaces["node_sets"].spaces) > 0
            and all(
                [
                    isinstance(node_set_space, Repeated)
                    for node_set_space in self.spaces["node_sets"].spaces.values()
                ]
            )
        ), "Graph requires at least one node set"
        self.node_sets = self.spaces["node_sets"]

        self.edge_sets = None
        if "edge_sets" in self.spaces and isinstance(self.spaces["edge_sets"], Dict):
            check_edge_sets = True
            for edge_set_space in self.spaces["edge_sets"].spaces.values():
                if not (
                    isinstance(edge_set_space, Repeated)
                    and isinstance(edge_set_space.child_space, Dict)
                    and "adjacency" in edge_set_space.child_space.spaces
                    and "features" in edge_set_space.child_space.spaces
                ):
                    check_edge_sets = False
            if check_edge_sets:
                self.edge_sets = self.spaces["edge_sets"]

        self.context = None
        if "context" in self.spaces and isinstance(self.spaces["context"], (Box, Dict)):
            self.context = self.spaces["context"]

    def sample(self):
        graph = {
            "node_sets": {},
        }

        node_sets_sizes = {}
        for node_set_name, node_set_space in self.node_sets.spaces.items():
            graph["node_sets"][node_set_name] = node_set_space.sample()
            node_sets_sizes[node_set_name] = len(graph["node_sets"][node_set_name])

        if self.edge_sets:
            graph["edge_sets"] = {}
            for edge_set_name, edge_set_space in self.edge_sets.spaces.items():
                source = self.graph_spec.edge_sets_spec[
                    edge_set_name
                ].adjacency_spec.source_name
                target = self.graph_spec.edge_sets_spec[
                    edge_set_name
                ].adjacency_spec.target_name
                max_n_edges = node_sets_sizes[source] * node_sets_sizes[target]
                # draw random number of edges
                n_edges = edge_set_space.np_random.integers(0, max_n_edges + 1)
                adjacency = []
                features = []
                if n_edges > 0:
                    adjacency_tuples = []
                    for i in range(node_sets_sizes[source]):
                        for j in range(node_sets_sizes[target]):
                            adjacency_tuples.append((np.array(i), np.array(j)))
                    adjacency = random.sample(adjacency_tuples, n_edges)
                    features = [
                        edge_set_space.child_space.spaces["features"].sample()
                        for _ in range(n_edges)
                    ]
                graph["edge_sets"][edge_set_name] = [
                    {"adjacency": a, "features": f}
                    for (a, f) in zip(adjacency, features)
                ]

        if self.context:
            graph["context"] = self.context.sample()

        return graph

    def contains(self, x):
        if not isinstance(x, dict) or len(x) != len(self.spaces):
            return False
        for k, space in self.spaces.items():
            if k not in x:
                return False
            if not space.contains(x[k]):
                return False
        return True

    def __repr__(self):
        return (
            "Graph("
            + ", ".join([str(k) + ":" + str(s) for k, s in self.spaces.items()])
            + ")"
        )
