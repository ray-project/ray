import gymnasium as gym
from gymnasium.spaces import Discrete, Box, Dict, Tuple

import numpy as np

import tensorflow_gnn as tfgnn

from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.spaces.graph import Graph


class GraphObsEnv(gym.Env):
    """Example of a custom env with an observation in form of a graph state.

    The observation is a graph (resp. its state) which consists of at least one
    node set (having some features) and optionally an arbitrary number of edge
    sets (having some features) and also some (global) graph context features.

    Note that the env returns as observation the current graph state which
    serves as input for a TF-GNN model (see TFGraphObsModel).
    The observation space is of type `ray.rllib.utils.spaces.graph.Graph`.
    """

    def __init__(self, config):
        # graph config
        graph_config = config.get("graph_config")
        assert (
            graph_config is not None
            and "graph_schema" in graph_config
            and isinstance(graph_config["graph_schema"], tfgnn.GraphSchema)
        ), "Graph config with a graph schema object is required"

        # action space of the env
        self.action_space = Discrete(4)

        graph_pieces = {}
        graph_schema = graph_config["graph_schema"]

        # node sets of graph
        node_sets = graph_schema.node_sets
        assert (
            node_sets is not None and len(node_sets) > 0
        ), "Graph requires at least one node set"
        node_sets_spaces = {}
        for node_set_name, node_set in node_sets.items():
            assert isinstance(node_set, tfgnn.proto.graph_schema_pb2.NodeSet)
            max_n_nodes = node_set.metadata.cardinality
            assert (
                max_n_nodes > 0
            ), f"Node set {node_set_name} requires positive numbers of max nodes"
            feature_spaces = {}
            for feature_name, feature in node_set.features.items():
                size = feature.shape.dim[0].size
                assert (
                    size >= 0
                ), f"Feature {feature_name} requires number of features >= 0"
                feature_space = Box(-np.inf, np.inf, shape=(size,))
                feature_spaces[feature_name] = feature_space
            node_sets_spaces[node_set_name] = Repeated(
                Dict(feature_spaces), max_len=max_n_nodes
            )
        graph_pieces["node_sets"] = Dict(node_sets_spaces)

        # edge sets of graph
        edge_sets = graph_schema.edge_sets
        if edge_sets:
            edge_sets_spaces = {}
            for edge_set_name, edge_set in edge_sets.items():
                assert isinstance(edge_set, tfgnn.proto.graph_schema_pb2.EdgeSet)
                assert (
                    edge_set.source in node_sets_spaces
                    and edge_set.target in node_sets_spaces
                ), (
                    f"Edge set {edge_set_name} has source {edge_set.source} and/or "
                    f"target {edge_set.target} which don't belong to nodes sets"
                )
                adjacency = Tuple(
                    (
                        Box(
                            0,
                            node_sets_spaces[edge_set.source].max_len - 1,
                            shape=(),
                            dtype=np.int32,
                        ),
                        Box(
                            0,
                            node_sets_spaces[edge_set.target].max_len - 1,
                            shape=(),
                            dtype=np.int32,
                        ),
                    )
                )
                feature_spaces = {}
                for feature_name, feature in edge_set.features.items():
                    size = feature.shape.dim[0].size
                    assert (
                        size >= 0
                    ), f"Feature {feature_name} requires number of features >= 0"
                    feature_space = Box(-np.inf, np.inf, shape=(size,))
                    feature_spaces[feature_name] = feature_space
                edge_sets_spaces[edge_set_name] = Repeated(
                    Dict({"features": Dict(feature_spaces), "adjacency": adjacency}),
                    max_len=node_sets_spaces[edge_set.source].max_len
                    * node_sets_spaces[edge_set.target].max_len,
                )
            graph_pieces["edge_sets"] = Dict(edge_sets_spaces)

        # (global) context features of graph
        if graph_schema.context.features:
            feature_spaces = {}
            for feature_name, feature in graph_schema.context.features.items():
                size = feature.shape.dim[0].size
                assert (
                    size >= 0
                ), f"Feature {feature_name} requires number of features >= 0"
                feature_space = Box(-np.inf, np.inf, shape=(size,))
                feature_spaces[feature_name] = feature_space
            graph_pieces["context"] = Dict(feature_spaces)

        # Observation is a graph
        graph_spec = tfgnn.create_graph_spec_from_schema_pb(graph_schema)
        self.observation_space = Graph(graph_spec, graph_pieces)

    def reset(self):
        return self.observation_space.sample()

    def step(self, action):
        print(f"Action: {action}")
        return self.observation_space.sample(), 1, True, {}
