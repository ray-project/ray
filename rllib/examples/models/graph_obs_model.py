import numpy as np

import tensorflow_gnn as tfgnn

from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.utils.spaces.graph import Graph
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


class TFGraphObsModel(TFModelV2):
    """Example of a TF model which receives a graph as input.

    For more information how to build GNNs with TF-GNN see
    https://github.com/tensorflow/gnn/blob/main/tensorflow_gnn/docs/guide/overview.md
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super().__init__(obs_space, action_space, num_outputs, model_config, name)

        assert isinstance(
            obs_space.original_space, Graph
        ), "(Original) obs space should be a 'Graph' space"

        input_graph = tf.keras.layers.Input(
            type_spec=obs_space.original_space.graph_spec
        )

        # set initial (hidden) states of graph pieces from their features
        graph = tfgnn.keras.layers.MapFeatures(
            node_sets_fn=self.set_initial_node_state,
            edge_sets_fn=self.set_initial_edge_state,
            context_fn=self.set_initial_context_state,
        )(input_graph)

        # core GNN (sequence of various graph updates)
        updated_graph = self.gnn(graph)

        agg_hidden_state_node_set = tfgnn.keras.layers.Pool(
            tfgnn.CONTEXT, "sum", node_set_name="node_set"
        )(updated_graph)

        # pull out logits for action head
        logits = tf.keras.layers.Dense(4)(agg_hidden_state_node_set)
        # vf estimation
        value = tf.keras.layers.Dense(1)(updated_graph.context["global_context"])

        # custom gnn model built upon TF-GNN
        self.gnn_model = tf.keras.Model(input_graph, [logits, value])

    def forward(self, input_dict, state, seq_lens):
        print("The graph obs input tensors:", input_dict["obs"])
        print()

        # graph state obsveration
        graph_dict = input_dict["obs"]
        # graph specifications
        graph_spec = self.obs_space.original_space.graph_spec
        # convert obs to a TF-GNN GraphTensor
        gt = convert_graph_dict_to_graph_tensor(graph_dict, graph_spec)

        # required for later concatenting of context features to nodes
        if any(
            context_feature_tensor.shape.rank != 2
            for context_feature_tensor in gt.context.features.values()
        ):
            context = {}
            for feature_name, feature_tensor in gt.context.features.items():
                num_components = gt.num_components.numpy()  # should be a scalar
                feature_shape = (
                    self.obs_space.original_space.graph_spec.context_spec.features_spec[
                        feature_name
                    ].shape
                )
                assert feature_shape.rank == 2
                context[feature_name] = tf.reshape(
                    feature_tensor, [num_components, feature_shape[-1]]
                )
            gt = gt.replace_features(context=context)

        logits, self._value = self.gnn_model(gt)

        return np.array(logits, dtype=np.float32), state

    def value_function(self):
        """Value function estimate"""
        return self._value

    def set_initial_node_state(self, inputs, **unused_kwargs):
        """Do some initial node state transformations, by default returned as
        'hidden_state'-feature, e.g.
        return tf.keras.layers.Concatenate()(list(inputs.features.values()))
        """
        return inputs.features

    def set_initial_edge_state(self, inputs, **unused_kwargs):
        """Do some initial edge state transformations, by default returned as
        'hidden_state'-feature, e.g.
        return tf.keras.layers.Concatenate()(list(inputs.features.values()))
        """
        return inputs.features

    def set_initial_context_state(self, inputs, **unused_kwargs):
        """Do some initial context state transformations, by default returned as
        'hidden_state'-feature, e.g.
        return tf.keras.layers.Concatenate()(list(inputs.features.values()))
        """
        return inputs.features

    def gnn(self, graph):
        """Actual (core) GNN"""
        # do a graph update on nodes, ...
        graph = tfgnn.keras.layers.GraphUpdate(
            # OPTIONAL: update edge set "edge_set" (others unchanged)
            # edge_sets={"edge_set": tfgnn.keras.layers.EdgeSetUpdate(...)},
            # update node set "node_set" (others unchanged)
            node_sets={
                "node_set": tfgnn.keras.layers.NodeSetUpdate(
                    # convolution over edges:
                    # concat "default" features of source and target nodes + edge set,
                    # apply transform (Dense), then pool (mean) and send to source
                    {
                        "edge_set": tfgnn.keras.layers.SimpleConv(
                            tf.keras.layers.Dense(64, "tanh"),
                            "mean",
                            receiver_tag=tfgnn.SOURCE,
                            receiver_feature="default",
                            sender_node_feature="default",
                            sender_edge_feature="default",
                        )
                    },
                    # next-state computation:
                    # concat old node state, conv result from edge set above and global
                    # context features, then apply transform (Dense) for new node state
                    tfgnn.keras.layers.NextStateFromConcat(
                        tf.keras.layers.Dense(32, "tanh")
                    ),
                    node_input_feature="default",
                    # also include context feature ("global_context") in node update
                    context_input_feature="global_context",
                )
            },
            # OPTIONAL: update context
            # context=tfgnn.keras.layers.ContextUpdate(...)
        )(graph)
        # OPTIONAL: further graph updates
        # graph = tfgnn.keras.layers.GraphUpdate(...)(graph)
        return graph


def convert_graph_dict_to_graph_tensor(
    graph_dict: dict, graph_spec: dict
) -> tfgnn.GraphTensor:
    assert (
        "node_sets" in graph_dict and len(graph_dict["node_sets"]) > 0
    ), "Graph (dict) requires a key 'node_sets' and at least one node set"
    node_sets = graph_dict["node_sets"]
    for node_set_key, node_set_values in node_sets.items():
        assert isinstance(
            node_set_values, RepeatedValues
        ), "A node set should be an instance of 'RepeatedValues'"
        node_set_values.lengths = tf.cast(node_set_values.lengths, tf.int32)
        sizes = tf.reshape(node_set_values.lengths, [-1, 1])  # tf.expand_dims()
        features = {}
        for feature_name, feature_tensor in node_set_values.values.items():
            features[feature_name] = tf.RaggedTensor.from_tensor(
                tensor=feature_tensor, lengths=node_set_values.lengths
            )
        node_sets[node_set_key] = tfgnn.NodeSet.from_fields(
            sizes=sizes, features=features
        )

    if "edge_sets" in graph_dict and len(graph_dict["edge_sets"]) > 0:
        edge_sets = graph_dict["edge_sets"]
        for edge_set_key, edge_set_values in edge_sets.items():
            assert isinstance(
                edge_set_values, RepeatedValues
            ), "An edge set should be an instance of 'RepeatedValues'"
            assert (
                "adjacency" in edge_set_values.values
                and len(edge_set_values.values["adjacency"]) == 2
            ), (
                f"Edge set {edge_set_key} requires adjacency data and "
                "should be a tuple/list of length 2"
            )
            assert edge_set_key in graph_spec.edge_sets_spec and isinstance(
                graph_spec.edge_sets_spec[edge_set_key], tfgnn.EdgeSetSpec
            ), (
                f"Edge set {edge_set_key} requires graph specifications with 'source' "
                "and 'target' nodes"
            )
            source_nodes = graph_spec.edge_sets_spec[
                edge_set_key
            ].adjacency_spec.source_name
            target_nodes = graph_spec.edge_sets_spec[
                edge_set_key
            ].adjacency_spec.target_name
            adjacency = edge_set_values.values["adjacency"]
            edge_set_values.lengths = tf.cast(edge_set_values.lengths, tf.int32)
            sizes = tf.reshape(edge_set_values.lengths, [-1, 1])
            features = {}
            if "features" in edge_set_values.values:
                for feature_name, feature_tensor in edge_set_values.values[
                    "features"
                ].items():
                    features[feature_name] = tf.RaggedTensor.from_tensor(
                        tensor=feature_tensor, lengths=edge_set_values.lengths
                    )
            edge_sets[edge_set_key] = tfgnn.EdgeSet.from_fields(
                sizes=sizes,
                features=features,
                adjacency=tfgnn.Adjacency.from_indices(
                    source=(
                        source_nodes,
                        tf.RaggedTensor.from_tensor(
                            tensor=tf.cast(adjacency[0], tf.int32),
                            lengths=edge_set_values.lengths,
                        ),
                    ),
                    target=(
                        target_nodes,
                        tf.RaggedTensor.from_tensor(
                            tensor=tf.cast(adjacency[1], tf.int32),
                            lengths=edge_set_values.lengths,
                        ),
                    ),
                ),
            )

    if "context" in graph_dict:
        assert isinstance(
            graph_dict["context"], dict
        ), "Context should be a dict of (plain) TF Tensors"
        context = tfgnn.Context.from_fields(
            features={
                feature_name: feature_tensor
                for (feature_name, feature_tensor) in graph_dict["context"].items()
            }
        )

    gt = tfgnn.GraphTensor.from_pieces(
        node_sets=node_sets, edge_sets=edge_sets, context=context
    )
    # instead of batched GraphTensor return scalar GraphTensor w/ multiple components
    return gt.merge_batch_to_components()
