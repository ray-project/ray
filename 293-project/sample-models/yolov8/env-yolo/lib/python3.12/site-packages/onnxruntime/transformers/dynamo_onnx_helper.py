# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------
import logging

import onnx


class DynamoOnnxHelper:
    """
    Helper class for processing ONNX models exported by torch Dynamo.
    """

    def __init__(self, model: onnx.ModelProto):
        self.model = model

    def update_edges(self, edge_mapping: dict) -> None:
        """
        Updates the edges in the model according to the given mapping.
        """
        for node in self.model.graph.node:
            for i in range(len(node.input)):
                if node.input[i] in edge_mapping:
                    node.input[i] = edge_mapping[node.input[i]]
            for i in range(len(node.output)):
                if node.output[i] in edge_mapping:
                    node.output[i] = edge_mapping[node.output[i]]

        for graph_input in self.model.graph.input:
            if graph_input.name in edge_mapping:
                graph_input.name = edge_mapping[graph_input.name]
        for graph_output in self.model.graph.output:
            if graph_output.name in edge_mapping:
                graph_output.name = edge_mapping[graph_output.name]

    def unroll_function(self, func_name: str) -> None:
        """
        Unrolls the function with the given name in the model.
        """
        logging.info(f"Unrolling function {func_name}...")
        nodes_to_remove = []
        nodes_to_add = []
        edges_to_remove = []
        edges_to_add = []
        for node in self.model.graph.node:
            if node.op_type == func_name:
                nodes_to_remove.append(node)
                edges_to_remove.extend(list(node.input) + list(node.output))

        func_to_remove = None
        for f in self.model.functions:
            if f.name == func_name:
                nodes_to_add.extend(list(f.node))
                edges_to_add.extend(list(f.input) + list(f.output))
                func_to_remove = f

        assert len(edges_to_remove) == len(edges_to_add)

        for node in nodes_to_remove:
            self.model.graph.node.remove(node)
        for node in nodes_to_add:
            self.model.graph.node.append(node)
        if func_to_remove is not None:
            self.model.functions.remove(func_to_remove)

        edge_mapping = {}
        for i in range(len(edges_to_remove)):
            k = edges_to_remove[i]
            v = edges_to_add[i]
            if k != v:
                edge_mapping[k] = v

        return self.update_edges(edge_mapping)

    def remove_function(self, func_name: str, input_id: int, output_id: int) -> None:
        """
        Removes the function in the model.
        """
        edge_mapping = {}
        nodes_to_remove = []
        for node in self.model.graph.node:
            if node.op_type.find(func_name) != -1:
                edge_mapping[node.input[input_id]] = node.output[output_id]
                nodes_to_remove.append(node)
        for node in nodes_to_remove:
            self.model.graph.node.remove(node)

        self.update_edges(edge_mapping)

    def remove_dropout_layer(self) -> None:
        """
        Removes the dropout layer in the model.
        """
        logging.info("Removing dropout layer...")
        self.remove_function("Dropout", 0, 0)

    def remove_lm_head_layer(self) -> None:
        """
        Removes the LM head layer in the model.
        """
        logging.info("Removing LM head layer...")
        # bugbug: need to copy the right vi over
        self.remove_function("Linear_lm_head", 2, 0)
