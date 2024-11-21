# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation.  All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------
from logging import getLogger
from typing import Dict

from fusion_base import Fusion
from onnx import helper
from onnx_model import OnnxModel

logger = getLogger(__name__)


class FusionLayerNormalization(Fusion):
    def __init__(self, model: OnnxModel):
        super().__init__(model, "LayerNormalization", "ReduceMean")

    def fuse(self, node, input_name_to_nodes: Dict, output_name_to_node: Dict):
        """
        Fuse Layer Normalization subgraph into one node LayerNormalization:
              +----------------------+
              |                      |
              |                      v
          [Root] --> ReduceMean -->  Sub  --> Pow --> ReduceMean --> Add --> Sqrt --> Div --> Mul --> Add
                     (axis=2 or -1)  |      (Y=2)   (axis=2 or -1)  (E-6 or E-12 or 0)    ^
                                     |                                               |
                                     +-----------------------------------------------+

         It also handles cases of duplicated sub nodes exported from older version of PyTorch:
              +----------------------+
              |                      v
              |           +-------> Sub-----------------------------------------------+
              |           |                                                           |
              |           |                                                           v
          [Root] --> ReduceMean -->  Sub  --> Pow --> ReduceMean --> Add --> Sqrt --> Div  --> Mul --> Add
              |                      ^
              |                      |
              +----------------------+
        """
        subgraph_nodes = []
        children = self.model.get_children(node, input_name_to_nodes)
        if len(children) == 0 or len(children) > 2:
            return

        root_input = node.input[0]

        if children[0].op_type != "Sub" or children[0].input[0] != root_input:
            return

        if len(children) == 2:
            if children[1].op_type != "Sub" or children[1].input[0] != root_input:
                return

        div_node = None
        for child in children:
            # Check if Sub --> Div exists
            div_node_1 = self.model.find_first_child_by_type(child, "Div", input_name_to_nodes, recursive=False)

            # Check if Sub --> Cast --> Div
            div_node_2 = self.model.match_child_path(child, ["Cast", "Div"], exclude=[])

            if div_node_1 is not None:
                div_node = div_node_1
            elif div_node_2 is not None:
                div_node = div_node_2[-1]
        if div_node is None:
            return

        path_id, parent_nodes, _ = self.model.match_parent_paths(
            div_node,
            [
                (["Sqrt", "Add", "ReduceMean", "Pow", "Sub"], [1, 0, 0, 0, 0]),
                (["Sqrt", "Add", "ReduceMean", "Pow", "Cast", "Sub"], [1, 0, 0, 0, 0, 0]),
            ],
            output_name_to_node,
        )
        if path_id < 0:
            return

        sub_node = parent_nodes[-1]
        if sub_node not in children:
            return

        second_add_node = parent_nodes[1]
        i, add_weight = self.model.get_constant_input(second_add_node)
        if add_weight is None or add_weight <= 0 or add_weight > 1.0e-4:
            logger.debug(f"skip SkipLayerNormalization fusion since epsilon value is not expected: {add_weight}")
            return

        pow_node = parent_nodes[3]
        if self.model.find_constant_input(pow_node, 2.0) != 1:
            return

        temp_node = input_name_to_nodes[div_node.output[0]][0]
        if temp_node.op_type == "Cast":
            # Div --> Cast --> Mul
            subgraph_nodes.append(temp_node)  # add Cast node to list of subgraph nodes
            mul_node = input_name_to_nodes[temp_node.output[0]][0]
        else:
            # Div --> Mul
            mul_node = temp_node
        if mul_node.op_type != "Mul":
            return

        last_add_node = input_name_to_nodes[mul_node.output[0]][0]
        if last_add_node.op_type != "Add":
            return

        subgraph_nodes.append(node)
        subgraph_nodes.extend(children)
        subgraph_nodes.extend(parent_nodes[:-1])

        subgraph_nodes.extend([last_add_node, mul_node, div_node])
        if not self.model.is_safe_to_fuse_nodes(
            subgraph_nodes,
            last_add_node.output,
            input_name_to_nodes,
            output_name_to_node,
        ):
            logger.debug("It is not safe to fuse LayerNormalization node. Skip")
            return

        node_before_weight = div_node if temp_node.op_type != "Cast" else temp_node
        weight_input = mul_node.input[1 - self.model.input_index(node_before_weight.output[0], mul_node)]
        if not self.model.is_constant_with_specified_dimension(weight_input, 1, "layernorm weight"):
            return

        bias_input = last_add_node.input[1 - self.model.input_index(mul_node.output[0], last_add_node)]
        if not self.model.is_constant_with_specified_dimension(bias_input, 1, "layernorm bias"):
            return

        self.nodes_to_remove.extend(subgraph_nodes)

        normalize_node = helper.make_node(
            "LayerNormalization",
            inputs=[node.input[0], weight_input, bias_input],
            outputs=[last_add_node.output[0]],
            name=self.model.create_node_name("LayerNormalization", name_prefix="LayerNorm"),
        )
        normalize_node.attribute.extend([helper.make_attribute("epsilon", float(add_weight))])
        self.nodes_to_add.append(normalize_node)
        self.node_name_to_graph_name[normalize_node.name] = self.this_graph_name


class FusionLayerNormalizationTF(Fusion):
    def __init__(self, model: OnnxModel):
        super().__init__(model, "LayerNormalization", "Add", "TF")

    def fuse(self, node, input_name_to_nodes: Dict, output_name_to_node: Dict):
        """
         Layer Norm from Tensorflow model(using keras2onnx or tf2onnx):
          +------------------------------------+
          |                                    |
          |                                    |
        (Cast_1)                               |
          |                                    |
          |                                    v                                           (B)                             (B)             (A)
         Add --> (Cast_1) --> ReduceMean -->  Sub  --> Mul --> ReduceMean --> (Cast_3) --> Add --> Sqrt --> Reciprocol --> Mul --> Mul --> Sub --> Add
          |                       |                                                                                         |       ^              ^
          |                       |                                                                                         |       |              |
          |                       +--------------------------------------------------(Cast_2)-------------------------------|-------+              |
          |                                                                                                                 v                      |
          +---------------------------------------------------------------------------------------------------------------> Mul--------------------+
        """
        return_indice = []
        _, parent_nodes, return_indice = self.model.match_parent_paths(
            node,
            [
                (
                    [
                        "Sub",
                        "Mul",
                        "Mul",
                        "Reciprocal",
                        "Sqrt",
                        "Add",
                        "ReduceMean",
                        "Mul",
                        "Sub",
                        "ReduceMean",
                    ],
                    [1, 1, None, 0, 0, 0, None, 0, 0, None],
                ),
                (
                    [
                        "Sub",
                        "Mul",
                        "Mul",
                        "Reciprocal",
                        "Sqrt",
                        "Add",
                        "Cast",
                        "ReduceMean",
                        "Mul",
                        "Sub",
                        "ReduceMean",
                    ],
                    [1, 1, None, 0, 0, 0, 0, None, 0, 0, None],
                ),
            ],
            output_name_to_node,
        )

        if parent_nodes is None:
            return

        assert len(return_indice) == 3
        if not (return_indice[0] in [0, 1] and return_indice[1] in [0, 1] and return_indice[2] in [0, 1]):
            logger.debug("return indice is exepected in [0, 1], but got {return_indice}")
            return

        (
            sub_node_0,
            mul_node_0,
            mul_node_1,
            reciprocol_node,
            sqrt_node,
            add_node_0,
        ) = parent_nodes[:6]
        reduce_mean_node_0, mul_node_2, sub_node_1, reduce_mean_node_1 = parent_nodes[-4:]

        cast_node_3 = None
        if len(parent_nodes) == 11:
            cast_node_3 = parent_nodes[6]
            assert cast_node_3.op_type == "Cast"

        mul_node_3 = self.model.match_parent(node, "Mul", 0, output_name_to_node)
        if mul_node_3 is None:
            logger.debug("mul_node_3 not found")
            return

        node_before_reduce = self.model.get_parent(reduce_mean_node_1, 0, output_name_to_node)
        root_node = (
            node_before_reduce
            if cast_node_3 is None
            else self.model.get_parent(node_before_reduce, 0, output_name_to_node)
        )
        if root_node is None:
            logger.debug("root node is none")
            return

        i, epsilon = self.model.get_constant_input(add_node_0)
        if epsilon is None or epsilon <= 0 or (epsilon > 1.0e-5 and cast_node_3 is None):
            logger.debug("epsilon is not matched")
            return

        if cast_node_3 is None and (
            reduce_mean_node_1.input[0] not in mul_node_3.input or reduce_mean_node_1.input[0] not in sub_node_1.input
        ):
            logger.debug("reduce_mean_node_1 and mul_node_3 shall link from root node")
            return

        if cast_node_3 is not None and (
            node_before_reduce.input[0] not in mul_node_3.input or reduce_mean_node_1.input[0] not in sub_node_1.input
        ):
            logger.debug("reduce_mean_node_1 and mul_node_3 shall link from root node")
            return

        if mul_node_2.input[0] != mul_node_2.input[1]:
            logger.debug("mul_node_2 shall have two same inputs")
            return

        subgraph_nodes = [
            node,
            sub_node_0,
            mul_node_0,
            mul_node_1,
            reciprocol_node,
            sqrt_node,
            add_node_0,
            reduce_mean_node_0,
            mul_node_2,
            sub_node_1,
            reduce_mean_node_1,
            mul_node_3,
        ]

        if cast_node_3 is not None:
            cast_node_2 = self.model.match_parent(mul_node_0, "Cast", 0, output_name_to_node)
            if cast_node_2 is None:
                logger.debug("cast_node_2 not found")
                return
            subgraph_nodes.extend([node_before_reduce, cast_node_2, cast_node_3])

        if not self.model.is_safe_to_fuse_nodes(
            subgraph_nodes,
            node.output,
            self.model.input_name_to_nodes(),
            self.model.output_name_to_node(),
        ):
            logger.debug("not safe to fuse layer normalization")
            return

        self.nodes_to_remove.extend(subgraph_nodes)

        weight_input = mul_node_1.input[1]
        bias_input = sub_node_0.input[0]

        # TODO: add epsilon attribute
        fused_node = helper.make_node(
            "LayerNormalization",
            inputs=[mul_node_3.input[0], weight_input, bias_input],
            outputs=[node.output[0]],
            name=self.model.create_node_name("LayerNormalization", name_prefix="LayerNorm"),
        )
        fused_node.attribute.extend([helper.make_attribute("epsilon", float(epsilon))])
        self.nodes_to_add.append(fused_node)
        self.node_name_to_graph_name[fused_node.name] = self.this_graph_name
