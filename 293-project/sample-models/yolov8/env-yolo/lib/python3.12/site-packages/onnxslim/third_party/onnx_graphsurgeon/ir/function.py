#
# SPDX-FileCopyrightText: Copyright (c) 1993-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import copy
from typing import List, Sequence

from onnxslim.third_party.onnx_graphsurgeon.ir.graph import Graph
from onnxslim.third_party.onnx_graphsurgeon.ir.node import Node
from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Tensor, Variable
from onnxslim.third_party.onnx_graphsurgeon.logger import G_LOGGER
from onnxslim.third_party.onnx_graphsurgeon.util import misc


class Function(Graph):
    """
    Represents a local function, which is a default implementation of a Custom Op. This default implementation is
    represented as a Graph of other Ops.

    Functions are used in a model by creating a Node with the same name and domain as the function. This can be done
    using the __call__() method of a Function, which creates this new node and appends it to a Graph. A Function is not
    a subgraph of a Graph, and its Nodes, Tensors, and subgraphs are entirely separate from the main Graph.

    Functions can be composed of other functions, but cyclical or recursive definitions are not allowed in ONNX.
    """

    DEFAULT_DOMAIN = "onnx_graphsurgeon"

    def __init__(
        self,
        name: str,
        domain: str = None,
        nodes: Sequence[Node] = None,
        inputs: Sequence[Tensor] = None,
        outputs: Sequence[Tensor] = None,
        doc_string: str = None,
        opset: int = None,
        import_domains: "Sequence[onnx.OperatorSetIdProto]" = None,
        functions: "Sequence[Function]" = None,
        attrs: dict = None,
    ):
        """
        Args:
            name (str): The name of the function.
            domain (str): The domain/namespace of this function.
            nodes (Sequence[Node]): A list of the nodes in this function.
            inputs (Sequence[Tensor]): A list of graph input Tensors.
            outputs (Sequence[Tensor]): A list of graph output Tensors.
            doc_string (str): A doc_string for the function. Defaults to "".
            opset (int): The ONNX opset used by nodes in this function.
            import_domains (Sequence[onnx.OperatorSetIdProto]): The list of domains used by nodes in this function.
            functions (Sequence[Function]): The list of functions in this model.
            attrs (dict): A mapping of attribute names to their default values.
                Nodes within this function can have attributes which take on the values of the Function attributes.
                When a Function is instantiated into a Node, providing attributes to that Node will override the Function's
                default attribute values. A default value of `None` means that the instantiated Node must provide the value
                of that attribute (in other words, it is a required attribute).
        """
        self.domain = misc.default_value(domain, Function.DEFAULT_DOMAIN)
        self.attrs = misc.default_value(attrs, {})

        super().__init__(
            nodes,
            inputs,
            outputs,
            name=name,
            doc_string=doc_string,
            opset=opset,
            import_domains=import_domains,
            functions=functions,
        )

        # Properties of Graph that Function doesn't have.
        del self.producer_name
        del self.producer_version

    @property
    def unique_id(self):
        """Returns a tuple which uniquely identifies this function."""
        return (self.domain, self.name)

    def cleanup(
        self,
        remove_unused_node_outputs=False,
        recurse_subgraphs=True,
        remove_unused_graph_inputs=False,
        recurse_functions=False,
    ):
        """See Graph.cleanup() The only difference is that 'recurse_functions' defaults to False, so that only this
        Function is cleaned up.
        """
        if recurse_functions:
            G_LOGGER.warning(
                "Function.cleanup() called with recurse_functions=True, meaning that other functions will also be cleaned up."
            )
        return super().cleanup(
            remove_unused_node_outputs=remove_unused_node_outputs,
            recurse_subgraphs=recurse_subgraphs,
            remove_unused_graph_inputs=remove_unused_graph_inputs,
            recurse_functions=recurse_functions,
        )

    def fold_constants(self, recurse_functions=False, **kwargs):
        """See Graph.fold_constants() The only difference is that 'recurse_functions' defaults to False, so that only
        this Function's constants are folded.
        """
        if recurse_functions:
            G_LOGGER.warning(
                "Function.fold_constants() called with recurse_functions=True, meaning that other functions will also be const-folded."
            )
        return super().fold_constants(recurse_functions=recurse_functions, **kwargs)

    def toposort(
        self,
        recurse_subgraphs=True,
        recurse_functions=False,
        mode="nodes",
    ):
        """See Graph.toposort() The only difference is that 'recurse_functions' defaults to False and mode defaults to
        "nodes", so that by default only this function's nodes will be sorted.
        """
        if recurse_functions:
            G_LOGGER.warning(
                "Function.toposort() called with recurse_functions=True, meaning that other functions will be sorted."
            )
        return super().toposort(
            recurse_subgraphs=recurse_subgraphs,
            recurse_functions=recurse_functions,
            mode=mode,
        )

    def __call__(self, graph, inputs=None, outputs=None, *args, **kwargs) -> List[Tensor]:
        """
        Creates a Node which is an instance of this function. The created node can be used in a Graph or another
        Function.

        The provided inputs are processed the same way as in Graph.layer().
        If outputs are not provided, they are created based on the Function's outputs.

        Args:
            graph (Union[Graph, Function]): The Graph of Function to add the new node to.
            inputs (List[Union[Tensor, str, numpy.ndarray]]): The list of inputs.
            outputs (List[Union[Tensor, str, numpy.ndarray]]): The list of outputs.
            attrs (Dict[str, Any]): A list of attributes for the node.
                The attribute names should be a subset of this Function's attribute names.
            args/kwargs: These are passed directly to the constructor of Node.

        Returns:
            List[Tensor]: The output tensors of the node.
        """
        if inputs is not None and len(inputs) != len(self.inputs):
            msg_template = "Function {} expects {} inputs, but was called with {} inputs."
            G_LOGGER.warning(msg_template.format(self.name, len(self.inputs), len(inputs)))

        new_output_indices = []
        if outputs is None:
            # Graph.layer() will create Tensors and make sure the names do not conflict.
            outputs = [out.name for out in self.outputs]
            new_output_indices = list(range(len(outputs)))
        elif len(outputs) != len(self.outputs):
            msg_template = "Function {} expects {} outputs, but was called with {} outputs."
            G_LOGGER.warning(msg_template.format(self.name, len(self.outputs), len(outputs)))
        else:
            new_output_indices = [i for i in range(len(outputs)) if not isinstance(outputs[i], Tensor)]

        attrs = kwargs.get("attrs", None)
        if attrs is not None:
            for attr_name, default_val in self.attrs.items():
                if default_val is None and attr_name not in attrs:
                    msg_template = "Function {} called without required attribute: {}"
                    G_LOGGER.warning(msg_template.format(self.name, attr_name))

        inputs = misc.default_value(inputs, [])
        outputs = misc.default_value(outputs, [])
        outputs = graph.layer(
            *args,
            **kwargs,
            op=self.name,
            domain=self.domain,
            inputs=inputs,
            outputs=outputs,
        )

        # For newly created output tensors, set their shape and dtype to match the Function definition.
        for i in new_output_indices:
            outputs[i].dtype = self.outputs[i].dtype
            outputs[i].shape = self.outputs[i].shape

        return outputs

    def copy(self):
        """
        Copy the function.

        This makes copies of all nodes and tensors in the function, but will not
        do a deep-copy of weights or attributes (with the exception of ``Graph``
        attributes, which will be copied using their ``copy`` method).

        Returns:
            Function: A copy of the function.
        """
        local_tensor_copies = {n: t.copy() for n, t in self.tensors().items()}

        def get_tensor(name):
            """Retrieve a tensor by name from a deep-copied dictionary of tensors."""
            return local_tensor_copies[name] if name else Variable.empty()

        # Next, copy nodes, and update inputs/outputs
        new_nodes = []
        for node in self.nodes:
            new_node = node.copy(
                inputs=[get_tensor(inp.name) for inp in node.inputs],
                outputs=[get_tensor(out.name) for out in node.outputs],
                tensor_map=local_tensor_copies,
            )
            new_nodes.append(new_node)
        new_func_inputs = [get_tensor(inp.name) for inp in self.inputs]
        new_func_outputs = [get_tensor(out.name) for out in self.outputs]

        new_attrs = {name: copy.copy(val) for name, val in self.attrs.items()}

        return Function(
            self.name,
            self.domain,
            nodes=new_nodes,
            inputs=new_func_inputs,
            outputs=new_func_outputs,
            doc_string=self.doc_string,
            opset=self.opset,
            import_domains=self.import_domains,
            functions=self.functions,
            attrs=new_attrs,
        )

    def __eq__(self, other: "Function"):
        """Checks equality of self with another Function object based on their attributes."""

        def sequences_equal(seq1, seq2):
            """Checks if two sequences are equal in length and elements."""
            return len(seq1) == len(seq2) and all(elem1 == elem2 for elem1, elem2 in zip(seq1, seq2))

        return (
            self.unique_id == other.unique_id
            and self.opset == other.opset
            and self.import_domains == other.import_domains
            and sequences_equal(self.inputs, other.inputs)
            and sequences_equal(self.outputs, other.outputs)
            and sequences_equal(self.nodes, other.nodes)
        )

    def __str__(self):
        """Returns a string representation of the function including its name, domain, opset, inputs, nodes, and
        outputs.
        """
        nodes_str = "\n".join([str(node) for node in self.nodes])
        out = f"Function {self.name}, Domain {self.domain}, Opset {self.opset}"
        out += f"\nInputs: {self.inputs}"
        out += f"\nNodes: {nodes_str}"
        out += f"\nOutputs: {self.outputs}"
        return out
