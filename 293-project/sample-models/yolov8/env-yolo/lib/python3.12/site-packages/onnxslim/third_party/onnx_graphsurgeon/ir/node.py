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

from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List

from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Tensor
from onnxslim.third_party.onnx_graphsurgeon.logger import G_LOGGER
from onnxslim.third_party.onnx_graphsurgeon.util import misc


class Node:
    @dataclass
    class AttributeRef:
        """
        An AttributeRef is an attribute value which references an attribute in the parent function. A node's attribute
        can only be an AttributeRef if the node lives inside a Function.

        Args:
            name (str): The name of the referenced attribute in the parent Function.
            type (type): The attribute's type.
        """

        name: str
        type: type

    def __init__(
        self,
        op: str,
        name: str = None,
        attrs: Dict[str, object] = None,
        inputs: List["Tensor"] = None,
        outputs: List["Tensor"] = None,
        domain: str = None,
    ):
        """
        A node represents an operation in a graph, and consumes zero or more Tensors, and produces zero or more Tensors.

        Args:
            op (str): The operation this node performs.

            name (str): The name of this node.
            attrs (Dict[str, object]): A dictionary that maps attribute names to their values.
            inputs (List[Tensor]): A list of zero or more input Tensors.
            outputs (List[Tensor]): A list of zero or more output Tensors.
            domain (str): The domain of this node,
        """
        self.op = op
        self.name = misc.default_value(name, "")
        self.attrs = misc.default_value(attrs, OrderedDict())
        self.inputs = misc.SynchronizedList(self, field_name="outputs", initial=misc.default_value(inputs, []))
        self.outputs = misc.SynchronizedList(self, field_name="inputs", initial=misc.default_value(outputs, []))
        self.domain = domain

    def i(self, tensor_idx=0, producer_idx=0):
        """
        Convenience function to get a producer node of one of this node's input tensors. Note that the parameters are
        swapped compared to the o() function; this is because tensors are likely to have only a single producer.

        For example:
        ::

            assert node.i() == node.inputs[0].inputs[0]
            assert node.i(1, 2) == node.inputs[1].inputs[2]

        Args:
            tensor_idx (int): The index of the input tensor of this node. Defaults to 0.
            producer_idx (int): The index of the producer of the input tensor, if the tensor has multiple producers. Defaults to 0

        Returns:
            Node: The specified producer (input) node.
        """
        return self.inputs[tensor_idx].inputs[producer_idx]

    def o(self, consumer_idx=0, tensor_idx=0):
        """
        Convenience function to get a consumer node of one of this node's output tensors.

        For example:
        ::

            assert node.o() == node.outputs[0].outputs[0]
            assert node.o(2, 1) == node.outputs[1].outputs[2]

        Args:
            consumer_idx (int): The index of the consumer of the input tensor. Defaults to 0.
            tensor_idx (int): The index of the output tensor of this node, if the node has multiple outputs. Defaults to 0.

        Returns:
            Node: The specified consumer (output) node
        """
        return self.outputs[tensor_idx].outputs[consumer_idx]

    def subgraphs(self, recursive=False):
        """
        Convenience function to iterate over all subgraphs which are contained in this node. Node subgraphs are found in
        attributes of ONNX control flow nodes such as 'If' and 'Loop'.

        Args:
            recursive (bool): Whether to recurse into the subgraph nodes when looking for subgraphs. Defaults to False.

        Returns:
            A generator which iterates over this node's subgraphs.
        """
        from onnxslim.third_party.onnx_graphsurgeon.ir.graph import Graph

        visit_queue = [self]

        # This prevents infinite recursion in the (illegal) case of cyclical graphs.
        visited = set()

        while visit_queue:
            node = visit_queue.pop()
            for attr in node.attrs.values():
                if isinstance(attr, Graph) and id(attr) not in visited:
                    visited.add(id(attr))
                    if recursive:
                        visit_queue.extend(attr.nodes)
                    yield attr

    def __setattr__(self, name, value):
        """Sets the attribute 'name' to 'value', handling special cases for 'inputs' and 'outputs' attributes."""
        if name in {"inputs", "outputs"}:
            try:
                attr = getattr(self, name)
                if value is attr:
                    # This can happen when using things like +=
                    # The __iadd__ is executed followed by an assignment
                    return

                attr.clear()
                attr.extend(value)
            except AttributeError:
                super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def copy(
        self,
        inputs: List["Tensor"] = None,
        outputs: List["Tensor"] = None,
        tensor_map=None,
    ):
        """
        Makes a shallow copy of this node, overriding input and output information.

        Note: Generally, you should only ever make a copy of a Graph.
        """
        from onnxslim.third_party.onnx_graphsurgeon.ir.graph import Graph

        new_attrs = OrderedDict()
        for name, attr in self.attrs.items():
            new_attrs[name] = attr.copy(tensor_map) if isinstance(attr, Graph) else attr
        return Node(
            self.op,
            self.name,
            new_attrs,
            inputs=inputs,
            outputs=outputs,
            domain=self.domain,
        )

    def __str__(self):
        """Return a string representation of the object showing its name and operation."""
        ret = f"{self.name} ({self.op})"

        def add_io(name, io):
            """Add the input or output operations and their names to the string representation of the object."""
            nonlocal ret
            ret += f"\n\t{name}: ["
            for elem in io:
                ret += f"\n\t\t{elem}"
            ret += "\n\t]"

        add_io("Inputs", self.inputs)
        add_io("Outputs", self.outputs)

        if self.attrs:
            ret += f"\nAttributes: {self.attrs}"

        if self.domain:
            ret += f"\nDomain: {self.domain}"

        return ret

    def __repr__(self):
        """Return the string representation of the Ultralytics object."""
        return self.__str__()

    def __eq__(self, other):
        """Check whether two nodes are equal by comparing name, attributes, op, inputs, and outputs."""
        G_LOGGER.verbose(f"Comparing node: {self.name} with {other.name}")
        attrs_match = self.name == other.name and self.op == other.op and self.attrs == other.attrs
        if not attrs_match:
            return False

        inputs_match = misc.sequences_equal(self.inputs, other.inputs)
        if not inputs_match:
            return False

        outputs_match = misc.sequences_equal(self.outputs, other.outputs)
        return self.domain == other.domain if outputs_match else False
