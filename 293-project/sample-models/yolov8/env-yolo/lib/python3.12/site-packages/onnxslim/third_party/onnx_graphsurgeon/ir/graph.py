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
import numbers
from collections import OrderedDict, defaultdict
from typing import List, Sequence

import numpy as np

from onnxslim.third_party.onnx_graphsurgeon.ir.node import Node
from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Constant, Tensor, Variable
from onnxslim.third_party.onnx_graphsurgeon.logger import G_LOGGER, LogMode
from onnxslim.third_party.onnx_graphsurgeon.util import misc


class NodeIDAdder:
    def __init__(self, graph):
        """Initializes NodeIDAdder with a specified graph."""
        self.graph = graph

    def __enter__(self):
        """Assigns unique `id` attributes to each node in the graph upon entering the context."""
        # Using the index in the node list allows the same object to count as different nodes.
        for index, node in enumerate(self.graph.nodes):
            node.id = index

    def __exit__(self, exc_type, exc_value, traceback):
        """Removes `id` attributes from each node in the graph upon exiting the context."""
        for node in self.graph.nodes:
            del node.id


class Graph:
    """Represents a graph containing nodes and tensors."""

    DEFAULT_OPSET = 11
    OPSET_FUNC_MAP = defaultdict(dict)  # Ops registered for specific opsets.
    GLOBAL_FUNC_MAP = {}  # Ops registered for ALL opsets.

    @staticmethod
    def register(opsets=None):
        """
        Registers a function with the Graph class for the specified group of opsets. After registering the function, it
        can be accessed like a normal member function.

        For example:
        ::

            @Graph.register()
            def add(self, a, b):
                '''Registers a function with the Graph class for the specified group of opsets for dynamic access as a member function.'''
                return self.layer(op="Add", inputs=[a, b], outputs=["add_out_gs"])

            graph.add(a, b)

        Args:
            opsets (Sequence[int]):
                    A group of opsets for which to register the function. Multiple functions with the same
                    name may be registered simultaneously if they are registered for different opsets.
                    Registering a function with a duplicate name for the same opsets will overwrite any
                    function previously registered for those opsets.  By default, the function is
                    registered for all opsets.
        """

        def register_func(func):
            """Registers a function for different opsets, overwriting any previously registered function with the same
            name.
            """
            if hasattr(Graph, func.__name__):
                G_LOGGER.warning(
                    f"Registered function: {func.__name__} is hidden by a Graph attribute or function with the same name. "
                    "This function will never be called!"
                )

            # Default behavior is to register functions for all opsets.
            if opsets is None:
                Graph.GLOBAL_FUNC_MAP[func.__name__] = func
            else:
                for opset in opsets:
                    Graph.OPSET_FUNC_MAP[opset][func.__name__] = func
            return func

        return register_func

    def __init__(
        self,
        nodes: Sequence[Node] = None,
        inputs: Sequence[Tensor] = None,
        outputs: Sequence[Tensor] = None,
        name=None,
        doc_string=None,
        opset=None,
        import_domains=None,
        producer_name: str = None,
        producer_version: str = None,
        functions: "Sequence[Function]" = None,
    ):
        """
        Args:
            nodes (Sequence[Node]): A list of the nodes in this graph.
            inputs (Sequence[Tensor]): A list of graph input Tensors.
            outputs (Sequence[Tensor]): A list of graph output Tensors.
            name (str): The name of the graph. Defaults to "onnx_graphsurgeon_graph".
            doc_string (str): A doc_string for the graph. Defaults to "".
            opset (int): The ONNX opset to use when exporting this graph.
            producer_name (str): The name of the tool used to generate the model. Defaults to "".
            producer_version (str): The version of the generating tool. Defaults to "".
        """
        self.nodes = misc.default_value(nodes, [])
        self.inputs = list(misc.default_value(inputs, []))
        self.outputs = list(misc.default_value(outputs, []))

        self.name = misc.default_value(name, "onnx_graphsurgeon_graph")
        self.__name__ = self.name

        self.doc_string = misc.default_value(doc_string, "")
        self.opset = misc.default_value(opset, Graph.DEFAULT_OPSET)
        self.producer_name = misc.default_value(producer_name, "")
        self.producer_version = misc.default_value(producer_version, "")
        self.import_domains = import_domains
        # For layer() function
        self.name_idx = 0

        # In ONNX, the same list of Functions is shared between all Graphs & Functions in a model.
        # Protect the list object with an underscore as self._functions
        # Users should access/modify/set this list via graph.functions
        self._functions = list(misc.default_value(functions, []))
        self._merge_subgraph_functions()

        # Printing graphs can be very expensive
        G_LOGGER.ultra_verbose(lambda: f"Created Graph: {self}")

    def __getattr__(self, name):
        """Dynamically handles attribute access, falling back to superclass attribute retrieval if not found."""
        try:
            return super().__getattribute__(name)
        except AttributeError as err:
            # Warn user if the name matches multiple registered functions.
            methods = []
            method_descs = []

            # Opset specific ops always take priority over global ops.
            if self.opset in Graph.OPSET_FUNC_MAP and name in Graph.OPSET_FUNC_MAP[self.opset]:
                methods.append(Graph.OPSET_FUNC_MAP[self.opset][name])
                method_descs.append(f'GraphSurgeon-registered function "{name}" with opset {self.opset}')

            # Registered ops take priority over Local Functions.
            if name in Graph.GLOBAL_FUNC_MAP:
                methods.append(Graph.GLOBAL_FUNC_MAP[name])
                method_descs.append(f'GraphSurgeon-registered function "{name}"')

            for func in self.functions:
                if func.name == name:
                    methods.append(func.__call__)
                    method_descs.append(f'Local Function "{func.name}" with domain "{func.domain}"')

            if methods:
                if len(methods) > 1:
                    msg_template = (
                        "Method name {} is overloaded with the following candidates: {}. " + "Choosing candidate {}"
                    )
                    G_LOGGER.warning(
                        message=msg_template.format(name, method_descs, method_descs[0]),
                        mode=LogMode.ONCE,
                    )
                return lambda *args, **kwargs: methods[0](self, *args, **kwargs)

            found_in_other_opsets = {opset for opset, opset_map in Graph.OPSET_FUNC_MAP.items() if name in opset_map}

            G_LOGGER.error(
                f"Function: '{name}' was not registered for opset {self.opset}. "
                + (
                    f"Note: '{name}' was registered for opsets: {found_in_other_opsets}."
                    if found_in_other_opsets
                    else ""
                )
            )
            raise err

    def __setattr__(self, name, value):
        """Sets an attribute to the given value, converting 'inputs' and 'outputs' to lists if applicable."""
        if name in {"inputs", "outputs"}:
            value = list(value)
        return super().__setattr__(name, value)

    @property
    def functions(self) -> "List[Function]":
        """Returns the list of subgraph functions associated with this graph."""
        return self._functions

    @functions.setter
    def functions(self, new_fns: "Sequence[Function]"):
        """Get or set the list of functions, ensuring changes propagate to all associated subgraphs and functions."""
        # this graph, its subgraphs, and its functions.
        # If the user sets a new value for self.functions,
        # all subgraphs and functions should also see this new value.
        self._functions.clear()
        self._functions += list(new_fns)

    def __eq__(self, other: "Graph"):
        """Check for equality between two Graph objects by comparing their nodes, inputs, and outputs."""
        nodes_match = misc.sequences_equal(self.nodes, other.nodes)
        if not nodes_match:
            return False
        inputs_match = misc.sequences_equal(self.inputs, other.inputs)
        if not inputs_match:
            return False
        outputs_match = misc.sequences_equal(self.outputs, other.outputs)
        if not outputs_match:
            return False

        return self.opset == other.opset and self.import_domains == other.import_domains

    def node_ids(self):
        """
        Returns a context manager that supplies unique integer IDs for Nodes in the Graph.

        For example:
        ::

            with graph.node_ids():
                assert graph.nodes[0].id != graph.nodes[1].id

        Returns:
            NodeIDAdder: A context manager that supplies unique integer IDs for Nodes.
        """
        return NodeIDAdder(self)

    # Gets the node ID for a node. All internal code should use this instead of accessing `node.id` directly.
    def _get_node_id(self, node):
        """Gets the node ID for a node, ensuring all internal code uses this instead of directly accessing `node.id`."""
        try:
            return node.id
        except AttributeError:
            G_LOGGER.critical(
                f"Encountered a node not in the graph:\n{node}.\n\n"
                "To fix this, please append the node to this graph's `nodes` attribute."
            )

    # A tensor is local if it is produced in this graph, or is explicitly a graph input.
    def _local_tensors(self):
        """Return a dictionary of tensors that are local to the graph, including nodes' outputs, graph inputs, and
        constants.
        """
        local_tensors = {t.name: t for node in self.nodes for t in node.outputs if not t.is_empty()}
        local_tensors.update({t.name: t for t in self.inputs})
        local_tensors.update({t.name: t for t in self.tensors().values() if isinstance(t, Constant)})
        return local_tensors

    # Returns tensors used by this graph which are not present in the graph.
    # These may come from an outer graph for example.
    def _foreign_tensors(self):
        """Returns tensors used by this graph which are not present in the graph, potentially from an outer graph."""
        local_tensors = self._local_tensors()
        foreign_tensors = {}

        def is_foreign_tensor(tensor):
            """Check if a tensor is foreign by verifying its absence in local tensors."""
            return tensor.name not in local_tensors

        for node in self.nodes:
            foreign_tensors.update({t.name: t for t in node.inputs if is_foreign_tensor(t)})

            for subgraph in node.subgraphs():
                subgraph_foreign_tensors = subgraph._foreign_tensors()
                # Some of the foreign tensors from a subgraph may come from this graph.
                subgraph_foreign_tensors = {
                    t.name: t for t in subgraph_foreign_tensors.values() if is_foreign_tensor(t)
                }
                foreign_tensors.update(subgraph_foreign_tensors)

        return foreign_tensors

    def _get_used_node_ids(self):
        """Returns a dictionary of tensors that are used by node IDs in the current subgraph."""
        local_tensors = self._local_tensors()

        class IgnoreDupAndForeign:
            def __init__(self, initial_tensors=None):
                """Initialize IgnoreDupAndForeign with an optional list of initial tensors."""
                tensors = misc.default_value(initial_tensors, [])
                self.seen_tensors = {tensor.name for tensor in tensors}

            def __call__(self, tensor):
                """Check if a tensor should be included based on its name and whether it has been seen before."""
                # False if it should be filtered out.
                if tensor.is_empty():
                    return True
                elif tensor.name not in local_tensors:
                    return False
                elif tensor.name not in self.seen_tensors:
                    self.seen_tensors.add(tensor.name)
                    return True
                return False

        # Traverse backwards from outputs to find all used nodes.
        ignore_tensors = IgnoreDupAndForeign()
        used_tensors = list(filter(ignore_tensors, self.outputs))
        used_node_ids = set()

        index = 0
        while index < len(used_tensors):
            used_tensor = used_tensors[index]
            index += 1
            for node in used_tensor.inputs:
                # Must cast to list here, otherwise node_used_tensors will be SynchronizedList!
                node_used_tensors = list(node.inputs)

                # If a node includes a subgraph, get any tensors that it uses from the outer graph.
                for subgraph in node.subgraphs():
                    node_used_tensors += list(subgraph._foreign_tensors().values())

                used_node_ids.add(self._get_node_id(node))
                used_tensors.extend(filter(ignore_tensors, node_used_tensors))
        return used_node_ids, used_tensors

    def _merge_subgraph_functions(self):
        """Merge function lists of subgraphs into the parent graph's function list."""
        # function list than the parent graph. This function merges those lists.
        func_ids = {func.unique_id for func in self.functions}

        def absorb_function_list(func_list):
            """Absorb and merge unique functions from a provided function list into the parent graph's function list."""
            for func in func_list:
                if func.unique_id not in func_ids:
                    self.functions.append(func)
                    func_ids.add(func.unique_id)
            return self.functions

        for graph in self.functions + [self]:
            for subgraph in graph.subgraphs(recursive=True):
                new_list = absorb_function_list(subgraph.functions)
                subgraph._functions = new_list

        for func in self.functions:
            func._functions = absorb_function_list(func.functions)

    def subgraphs(self, recursive=False):
        """
        Convenience function to iterate over all subgraphs which are contained in this graph. Subgraphs are found in the
        attributes of ONNX control flow nodes such as 'If' and 'Loop'.

        Args:
            recursive (bool): Whether to recursively search this graph's subgraphs for more subgraphs. Defaults to False.

        Returns:
            A generator which iterates over the subgraphs contained in this graph.
        """
        for node in self.nodes:
            yield from node.subgraphs(recursive=recursive)

    def cleanup(
        self,
        remove_unused_node_outputs=False,
        recurse_subgraphs=True,
        remove_unused_graph_inputs=False,
        recurse_functions=True,
    ):
        """
        Removes unused nodes and tensors from the graph. A node or tensor is considered unused if it does not contribute
        to any of the graph outputs.

        Additionally, any producer nodes of graph input tensors, as well as consumer nodes of graph output
        tensors that are not in the graph, are removed from the graph.

        *Note: This function will never modify graph output tensors.*

        Args:
            remove_unused_node_outputs (bool): Whether to remove unused output tensors of nodes. This will never remove
                empty-tensor (i.e. optional, but omitted) outputs. Defaults to False.
            recurse_subgraphs (bool):
                    Whether to recursively cleanup subgraphs.
                    Defaults to True.
            remove_unused_graph_inputs (bool):
                    Whether to remove unused graph inputs.
                    Defaults to False.
            recurse_functions (bool):
                    Whether to also clean up this graph's local functions.
                    Defaults to True.

        Returns:
            self
        """

        def cleanup_subgraphs():
            """Clean up subgraphs by removing unused node outputs and graph inputs, optionally recursing into subgraphs
            and local functions.
            """
            for subgraph in self.subgraphs():
                subgraph.cleanup(
                    remove_unused_node_outputs=remove_unused_node_outputs,
                    recurse_subgraphs=recurse_subgraphs,
                    remove_unused_graph_inputs=False,
                    recurse_functions=False,  # Only cleanup functions once
                )

        if recurse_subgraphs:
            cleanup_subgraphs()

        if recurse_functions:
            for func in self.functions:
                func.cleanup(
                    remove_unused_node_outputs=remove_unused_node_outputs,
                    recurse_subgraphs=recurse_subgraphs,
                    remove_unused_graph_inputs=remove_unused_graph_inputs,
                    recurse_functions=False,  # No infinite recursion
                )

        G_LOGGER.verbose(f"Cleaning up {self.name}")

        with self.node_ids():
            # Graph input producers must be removed first so used_node_ids is correct.
            for inp in self.inputs:
                inp.inputs.clear()

            used_node_ids, used_tensors = self._get_used_node_ids()

            inputs = []
            for inp in self.inputs:
                if inp in used_tensors or not remove_unused_graph_inputs:
                    inputs.append(inp)
                else:
                    G_LOGGER.debug(f"Removing unused input: {inp}")
            self.inputs = inputs

            nodes = []

            for node in self.nodes:
                if self._get_node_id(node) in used_node_ids:
                    nodes.append(node)
                else:
                    node.inputs.clear()
                    node.outputs.clear()
                    G_LOGGER.ultra_verbose(f"Removing unused node: {node}")

            # Remove any hanging tensors - tensors without outputs
            if remove_unused_node_outputs:
                graph_output_names = {tensor.name for tensor in self.outputs}
                for node in nodes:

                    def is_hanging_tensor(tensor):
                        """Checks if a tensor is hanging by verifying it is non-empty, has no outputs, and is not a
                        graph output.
                        """
                        return (
                            not tensor.is_empty() and len(tensor.outputs) == 0 and tensor.name not in graph_output_names
                        )

                    to_remove = [out for out in node.outputs if is_hanging_tensor(out)]
                    for out in to_remove:
                        if out in node.outputs:
                            node.outputs.remove(out)

            self.nodes = nodes

            return self

    def toposort(
        self,
        recurse_subgraphs=True,
        recurse_functions=True,
        mode="full",
    ):
        """
        Topologically sort the graph in place.

        Args:
            recurse_subgraphs (bool):
                    Whether to recursively topologically sort subgraphs.
                    Only applicable when mode="full" or mode="nodes".
                    Defaults to True.
            recurse_functions (bool):
                    Whether to topologically sort the nodes of this graph's functions.
                    Only applicable when mode="full" or mode="nodes".
                    Defaults to True.
            mode (str):
                    Whether to reorder this graph's list of nodes, list of functions, or both.
                    Possible values:
                    - "full": Topologically sort the list of nodes and the list of functions.
                    - "nodes": Only sort the list of nodes.
                    - "functions": Only sort the list of functions.
                    Defaults to "full".

        Returns:
            self
        """
        ALLOWED_MODES = ["full", "nodes", "functions"]
        if mode not in ALLOWED_MODES:
            G_LOGGER.critical(f'Mode "{mode}" not in {ALLOWED_MODES}')

        sort_nodes = mode in {"full", "nodes"}
        sort_functions = mode in {"full", "functions"}

        if sort_nodes and recurse_functions:
            for func in self.functions:
                func.toposort(recurse_subgraphs=recurse_subgraphs, mode="nodes")

        if sort_nodes and recurse_subgraphs:
            for subgraph in self.subgraphs():
                subgraph.toposort(recurse_subgraphs=True, recurse_functions=False, mode="nodes")

        G_LOGGER.debug(f"Topologically sorting {self.name}")

        # Keeps track of a node and its level in the graph hierarchy.
        # 0 corresponds to an input node, N corresponds to a node with N layers of inputs.
        class HierarchyDescriptor:
            def __init__(self, node_or_func, level=None):
                """Initializes a HierarchyDescriptor with a node or function and an optional level in the graph
                hierarchy.
                """
                self.node_or_func = node_or_func
                self.level = level

            def __lt__(self, other):
                """Defines less-than comparison behavior based on hierarchy levels."""
                return self.level < other.level

        hierarchy_levels = {}  # Dict[int, HierarchyDescriptor]

        local_tensors = self._local_tensors()
        func_id_to_func = {}

        def get_id(node_or_func):
            """Returns the unique ID for a Node object or a function."""
            if isinstance(node_or_func, Node):
                return self._get_node_id(node_or_func)
            return node_or_func.unique_id

        def get_hierarchy_level(node_or_func, visited=None):
            """Returns the hierarchy level of a node or function, with optional tracking of visited elements."""
            visited = misc.default_value(visited, set())
            visited.add(get_id(node_or_func))

            def get_inputs(node_or_func):
                """Find all nodes used by a given node or function."""

                def get_used_nodes(node):
                    """Find all nodes that are used as inputs by a given node."""
                    inputs = {}

                    def add_local_producers(tensor):
                        """Add local tensors and their producer nodes to the inputs dictionary."""
                        nonlocal inputs
                        if tensor.name in local_tensors:
                            for inp_node in tensor.inputs:
                                inputs[self._get_node_id(inp_node)] = inp_node

                    for tensor in node.inputs:
                        add_local_producers(tensor)

                    # If a node includes a subgraph, get any tensors that it uses from the outer graph.
                    for subgraph in node.subgraphs():
                        for tensor in subgraph._foreign_tensors().values():
                            add_local_producers(tensor)

                    return inputs.values()

                # Find all functions used in this list of nodes.
                def get_used_funcs(nodes):
                    """Return a dictionary of functions used in the provided list of nodes."""
                    inputs = {}
                    for subgraph in self.subgraphs():
                        inputs.update(get_used_funcs(subgraph.nodes))
                    for node in nodes:
                        func_id = (node.domain, node.op)
                        if func_id in func_id_to_func:
                            inputs[func_id] = func_id_to_func[func_id]
                    return inputs

                if isinstance(node_or_func, Node):
                    inputs = get_used_nodes(node_or_func)
                else:
                    inputs = get_used_funcs(node_or_func.nodes).values()
                return inputs

            if get_id(node_or_func) in hierarchy_levels:
                return hierarchy_levels[get_id(node_or_func)].level

            # The level of a node is the level of its highest input + 1.
            max_input_level = max(
                [get_hierarchy_level(inp, visited=visited) for inp in get_inputs(node_or_func)] + [-1]
            )
            visited.remove(get_id(node_or_func))

            hierarchy_levels[get_id(node_or_func)] = HierarchyDescriptor(node_or_func, level=max_input_level + 1)
            return max_input_level + 1

        if sort_nodes:
            with self.node_ids():
                for node in self.nodes:
                    hierarchy_levels[get_id(node)] = HierarchyDescriptor(node, level=get_hierarchy_level(node))
            self.nodes = [hd.node_or_func for hd in sorted(hierarchy_levels.values())]

        if sort_functions:
            self._merge_subgraph_functions()
            func_id_to_func.update({func.unique_id: func for func in self.functions})
            hierarchy_levels.clear()
            for func in self.functions:
                hierarchy_levels[func.unique_id] = HierarchyDescriptor(func, level=get_hierarchy_level(func))
            self.functions = [hd.node_or_func for hd in sorted(hierarchy_levels.values())]

        return self

    def tensors(self, check_duplicates=False):
        """
        Creates a tensor map of all the tensors used by this graph by walking over all nodes. Empty tensors are omitted
        from this map.

        Tensors are guaranteed to be in order of the nodes in the graph. Hence, if the graph is topologically sorted, the tensor map will be too.

        Args:
            check_duplicates (bool): Whether to fail if multiple tensors with the same name are encountered.

        Raises:
            OnnxGraphSurgeonException: If check_duplicates is True and multiple distinct tensors in the graph share the same name.

        Returns:
            OrderedDict[str, Tensor]: A mapping of tensor names to tensors.
        """
        tensor_map = OrderedDict()

        def add_to_tensor_map(tensor):
            """Add a tensor to the tensor_map if it is not empty and ensure no duplicate tensor names exist."""
            if not tensor.is_empty():
                if tensor.name in tensor_map and tensor_map[tensor.name] is not tensor:
                    msg = f"Found distinct tensors that share the same name:\n[id: {id(tensor_map[tensor.name])}] {tensor_map[tensor.name]}\n[id: {id(tensor)}] {tensor}\n"
                    msg += (
                        f"Note: Producer node(s) of first tensor:\n{tensor_map[tensor.name].inputs}\nProducer node(s) of second tensor:\n{tensor.inputs}"
                    )

                    if check_duplicates:
                        G_LOGGER.critical(msg)
                    # G_LOGGER.warning(msg)

                tensor_map[tensor.name] = tensor

        # I/O tensors may not be attached to nodes.
        for io_tensor in self.inputs:
            add_to_tensor_map(io_tensor)

        for node in self.nodes:
            for tensor in node.inputs + node.outputs:
                add_to_tensor_map(tensor)

        for io_tensor in self.outputs:
            add_to_tensor_map(io_tensor)

        return tensor_map

    def fold_constants(
        self,
        fold_shapes=True,
        recurse_subgraphs=True,
        partitioning=None,
        error_ok=True,
        flatten_subgraphs=True,
        size_threshold=None,
        should_exclude_node=None,
        recurse_functions=True,
    ):
        """
        Folds constants in-place in the graph. The graph's nodes and functions must be topologically sorted prior to
        calling this function (see `toposort()`).

        This function will not remove constants after folding them. In order to get rid of
        these hanging nodes, you can run the `cleanup()` function.

        *Note: Due to how this function is implemented, the graph must be exportable to ONNX,
        and evaluable in ONNX-Runtime. Additionally, ONNX-Runtime must be installed.*

        Args:
            fold_shapes (bool):
                    Whether to fold `Shape` nodes in the graph.
                    This requires shapes to be inferred in the graph, and can only fold
                    static shapes.
                    Defaults to True.
            recurse_subgraphs (bool):
                    Whether to recursively fold constants in subgraphs.
                    Defaults to True.
            partitioning (Union[str, None]):
                    Whether/How to partition the graph so that errors in folding one
                    part of a model do not affect other parts. Available modes are:

                    - None: Do not partition the graph. If inference fails, no constants are folded.
                    - "basic": Partition the graph. If inference fails in one partition, other partitions will
                            remain unaffected.
                    - "recursive": Partition the graph recursively. If inference fails in a partition, the partition
                            will be further partitioned.

                    Defaults to None.
            error_ok (bool):
                    Whether inference errors should be suppressed.
                    When this is False, any errors encountered during inference will be re-raised.
                    Defaults to True.
            flatten_subgraphs (bool):
                    Whether to flatten subgraphs where possible. For example, `If` nodes with a constant condition
                    can be flattened into the parent graph.
            size_threshold (int):
                    The maximum size threshold, in bytes, for which to fold constants.
                    Any tensors larger than this value will not be folded.
                    Set to ``None`` to disable the size threshold and always fold constants.
                    For example, some models may apply ops like `Tile` or `Expand` to constants, which can
                    result in very large tensors. Rather than pre-computing those constants and bloating
                    the model size, it may be desirable to skip folding them and allow them to be computed
                    at runtime.
                    Defaults to None.
            should_exclude_node (Callable[[gs.Node], bool]):
                    A callable that accepts an onnx-graphsurgeon node from the graph and reports whether it should
                    be excluded from folding. This is only called for nodes which are otherwise foldable.
                    Note that preventing a node from being folded also prevents its consumers from being folded.
                    Defaults to a callable that always returns False.
            recurse_functions (bool):
                    Whether to fold constants in this graph's Functions.
                    Defaults to True.

        Returns:
            self
        """
        from onnxslim.third_party.onnx_graphsurgeon.exporters.onnx_exporter import (
            dtype_to_onnx,
            export_onnx,
        )

        custom_should_exclude_node = misc.default_value(should_exclude_node, lambda node: False)

        # Don't fold nodes with attribute values which are variable.
        def should_exclude_node(node):
            """Determine if an ONNX graph node should be excluded based on its attributes."""
            for attr_val in node.attrs.values():
                if isinstance(attr_val, Node.AttributeRef):
                    return True
            return custom_should_exclude_node(node)

        PARTITIONING_MODES = [None, "basic", "recursive"]
        if partitioning not in PARTITIONING_MODES:
            G_LOGGER.critical(f"Argument for parameter 'partitioning' must be one of: {PARTITIONING_MODES}")
        ORT_PROVIDERS = ["CPUExecutionProvider"]

        G_LOGGER.debug(f"Folding constants in {self.name}")

        # We apply constant folding in 5 passes:
        # Pass 1 lowers 'Constant' nodes into Constant tensors.
        # Pass 2 elides casts applied to shape tensors. This is done separately from other shape folding
        #   since it operates on the original graph rather than a clone.
        # Pass 3 finds all Constant tensors in the graph, then finds all descendants which are dependent
        #   only on constants.
        # Pass 4 searches for Shape nodes that have variable inputs (i.e. not marked const in pass 1)
        #    and turns them into Constants iff the input has a statically known shape.
        # Pass 5 computes the descendants determined in Pass 3 using ONNX-Runtime and replaces them in the graph.

        # Pass 1: Lower constant nodes
        for tensor in self.tensors().values():
            if len(tensor.inputs) == 1:
                node = tensor.inputs[0]
                if node.op == "Constant" and tensor.outputs:
                    if len(node.attrs) != 1:
                        G_LOGGER.warning("Constant node must contain exactly one attribute")
                        continue
                    attr_name, attr_val = list(node.attrs.items())[0]
                    allowed_attrs = {
                        "value",
                        "value_float",
                        "value_floats",
                        "value_int",
                        "value_ints",
                    }
                    if attr_name not in allowed_attrs:
                        G_LOGGER.warning(f"Unsupported attribute for Constant node: {attr_name}")
                        continue
                    if isinstance(attr_val, Node.AttributeRef):
                        continue
                    elif isinstance(attr_val, Constant):
                        arr = attr_val._values  # Using ._values avoids copying
                    else:
                        arr = np.array(attr_val, dtype=tensor.dtype)
                    tensor.to_constant(arr)
                    tensor.inputs.clear()

        # Pass 2: Run shape-tensor cast elision
        def run_cast_elision(node):
            """Perform cast elision optimization on an ONNX node to eliminate unnecessary cast operations."""
            import onnx

            # Search for Cast(s) (from int -> float) -> intermediate operator (with float constants) -> Cast(s) (back to int)
            # This pattern is problematic for TensorRT since these operations may be performed on Shape Tensors, which
            # are not allowed to be floating point type. Attempt to fold the pattern here
            VALID_CAST_ELISION_OPS = {
                "Add",
                "Sub",
                "Mul",
                "Div",
                "Max",
                "Min",
                "Equal",
                "Greater",
                "Less",
                "Concat",
            }

            if node.op not in VALID_CAST_ELISION_OPS:
                return

            # If the uncasted outputs of this node have any consumers other than "Cast" nodes,
            # then we cannot elide the cast.
            for out_tensor in node.outputs:
                if out_tensor in self.outputs:
                    return

                if any(out_node.op != "Cast" for out_node in out_tensor.outputs):
                    return

            # Get list of input nodes that cast to float32
            inp_casts = [
                inp_node
                for inp_tensor in node.inputs
                for inp_node in inp_tensor.inputs
                if inp_node.op == "Cast" and inp_node.attrs["to"] == onnx.TensorProto.DataType.FLOAT
            ]

            # No cast nodes found, return early
            if not inp_casts:
                return

            # Ensure that all input cast nodes are casting from the same type
            inp_dtypes = [dtype_to_onnx(inp_cast.inputs[0].dtype) for inp_cast in inp_casts]
            if len(set(inp_dtypes)) != 1:
                return

            final_type = inp_dtypes[0]

            # Get list of output nodes that cast to int32 or int64
            out_casts = [
                out_node
                for out_tensor in node.outputs
                for out_node in out_tensor.outputs
                if out_node.op == "Cast"
                and out_node.attrs["to"] in {onnx.TensorProto.DataType.INT32, onnx.TensorProto.DataType.INT64}
            ]

            # No cast node found on outputs, return early
            if not out_casts:
                return

            # Ensure that all output cast nodes are casting to the same type and that this
            # matches the original type before the inputs were casted.
            out_dtypes = [out_cast.attrs["to"] for out_cast in out_casts]
            if len(set(out_dtypes)) != 1 or out_dtypes[0] != final_type:
                return

            # If all checks passed, reconnect inputs/outputs to the consumers/producers
            # of the Cast nodes.
            # Note that we need to be careful in how we rebind tensors since they may
            # be used by multiple nodes. Thus, it is not necessarily safe to assume that
            # `cast_node.inputs[0].outputs[0] == cast_node`.
            for index, inp in enumerate(node.inputs):
                if isinstance(inp, Constant):
                    inp.values = inp.values.astype(onnx.helper.tensor_dtype_to_np_dtype(final_type))

                for cast in inp_casts:
                    if cast.outputs[0] == inp:
                        node.inputs[index] = cast.inputs[0]

            for index, out in enumerate(node.outputs):
                for cast in out_casts:
                    if cast.inputs[0] == out:
                        out_tensor = cast.outputs[0]
                        out_tensor.inputs.clear()  # Disconnect from Cast
                        node.outputs[index] = out_tensor

        if fold_shapes:
            # Perform shape tensor cast elision prior to most other folding
            G_LOGGER.debug(f"Performing shape tensor cast elision in {self.name}")
            try:
                with self.node_ids():
                    for node in self.nodes:
                        run_cast_elision(node)
            except Exception as err:
                if not error_ok:
                    raise err
                G_LOGGER.warning("'{:}' routine failed with: {:}".format("Shape tensor cast elision", err))

        # Note that most of the remaining passes operate on a clone of the original graph.
        # Pass 3: Find all descendants of constant tensors

        graph_clone = self.copy()
        clone_tensors = graph_clone.tensors()

        # If 'self' is a Function, then these fields need to be set so it can be exported as an ONNX Graph.
        graph_clone.producer_name = ""
        graph_clone.producer_version = ""

        def update_foldable_outputs(graph_constants):
            """Updates the graph's outputs to ensure certain operations remain foldable."""

            def is_foldable(node):
                """Determines if a given node operation is foldable based on its type."""
                NO_FOLD_OPS = {
                    "QuantizeLinear",
                    "DequantizeLinear",
                    "DynamicQuantizeLinear",
                }
                if node.op in NO_FOLD_OPS:
                    return False

                def all_tensors_const(tensors):
                    """Check if all tensors in a given list are constants in the graph."""
                    return all(t.name in graph_constants for t in tensors if not t.is_empty())

                if not all_tensors_const(node.inputs):
                    return False

                all_subgraph_foreign_tensors_const = True
                for subgraph in node.subgraphs():
                    foreign_tensors = subgraph._foreign_tensors().values()
                    all_subgraph_foreign_tensors_const &= all_tensors_const(foreign_tensors)

                return all_subgraph_foreign_tensors_const and not should_exclude_node(node)

            # Walks along the outputs of graph_constants to see if they can also be computed statically.
            # Since the graph is topologically sorted, this should find all constant nodes in the graph.
            for node in graph_clone.nodes:
                if is_foldable(node):
                    graph_constants.update({out.name: out for out in node.outputs})
            return graph_constants

        graph_constants = {}
        for name, tensor in clone_tensors.items():
            if isinstance(tensor, Constant):
                if any((t.op == "Gather" and t.inputs.index(tensor) == 0) for t in tensor.outputs):
                    if len(tensor.outputs) <= 1:
                        graph_constants[name] = tensor
                else:
                    graph_constants[name] = tensor

        graph_constants = update_foldable_outputs(graph_constants)

        # Pass 4: Shape Folding

        def get_producer(tensor, op):
            """Get the producer of the specified tensor iff it matches op."""
            if len(tensor.inputs) != 1:
                return None

            node = tensor.inputs[0]
            return None if node.op != op else node

        def get_input(node, index=0):
            """Get the input tensor of a node iff the input tensor is not already marked a graph constant."""
            if node is None:
                return None

            inp = node.inputs[index]

            # If the input was already found to be a constant, it will be folded anyway.
            return None if inp.name in graph_constants else inp

        def get_scalar_value(tensor):
            """Gets the scalar value of a constant tensor with a single item."""
            return list(tensor.values)[0] if tensor.shape else tensor.values

        def fold_shape(tensor):
            """Returns the input tensor shape if available, otherwise returns None."""
            inp = get_input(get_producer(tensor, "Shape"))
            if inp is None:
                return None

            if inp.shape is None or misc.is_dynamic_shape(inp.shape):
                return None
            return np.array(inp.shape, dtype=np.int64)

        def fold_shape_gather(tensor):
            """Retrieves and returns the shape of the input tensor as a NumPy array, otherwise returns None."""
            gather = get_producer(tensor, "Gather")
            if gather is None:
                return None

            data = gather.inputs[0]
            indices_tensor = gather.inputs[1]

            inp = get_input(get_producer(data, "Shape"))
            if inp is None or inp.shape is None:
                return None

            if not isinstance(indices_tensor, Constant):
                return None

            indices = indices_tensor.values
            if not indices.shape:  # Scalar-case
                shape = inp.shape[int(indices)]
                if misc.is_dynamic_dimension(shape):
                    return None
            else:
                shape = [inp.shape[index] for index in indices]
                if misc.is_dynamic_shape(shape):
                    return None

            return np.array(shape, dtype=np.int64)

        def fold_shape_slice(tensor):
            """Fold tensor shape slice information into a NumPy array of int64 type."""
            slice = get_producer(tensor, "Slice")
            if slice is None:
                return None

            data = slice.inputs[0]

            if len(slice.inputs) >= 3:
                starts, ends = slice.inputs[1:3]
                if any(not isinstance(t, Constant) for t in [starts, ends]):
                    return None
                starts, ends = get_scalar_value(starts), get_scalar_value(ends)
            elif "starts" in slice.attrs and "ends" in slice.attrs:
                starts, ends = slice.attrs["starts"][0], slice.attrs["ends"][0]
            else:
                return None

            inp = get_input(get_producer(data, "Shape"))
            if inp is None or inp.shape is None:
                return None

            # For shape tensors, we can only slice on the 0th dimension.
            if len(slice.inputs) > 3:
                axes = slice.inputs[3]
                if not isinstance(axes, Constant):
                    return None

                if get_scalar_value(axes) != 0:
                    return None
            elif "axes" in slice.attrs:
                if slice.attrs["axes"][0] != 0:
                    return None

            steps = 1
            if len(slice.inputs) > 4:
                steps = slice.inputs[4]
                if not isinstance(steps, Constant):
                    return None
                steps = get_scalar_value(steps)
            elif "steps" in slice.attrs:
                steps = slice.attrs["steps"][0]

            shape = inp.shape[starts:ends:steps]
            if misc.is_dynamic_shape(shape):
                return None

            return np.array(shape, dtype=np.int64)

        if fold_shapes:
            # NOTE: The order of shape folding passes is important to maximize how much we fold (phase-ordering problem).
            SHAPE_FOLD_FUNCS = {fold_shape_gather, fold_shape_slice, fold_shape}
            for shape_fold_func in SHAPE_FOLD_FUNCS:
                try:
                    for tensor in clone_tensors.values():
                        shape_of = shape_fold_func(tensor)

                        if shape_of is not None:
                            G_LOGGER.ultra_verbose(f"Folding shape tensor: {tensor.name} to: {shape_of}")
                            graph_constants[tensor.name] = tensor.to_constant(shape_of)
                            graph_constants[tensor.name].inputs.clear()
                except Exception as err:
                    if not error_ok:
                        raise err
                    G_LOGGER.warning(f"'{shape_fold_func.__name__}' routine failed with:\n{err}")
                else:
                    graph_constants = update_foldable_outputs(graph_constants)

        # Pass 5: Evaluate all tensors descended from constants with ONNX-Runtime and replace them with constant values.

        def partition_and_infer(subgraph):
            """Evaluates and partitions the subgraph to infer constant values using ONNX-Runtime."""

            def get_out_node_ids():
                """Gets the final output nodes, identifying producer nodes of graph output tensors with no other
                outputs.
                """
                with subgraph.node_ids():
                    out_node_ids = set()
                    for out in subgraph.outputs:
                        if not out.outputs and not isinstance(out, Constant):
                            for n_inp in out.inputs:
                                out_node_ids.add(subgraph._get_node_id(n_inp))
                return out_node_ids

            # Compute each output node in a separate subgraph.
            out_node_ids = get_out_node_ids()
            constant_values = {}

            for index in out_node_ids:  # Have to use index since 'node' is not in part
                part = subgraph.copy()
                out_node = part.nodes[index]
                part.outputs = out_node.outputs
                part.name = f"Folding: {[out.name for out in part.outputs]}"
                part.cleanup(remove_unused_graph_inputs=True)
                names = [out.name for out in part.outputs]

                try:
                    # Determining types is not trivial, and ONNX-RT does its own type inference.
                    import onnxruntime as onnxrt

                    sess = onnxrt.InferenceSession(
                        export_onnx(part, do_type_check=False).SerializeToString(),
                        providers=ORT_PROVIDERS,
                    )
                    values = sess.run(names, {})
                except Exception as err:
                    G_LOGGER.warning(f"Inference failed for subgraph: {part.name}. Note: Error was:\n{err}")
                    if partitioning == "recursive":
                        G_LOGGER.verbose("Attempting to recursively partition subgraph")
                        # Partition failed, peel off last node.
                        # We only need to remove one node, so avoid doing an expensive call to cleanup()
                        part.outputs = out_node.inputs
                        del part.nodes[part.nodes.index(out_node)]
                        out_node.outputs.clear()
                        out_node.inputs.clear()
                    else:
                        G_LOGGER.info("You may see better results if you set partitioning='recursive'")
                        if not error_ok:
                            raise err

                    constant_values.update(partition_and_infer(part))
                else:
                    constant_values.update(dict(zip(names, values)))

            return constant_values

        # Only evaluate foldable values that have non-foldable outputs or are graph outputs.
        # Otherwise, if all the outputs are foldable, then we can just evaluate the outputs directly.
        # Additionally, if we can determine tensor size, do not evaluate tensors whose sizes exceed the size threshold.
        def should_eval_foldable(tensor):
            """Determine if foldable values should be evaluated based on output nature and tensor size constraints."""
            from onnxslim.third_party.onnx_graphsurgeon.importers.onnx_importer import (
                get_itemsize,
            )

            non_const = not isinstance(tensor, Constant)
            is_graph_output = not tensor.outputs
            has_non_foldable_outputs = any(out.name not in graph_constants for out in tensor.outputs)
            exceeds_size_threshold = (
                tensor.shape is not None
                and not misc.is_dynamic_shape(tensor.shape)
                and tensor.dtype is not None
                and size_threshold is not None
            ) and (misc.volume(tensor.shape) * get_itemsize(tensor.dtype) > size_threshold)

            return non_const and (is_graph_output or has_non_foldable_outputs) and not exceeds_size_threshold

        graph_clone.outputs = [t for t in graph_constants.values() if should_eval_foldable(t)]
        G_LOGGER.debug(f"Folding tensors: {graph_clone.outputs}")
        graph_clone.cleanup(remove_unused_graph_inputs=True, recurse_functions=False)

        # Using ._values avoids a deep copy of the values.
        constant_values = {
            name: tensor._values for name, tensor in graph_constants.items() if isinstance(tensor, Constant)
        }
        if graph_clone.outputs:
            if partitioning:
                constant_values.update(partition_and_infer(graph_clone))
            else:
                names = [t.name for t in graph_clone.outputs]
                try:
                    import os
                    import tempfile

                    import onnx
                    import onnxruntime as onnxrt

                    onnx_model = export_onnx(graph_clone, do_type_check=False)
                    if onnx_model.ByteSize() >= onnx.checker.MAXIMUM_PROTOBUF:
                        tmp_dir = tempfile.TemporaryDirectory()
                        tmp_path = os.path.join(tmp_dir.name, "tmp.onnx")
                        location = f"{os.path.basename(tmp_path)}.data"
                        if os.path.exists(location):
                            os.remove(location)
                        onnx.save(
                            onnx_model,
                            tmp_path,
                            save_as_external_data=True,
                            all_tensors_to_one_file=True,
                            location=location,
                        )
                        onnx_model = tmp_path
                    else:
                        onnx_model = onnx_model.SerializeToString()

                    sess = onnxrt.InferenceSession(
                        onnx_model,
                        providers=ORT_PROVIDERS,
                    )
                    values = sess.run(names, {})
                    constant_values.update(dict(zip(names, values)))
                except Exception as err:
                    G_LOGGER.warning(
                        "Inference failed. You may want to try enabling partitioning to see better results. "
                        f"Note: Error was:\n{err}"
                    )
                    G_LOGGER.verbose(f"Note: Graph was:\n{graph_clone}")
                    if not error_ok:
                        raise
        elif not constant_values:
            G_LOGGER.debug(
                f"Could not find any nodes in this graph ({self.name}) that can be folded. "
                "This could mean that constant folding has already been run on this graph. "
                "Skipping."
            )

        # Finally, replace the Variables in the original graph with constants.
        large_tensors = {}
        if constant_values:
            graph_tensors = self.tensors()
            for name, values in constant_values.items():
                tensor = graph_tensors[name]
                if isinstance(tensor, Constant) or not tensor.outputs:
                    # No need to fold tensors that are already constant.
                    continue

                if size_threshold is not None and values.nbytes > size_threshold:
                    G_LOGGER.debug(
                        f"Will not fold: '{name}' since its size in bytes ({values.nbytes}) exceeds the size threshold ({size_threshold})"
                    )
                    continue
                elif size_threshold is None and values.nbytes > (1 << 20):
                    large_tensors[name] = values.nbytes

                tensor.to_constant(values)
                tensor.inputs.clear()  # Constants do not need inputs

            if large_tensors:
                large_tensors_mib = {
                    tensor_name: f"{value // (1 << 20)} MiB" for tensor_name, value in large_tensors.items()
                }
                G_LOGGER.warning(
                    "It looks like this model contains foldable nodes that produce large outputs.\n"
                    "In order to avoid bloating the model, you may want to set a constant-folding size threshold.\n"
                    f"Note: Large tensors and their corresponding sizes were: {large_tensors_mib}",
                    mode=LogMode.ONCE,
                )

        # Folding subgraphs after the outer graph can lead to better folding.
        def fold_subgraphs():
            """Folds constants within subgraphs of the outer computational graph for optimization."""
            for subgraph in self.subgraphs():
                subgraph.fold_constants(
                    fold_shapes=fold_shapes,
                    recurse_subgraphs=recurse_subgraphs,
                    partitioning=partitioning,
                    error_ok=error_ok,
                    flatten_subgraphs=flatten_subgraphs,
                    size_threshold=size_threshold,
                    recurse_functions=False,  # Functions are folded later
                )

        if recurse_subgraphs:
            fold_subgraphs()

        if flatten_subgraphs:
            # Flatten conditional subgraphs
            index = 0
            while index < len(self.nodes):
                node = self.nodes[index]
                if node.op == "If" and isinstance(node.inputs[0], Constant):
                    G_LOGGER.debug(f"Flattening conditional: {node.name}")
                    cond = get_scalar_value(node.inputs[0])
                    subgraph = node.attrs["then_branch"] if cond else node.attrs["else_branch"]
                    # Need to add a suffix to subgraph tensors so they don't collide with outer graph tensors
                    for tensor in subgraph._local_tensors().values():
                        tensor.name += f"_subg_{index}_{subgraph.name}"

                    # The subgraph outputs correspond to the If node outputs. Only the latter are visible
                    # in the parent graph, so we rebind the producer nodes of the subgraph outputs to point
                    # to the output tensors of the If instead.
                    node_outputs = list(node.outputs)
                    for node_out, subgraph_out in zip(node_outputs, subgraph.outputs):
                        node_out.inputs.clear()
                        for producer in subgraph_out.inputs:
                            for tensor_idx, out_tensor in enumerate(producer.outputs):
                                if out_tensor == subgraph_out:
                                    producer.outputs[tensor_idx] = node_out

                    # Copy subgraph nodes into parent graph at the index of the If.
                    del self.nodes[index]
                    self.nodes[index:index] = subgraph.nodes
                    index += len(subgraph.nodes) - 1

                index += 1

        if recurse_functions:
            # Nodes which are constant-folded but not cleaned up can result in errors during inference,
            # so process functions in reverse topological order.
            for func in reversed(self.functions):
                func.fold_constants(
                    fold_shapes=fold_shapes,
                    recurse_subgraphs=recurse_subgraphs,
                    partitioning=partitioning,
                    error_ok=error_ok,
                    flatten_subgraphs=flatten_subgraphs,
                    size_threshold=size_threshold,
                    should_exclude_node=should_exclude_node,
                    recurse_functions=False,  # No infinite recursion
                )

        return self

    def _generate_name(self, prefix: str, existing_names: set):
        """Generate a unique name by appending an index to the given prefix, ensuring it does not clash with existing
        names.
        """
        # Generation is done by appending an index to the prefix.
        while True:
            name = f"{prefix}_{self.name_idx}"
            self.name_idx += 1
            if name not in existing_names:  # Ensure generated name is unique
                break
        return name

    def layer(self, inputs=None, outputs=None, *args, **kwargs):
        """
        Creates a node, adds it to this graph, and optionally creates its input and output tensors.

        The input and output lists can include various different types:

            - ``Tensor``:
                    Any Tensors provided will be used as-is in the inputs/outputs of the node created.
                    Therefore, you must ensure that the provided Tensors have unique names.
            - ``str``:
                    If a string is provided, this function will generate a new tensor using
                    the string to generate a name. It will append an index to the end of the provided string
                    to guarantee unique names.
            - ``numpy.ndarray``:
                    If a NumPy array is provided, this function will generate a Constant tensor
                    using the name prefix: "onnx_graphsurgeon_constant", and append an index to the end
                    of the prefix to guarantee unique names.
            - ``Union[List[Number], Tuple[Number]]``:
                    If a list or tuple of numbers (int or float) is provided, this function will
                    generate a Constant tensor using the name prefix: "onnx_graphsurgeon_lst_constant",
                    and append an index to the end of the prefix to guarantee unique names.
                    The values of the tensor will be a 1D array containing the specified values.
                    The datatype will be either `np.float32` or `np.int64`.

        Args:
            inputs (List[Union[Tensor, str, numpy.ndarray]]): The list of inputs
            outputs (List[Union[Tensor, str, numpy.ndarray]]): The list of outputs
            args/kwargs: These are passed directly to the constructor of Node

        Returns:
            List[Tensor]: The output tensors of the node
        """
        inputs = misc.default_value(inputs, [])
        outputs = misc.default_value(outputs, [])

        def process_io(io, existing_names):
            """Processes input/output elements, converting them to Tensor, Variable, or Constant, and ensuring unique
            names.
            """
            new_io = []
            for elem in io:
                if isinstance(elem, Tensor):
                    new_io.append(elem)
                elif isinstance(elem, str):
                    name = self._generate_name(elem, existing_names)
                    tensor = Variable(name=name)
                    new_io.append(tensor)
                elif isinstance(elem, np.ndarray):
                    name = self._generate_name("onnx_graphsurgeon_constant", existing_names)
                    new_io.append(Constant(name=name, values=elem))
                elif isinstance(elem, (list, tuple, numbers.Number)):
                    if isinstance(elem, (list, tuple)):
                        dtype = np.float32 if any(isinstance(x, float) for x in elem) else np.int64
                    else:
                        dtype = np.float32 if isinstance(elem, float) else np.int64
                    arr = np.array(elem, dtype=dtype)
                    name = self._generate_name("onnx_graphsurgeon_lst_constant", existing_names)
                    new_io.append(Constant(name=name, values=arr))
                else:
                    G_LOGGER.critical(
                        f"Unrecognized type passed to Graph.layer: {elem}.\n"
                        "\tHint: Did you forget to unpack a list with `*`?\n"
                        "\tPlease use Tensors, strings, or NumPy arrays."
                    )
                if new_io[-1].name:
                    existing_names.add(new_io[-1].name)
            return new_io

        existing_names = set(self.tensors().keys())  # set for fast lookup
        inputs = process_io(inputs, existing_names)
        outputs = process_io(outputs, existing_names)

        if "name" not in kwargs:
            kwargs["name"] = self._generate_name("onnx_graphsurgeon_node", {node.name for node in self.nodes})

        node = Node(*args, **kwargs, inputs=inputs, outputs=outputs)
        self.nodes.append(node)
        return node.outputs

    def copy(self, tensor_map: "OrderedDict[str, Tensor]" = None):
        """
        Copy the graph.

        This makes copies of all nodes and tensors in the graph, but will not
        do a deep-copy of weights or attributes (with the exception of ``Graph``
        attributes, which will be copied using their ``copy`` method).

        Args:
            tensor_map (OrderedDict[str, Tensor]):
                A mapping of tensor names to tensors from the outer graph.
                This should be ``None`` if this is the outer-most graph.

        Returns:
            Graph: A copy of the graph.
        """
        # First, reconstruct each tensor in the graph, but with no inputs or outputs
        tensor_map = copy.copy(misc.default_value(tensor_map, {}))

        local_tensor_copies = {}
        # When we're cloning a subgraph by itself, we need to use `tensors()` to get all
        # required tensors - even those produced by outer graphs.
        local_tensor_copies.update({n: t.copy() for n, t in self.tensors().items()})
        # However, we should prioritize copies already made by the outer graph.
        local_tensor_copies.update(tensor_map)
        # And locally produced tensors should take precedence over everything else.
        local_tensor_copies.update({n: t.copy() for n, t in self._local_tensors().items()})

        def get_tensor(name):
            """Retrieve a tensor by its name from local copies, or return an empty variable if no name is provided."""
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

        new_graph_inputs = [get_tensor(inp.name) for inp in self.inputs]
        new_graph_outputs = [get_tensor(out.name) for out in self.outputs]
        return Graph(
            nodes=new_nodes,
            inputs=new_graph_inputs,
            outputs=new_graph_outputs,
            name=copy.copy(self.name),
            doc_string=copy.copy(self.doc_string),
            opset=copy.copy(self.opset),
            import_domains=self.import_domains,
            functions=copy.copy(self.functions),
        )

    def __str__(self):
        """Return a string representation of the graph including its name, opset, local functions, inputs, nodes, and
        outputs.
        """
        nodes_str = "\n".join([str(node) for node in self.nodes])
        functions_str = ",".join([str(func.name) for func in self.functions])
        out = f"Graph {self.name} (Opset {self.opset})"
        out += f"\nLocal Functions: [{functions_str}]"
        out += f"\nInputs: {self.inputs}"
        out += f"\nNodes: {nodes_str}"
        out += f"\nOutputs: {self.outputs}"
        return out

    def __repr__(self):
        """Returns a string representation of the object."""
        return self.__str__()
