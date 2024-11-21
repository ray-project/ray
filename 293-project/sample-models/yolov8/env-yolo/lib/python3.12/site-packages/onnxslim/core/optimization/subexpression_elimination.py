import logging

from onnxslim.core.pattern import get_node_users
from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Variable

logger = logging.getLogger("onnxslim")


def find_and_remove_replaceable_nodes(nodes):
    """Find and remove duplicate or replaceable nodes in a given list of computational graph nodes."""

    def get_node_key(node):
        input_names = []
        for input_node in node.inputs:
            if isinstance(input_node, Variable):
                input_names.append(input_node.name)
        return "_".join(input_names) if input_names else None

    def replace_node_references(existing_node, to_be_removed_node):
        users = get_node_users(to_be_removed_node)
        for user in users:
            for inp in user.inputs:
                if inp in to_be_removed_node.outputs:
                    index = user.inputs.index(inp)
                    user.inputs.pop(index)
                    user.inputs.insert(index, existing_node.outputs[0])

        to_be_removed_node.inputs.clear()
        to_be_removed_node.outputs.clear()

    node_dict = {}
    for node in nodes:
        key = get_node_key(node)
        if key:
            if key in node_dict:
                node_dict[key].append(node)
            else:
                node_dict[key] = [node]

    for key, bucketed_nodes in node_dict.items():
        if len(bucketed_nodes) > 1:
            keep_nodes = [True] * len(bucketed_nodes)
            for i, node in enumerate(bucketed_nodes):
                if keep_nodes[i]:
                    for j in range(i + 1, len(bucketed_nodes)):
                        if keep_nodes[j]:
                            logger.debug(f"node.op {bucketed_nodes[i].op} idx i: {i}, idx j: {j}")
                            if can_be_replaced(node, bucketed_nodes[j]):
                                keep_nodes[j] = False
                                existing_node = node
                                to_be_removed_node = bucketed_nodes[j]
                                replace_node_references(existing_node, to_be_removed_node)
                                logger.debug(f"Node {to_be_removed_node.name} can be replaced by {existing_node.name}")


def sequences_equal(seq1, seq2):
    """Check if two sequences are equal by comparing their lengths and elements."""
    length_match = len(seq1) == len(seq2)
    if not length_match:
        return False

    return all(elem1 == elem2 for elem1, elem2 in zip(seq1, seq2))


def can_be_replaced(node, other_node):
    """Check if two nodes can be replaced based on their operations, attributes, and inputs."""
    attrs_match = node.op == other_node.op and node.attrs == other_node.attrs
    node_input = [input for input in node.inputs if not input.is_empty()]
    other_node_input = [input for input in other_node.inputs if not input.is_empty()]
    inputs_match = sequences_equal(node_input, other_node_input)

    return attrs_match and inputs_match


def subexpression_elimination(graph):
    """Perform subexpression elimination on a computational graph to optimize node operations."""
    nodes_by_op = {}

    for node in graph.nodes:
        op = node.op
        if op not in nodes_by_op:
            nodes_by_op[op] = []
        nodes_by_op[op].append(node)

    for nodes in nodes_by_op.values():
        find_and_remove_replaceable_nodes(nodes)
