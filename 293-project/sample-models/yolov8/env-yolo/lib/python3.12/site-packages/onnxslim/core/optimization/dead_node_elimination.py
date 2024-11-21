import logging

import numpy as np

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.utils import delete_node
from onnxslim.third_party.onnx_graphsurgeon.exporters.onnx_exporter import dtype_to_onnx
from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Constant, Variable

logger = logging.getLogger("onnxslim")


def dead_node_elimination(graph, is_subgraph=False):
    """Perform in-place constant folding optimizations on the given computational graph by eliminating redundant
    nodes.
    """
    for subgraph in graph.subgraphs():
        dead_node_elimination(subgraph, is_subgraph=True)

    for node in graph.nodes:
        if node.op in {"Identity", "Dropout"}:
            if not is_subgraph:
                delete_node(node)
                logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Pad":
            if len(node.inputs) > 1 and isinstance(node.inputs[1], Constant):
                pad_value = node.inputs[1].values.tolist()
                pad_value = pad_value if isinstance(pad_value, list) else [pad_value]
                if all(value == 0 for value in pad_value):
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Cast":
            inp_dtype = [dtype_to_onnx(input.dtype) for input in node.inputs][0]
            if inp_dtype == node.attrs["to"]:
                delete_node(node)
                logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Reshape":
            if (node.inputs[0].shape and len(node.inputs[0].shape) == 1) and (
                node.outputs[0].shape and len(node.outputs[0].shape) == 1
            ):
                delete_node(node)
                logger.debug(f"removing {node.op} op: {node.name}")
            elif node.inputs[0].shape and node.outputs[0].shape and node.inputs[0].shape == node.outputs[0].shape:
                delete_node(node)
                logger.debug(f"removing {node.op} op: {node.name}")
            else:
                node_output_shape = node.outputs[0].shape
                if node_output_shape and check_shape(node_output_shape) and not isinstance(node.inputs[1], gs.Constant):
                    shapes = [shape if isinstance(shape, int) else -1 for shape in node_output_shape]
                    reshape_const = gs.Constant(
                        f"{node.inputs[1].name}_",
                        values=np.array(shapes, dtype=np.int64),
                    )
                    node.inputs.pop(1)
                    node.inputs.insert(1, reshape_const)
                    logger.debug(f"replacing {node.op} op: {node.name}")
        elif node.op == "Mul":
            if (isinstance(node.inputs[1], Constant) and isinstance(node.inputs[0], Variable)) or (
                isinstance(node.inputs[0], Constant) and isinstance(node.inputs[1], Variable)
            ):
                idx, constant_variable = get_constant_variable(node, return_idx=True)
                if np.all(constant_variable.values == 1):
                    var_idx = 0 if idx == 1 else 1
                    delete_node(node, var_idx)
                    logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Add":
            if (isinstance(node.inputs[1], Constant) and isinstance(node.inputs[0], Variable)) or (
                isinstance(node.inputs[0], Constant) and isinstance(node.inputs[1], Variable)
            ):
                idx, constant_variable = get_constant_variable(node, return_idx=True)
                value = constant_variable.values
                var_idx = 0 if idx == 1 else 1
                if value.ndim == 0 and value == 0:
                    delete_node(node, var_idx)
                    logger.debug(f"removing {node.op} op: {node.name}")
                elif np.all(value == 0) and (node.inputs[0].shape == node.outputs[0].shape):
                    delete_node(node, var_idx)
                    logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Expand":
            # tests/test_onnx_nets.py::TestTimmClass::test_timm[lambda_resnet26rpt_256]
            if len(node.inputs) > 1 and isinstance(node.inputs[1], Constant):
                constant_variable = node.inputs[1]
                value = constant_variable.values
                if value.ndim == 0 and value == 1:
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")
                elif np.all(value == 1) and (node.inputs[0].shape == node.outputs[0].shape):
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Concat":
            if len(node.inputs) == 1:
                delete_node(node)
                logger.debug(f"removing {node.op} op: {node.name}")
            else:
                for input in node.inputs:
                    if isinstance(input, Constant) and input.values.size == 0:
                        node.inputs.remove(input)
        elif node.op == "Sub":
            if isinstance(node.inputs[1], Constant) and isinstance(node.inputs[0], Variable):
                constant_variable = node.inputs[1]
                value = constant_variable.values
                if value.ndim == 0 and value == 0:
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")
                elif np.all(value == 0) and (node.inputs[0].shape == node.outputs[0].shape):
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")
        elif node.op == "Div":
            if isinstance(node.inputs[1], Constant) and isinstance(node.inputs[0], Variable):
                constant_variable = node.inputs[1]
                value = constant_variable.values
                if value.ndim == 0 and value == 1:
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")
                elif np.all(value == 1) and (node.inputs[0].shape == node.outputs[0].shape):
                    delete_node(node)
                    logger.debug(f"removing {node.op} op: {node.name}")


def check_shape(shapes):
    """Verify that 'shapes' contains exactly one string and all other elements are positive integers."""
    string_count = 0
    non_negative_int_count = 0

    for item in shapes:
        if isinstance(item, str):
            string_count += 1
        elif isinstance(item, int) and item > 0:
            non_negative_int_count += 1

    return (string_count == 1 and non_negative_int_count == len(shapes) - 1) or non_negative_int_count == len(shapes)


def get_constant_variable(node, return_idx=False):
    """Return the first constant variable found in a node's inputs, optionally including the index."""
    for idx, input in enumerate(list(node.inputs)):
        if isinstance(input, Constant):
            return (idx, input) if return_idx else input
