import logging
import os
import tempfile

import numpy as np
import onnx
from onnx import checker

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.optimization import optimize_model
from onnxslim.core.utils import delete_node
from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Constant
from onnxslim.third_party.symbolic_shape_infer import SymbolicShapeInference
from onnxslim.utils import save

logger = logging.getLogger("onnxslim")


DEBUG = bool(os.getenv("ONNXSLIM_DEBUG"))
AUTO_MERGE = True if os.getenv("ONNXSLIM_AUTO_MERGE") is None else bool(int(os.getenv("ONNXSLIM_AUTO_MERGE")))


def input_shape_modification(model: onnx.ModelProto, input_shapes: str) -> onnx.ModelProto:
    """Modifies input tensor shapes in the ONNX model according to the specified input_shapes string."""
    if not input_shapes:
        return

    graph = gs.import_onnx(model)
    input_names = [input.name for input in graph.inputs]
    tensors = graph.tensors()

    for input_shape in input_shapes:
        key, values = input_shape.rsplit(":", 1)
        values_list = [int(value) for value in values.split(",")]
        if key not in input_names:
            raise Exception(f"Input name {key} not found in model, available keys: {' '.join(input_names)}")
        tensors[key].shape = values_list

    for tensor in tensors.values():
        if tensor.name not in input_names:
            if isinstance(tensor, Constant):
                continue
            tensor.shape = None

    model = gs.export_onnx(graph)

    return model


def output_modification(model: onnx.ModelProto, outputs: str) -> onnx.ModelProto:
    """Modifies the output layers of the ONNX model based on specified output names and data types."""
    graph = gs.import_onnx(model)
    graph.outputs.clear()
    tensors = graph.tensors()
    for output in outputs:
        values = output.rsplit(":", 1)
        if len(values) == 1:
            key = values[0]
            if key not in tensors.keys():
                raise Exception(f"Output name {key} not found in model, available keys: {' '.join(tensors.keys())}")
            dtype = tensors[key].dtype
            if dtype is None:
                dtype = np.float32
                logger.warning(f"Output layer {key} has no dtype, set to default {dtype}")
        else:
            key, dtype = values
            if dtype == "fp16":
                dtype = np.float16
            elif dtype == "fp32":
                dtype = np.float32
            elif dtype == "int32":
                dtype = np.int32
            elif dtype == "bool":
                dtype = bool
            else:
                raise Exception(f"Output layer {key} assigned unsupported dtype {dtype}")

        graph.outputs.append(tensors[key].to_variable(dtype=dtype, shape=tensors[key].shape))

    graph.cleanup(remove_unused_graph_inputs=True).toposort()
    model = gs.export_onnx(graph)

    return model


def input_modification(model: onnx.ModelProto, inputs: str) -> onnx.ModelProto:
    """Modifies the output layers of the ONNX model based on specified output names and data types."""
    graph = gs.import_onnx(model)
    graph.inputs.clear()
    tensors = graph.tensors()
    for input in inputs:
        values = input.rsplit(":", 1)
        if len(values) == 1:
            key = values[0]
            if key not in tensors.keys():
                raise Exception(f"Input name {key} not found in model, available keys: {' '.join(tensors.keys())}")
            dtype = tensors[key].dtype
            if dtype is None:
                dtype = np.float32
                logger.warning(f"Input layer {key} has no dtype, set to default {dtype}")
        else:
            key, dtype = values
            if dtype == "fp16":
                dtype = np.float16
            elif dtype == "fp32":
                dtype = np.float32
            elif dtype == "int32":
                dtype = np.int32
            elif dtype == "bool":
                dtype = bool
            else:
                raise Exception(f"Output layer {key} assigned unsupported dtype {dtype}")

        graph.inputs.append(tensors[key].to_variable(dtype=dtype, shape=tensors[key].shape))

    graph.cleanup(remove_unused_graph_inputs=True).toposort()
    model = gs.export_onnx(graph)

    return model


def shape_infer(model: onnx.ModelProto):
    """Infer tensor shapes in an ONNX model using symbolic and static shape inference techniques."""
    logger.debug("Start shape inference.")
    try:
        logger.debug("try onnxruntime shape infer.")
        model = SymbolicShapeInference.infer_shapes(model, auto_merge=AUTO_MERGE)
    except Exception as err:
        logger.debug(f"onnxruntime shape infer failed, try onnx shape infer. {err}")
        if model.ByteSize() >= checker.MAXIMUM_PROTOBUF:
            tmp_dir = tempfile.TemporaryDirectory()
            tmp_path = os.path.join(tmp_dir.name, "tmp.onnx")
            tmp_infer_path = os.path.join(tmp_dir.name, "tmp_infer.onnx")
            save(model, tmp_path)
            onnx.shape_inference.infer_shapes_path(tmp_path, tmp_infer_path)
            model = onnx.load(tmp_infer_path)
        else:
            model = onnx.shape_inference.infer_shapes(model)
    if DEBUG:
        onnx.save(model, "debug_shape_infer.onnx")
    logger.debug("Finish shape inference.")
    return model


def optimize(model: onnx.ModelProto, skip_fusion_patterns: str = None):
    """Optimize the given ONNX model with options to skip specific fusion patterns and return the optimized model."""
    logger.debug("Start converting model to gs.")
    graph = gs.import_onnx(model).toposort()
    logger.debug("Finish converting model to gs.")
    logger.debug("Start constant folding.")
    graph.fold_constants().cleanup().toposort()
    logger.debug("Finish constant folding.")
    logger.debug("Start optimize model.")
    model = optimize_model(graph, skip_fusion_patterns)
    logger.debug("Finish optimize model.")
    if DEBUG:
        onnx.save(model, "debug_slim.onnx")

    return model


def convert_data_format(model: onnx.ModelProto, dtype: str) -> onnx.ModelProto:
    """Convert ONNX model data format to specified dtype, supporting 'fp16' and 'fp32'."""
    if dtype == "fp16":
        from onnxconverter_common import float16

        model = float16.convert_float_to_float16(model)
    elif dtype == "fp32":
        graph = gs.import_onnx(model).toposort()

        for node in graph.nodes:
            if node.op == "Cast":
                inp_dtype = [input.dtype for input in node.inputs][0]
                if inp_dtype in {np.float16, np.float32}:
                    delete_node(node)

        for tensor in graph.tensors().values():
            if isinstance(tensor, gs.Variable) and tensor.dtype == np.float16:
                tensor.dtype = np.float32
            elif isinstance(tensor, gs.Constant) and tensor.dtype == np.float16:
                tensor.values = tensor.values.astype(np.float32)

        graph.cleanup(remove_unused_graph_inputs=True).toposort()
        model = gs.export_onnx(graph)

    return model


def freeze(model: onnx.ModelProto):
    """Freeze the input layers of an ONNX model by removing the initializers from the input graph."""
    inputs = model.graph.input
    name_to_input = {}
    for input in inputs:
        if input.name in name_to_input:
            logger.warning(f"Duplicate input name: {input.name}")
        name_to_input[input.name] = input

    for initializer in model.graph.initializer:
        if initializer.name in name_to_input:
            inputs.remove(name_to_input[initializer.name])
            name_to_input.pop(initializer.name)
