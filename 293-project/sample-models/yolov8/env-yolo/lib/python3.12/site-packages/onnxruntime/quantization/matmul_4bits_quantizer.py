# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from __future__ import annotations

import argparse
import copy
import importlib
import logging
import os

import numpy as np
import numpy.typing as npt
import onnx
from onnx.onnx_pb import GraphProto, ModelProto, NodeProto, TensorProto
from packaging import version

from onnxruntime.capi._pybind_state import quantize_matmul_4bits, quantize_qdq_matmul_4bits

from .calibrate import CalibrationDataReader
from .onnx_model import ONNXModel
from .quant_utils import QuantFormat, attribute_to_kwarg

logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s] - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


class WeightOnlyQuantConfig:
    def __init__(self, algorithm, quant_format):
        """This is the Base class for Weight Only Quant Configuration.

        Args:
            algorithm:
                weight only quantize algorithm name.
            quant_format: QuantFormat{QOperator, QDQ}.
                QOperator format quantizes the model with quantized operators directly.
                QDQ format quantize the model by inserting QuantizeLinear/DeQuantizeLinear on the tensor.
        """
        self.algorithm = algorithm
        self.quant_format = quant_format


class RTNWeightOnlyQuantConfig(WeightOnlyQuantConfig):
    def __init__(
        self,
        ratios=None,
        quant_format=QuantFormat.QOperator,
    ):
        """
        This is a class for round-to-nearest (RTN) algorithm Weight Only Quant Configuration.
        RTN is the most straightforward way to quantize weight using scale maps.

        Args:
            ratios:
                percentile of clip. Defaults to {}.
            quant_format (QuantFormat{QOperator, QDQ}, optional):
                QOperator format quantizes the model with quantized operators directly.
                QDQ format quantize the model by inserting QuantizeLinear/DeQuantizeLinear on the tensor.
                Defaults to QuantFormat.QOperator.
        """
        assert quant_format == QuantFormat.QOperator, "RTN only supports QOperator format"

        if ratios is None:
            ratios = {}
        super().__init__(
            algorithm="RTN",
            quant_format=quant_format,
        )
        self.ratios = ratios


class GPTQWeightOnlyQuantConfig(WeightOnlyQuantConfig):
    def __init__(
        self,
        calibration_data_reader: CalibrationDataReader,
        percdamp=0.01,
        block_size=128,
        actorder=False,
        mse=False,
        perchannel=True,
        quant_format=QuantFormat.QOperator,
    ):
        """
        This is a class for GPTQ algorithm Weight Only Quant Configuration.
        GPTQ algorithm provides more accurate quantization but requires more computational resources.

        Args:
            calibration_data_reader:
                a calibration data reader. It enumerates calibration data and generates inputs for the original model.
            percdamp:
                percent of the average Hessian diagonal to use for dampening.
            block_size (int, optional):
                channel number in one block to execute a GPTQ quantization iteration.
            actorder (bool, optional):
                whether rearrange Hessian matrix considering the diag's value.
            mse (bool, optional):
                whether get scale and zero point with mse error.
            perchannel (bool, optional):
                whether quantize weight per-channel.
            quant_format (QuantFormat{QOperator, QDQ}, optional):
                QOperator format quantizes the model with quantized operators directly.
                QDQ format quantize the model by inserting QuantizeLinear/DeQuantizeLinear on the tensor.
                Defaults to QuantFormat.QOperator.
        """
        assert quant_format == QuantFormat.QOperator, "GPTQ only supports QOperator format"

        super().__init__(
            algorithm="GPTQ",
            quant_format=quant_format,
        )
        self.calibration_data_reader = calibration_data_reader
        self.percdamp = percdamp
        self.block_size = block_size
        self.actorder = actorder
        self.mse = mse
        self.perchannel = perchannel


class HQQWeightOnlyQuantConfig(WeightOnlyQuantConfig):
    def __init__(
        self,
        block_size=128,
        bits=4,
        axis=1,
        quant_format=QuantFormat.QOperator,
    ):
        """
        This is a class for HQQ algorithm Weight Only Quant Configuration.
        HQQ algorithm quant weight without needing calibrate data.

        Args:
            block_size (int, optional):
                channel number in one block to execute a HQQ quantization iteration.
            bits (int, optional):
                how many bits to represent weight.
            axis (int, optional):
                0 or 1. which axis to quantize. https://arxiv.org/pdf/2309.15531.pdf
            quant_format (QuantFormat{QOperator, QDQ}, optional):
                QOperator format quantizes the model with quantized operators directly.
                QDQ format quantize the model by inserting QuantizeLinear/DeQuantizeLinear on the tensor.
                Defaults to QuantFormat.QOperator.
        """
        assert quant_format == QuantFormat.QOperator, "HQQ only supports QOperator format"

        super().__init__(
            algorithm="HQQ",
            quant_format=quant_format,
        )
        self.block_size = block_size
        self.bits = bits
        self.axis = axis


class DefaultWeightOnlyQuantConfig(WeightOnlyQuantConfig):
    def __init__(
        self,
        block_size: int = 128,
        is_symmetric: bool = False,
        accuracy_level: int | None = None,
        quant_format=QuantFormat.QOperator,
    ):
        """
        This is a class for weight only affine quantization configuration.

        Args:
            block_size (int, optional):
                channel number in one block to execute an affine quantization iteration.
            is_symmetric (bool, optional):
                whether quantize weight symmetrically.
            accuracy_level (int, optional):
                Accuracy level of the 4-bit quantized MatMul computation.
                Refer to the MatMulNBits contrib op's 'accuracy_level' attribute for details.
                (https://github.com/microsoft/onnxruntime/blob/main/docs/ContribOperators.md#commicrosoftmatmulnbits)
            quant_format (QuantFormat{QOperator, QDQ}, optional):
                QOperator format quantizes the model with quantized operators directly.
                QDQ format quantize the model by inserting QuantizeLinear/DeQuantizeLinear on the tensor.
                Defaults to QuantFormat.QOperator.
        """
        super().__init__(algorithm="DEFAULT", quant_format=quant_format)
        self.block_size = block_size
        self.is_symmetric = is_symmetric
        self.bits = 4
        self.accuracy_level = accuracy_level


def is_divisible(val1, val2):
    return int(val2 * np.ceil(val1 / val2)) == val1


class HQQWeightOnlyQuantizer:
    def __init__(
        self,
        config: HQQWeightOnlyQuantConfig,
    ):
        self.config = config

    # Proximal solver || weight - dequantize(quantize(weight))||_p^p
    @staticmethod
    def optimize_weights(
        tensor,
        scale,
        zero,
        min_max: list[int],
        axis: int = 0,
        opt_params: dict = None,  # noqa: RUF013
        verbose=False,
    ):
        import torch

        opt_params = {"lp_norm": 0.7, "beta": 1e1, "kappa": 1.01, "iters": 20} if opt_params is None else opt_params
        lp_norm, beta, kappa, iters = (
            opt_params["lp_norm"],
            opt_params["beta"],
            opt_params["kappa"],
            opt_params["iters"],
        )

        dtype = torch.float16 if tensor.is_cuda else torch.float32
        w_f = tensor.to(dtype)
        scale = scale.to(dtype)
        zero = zero.to(dtype)

        if lp_norm == 1:

            def shrink_op(x, beta):
                return torch.sign(x) * torch.nn.functional.relu(torch.abs(x) - 1.0 / beta)

        else:

            def shrink_op(x, beta, p=lp_norm):
                return torch.sign(x) * torch.nn.functional.relu(
                    torch.abs(x) - (1.0 / beta) * torch.pow(torch.abs(x) + 1e-8, p - 1)
                )

        best_error = 1e4
        for i in range(iters):
            w_q = torch.round(w_f * scale + zero).clamp(min_max[0], min_max[1])
            w_r = (w_q - zero) / scale
            w_e = shrink_op(w_f - w_r, beta)
            zero = torch.mean(w_q - (w_f - w_e) * scale, axis=axis, keepdim=True)
            beta *= kappa

            current_error = float(torch.abs(w_f - w_r).mean())
            if verbose:
                print(i, np.round(current_error, 6))
            if current_error < best_error:
                best_error = current_error
            else:
                break

        del w_f, w_q, w_r, w_e

        return scale, zero

    @staticmethod
    def pack_on_row_fast_248bit(pack_tensor, ori_int_tensor, bits):
        if pack_tensor.shape[0] == ori_int_tensor.shape[0]:
            ori_int_tensor = ori_int_tensor.T
            pack_tensor = pack_tensor.T
        if bits in [2, 4, 8]:
            compress_ratio = pack_tensor.element_size() * 8 // bits
            for j in range(compress_ratio):
                pack_tensor[0:] |= ori_int_tensor[j::compress_ratio] << (bits * (j))
        else:
            raise NotImplementedError("Only 2,4,8 bits are supported.")

    # from Official implementation of Half-Quadratic Quantization (HQQ)
    def quantize_internal(
        self, tensor, bits=4, channel_wise=True, group_size=64, optimize=True, round_zero=True, axis=1
    ):
        import torch

        weight = tensor.float()
        ori_shape = weight.shape

        pad_len = (group_size - ori_shape[axis] % group_size) % group_size
        if axis == 1:
            weight = torch.nn.functional.pad(weight, (0, pad_len), "constant", 0)
        else:
            weight = torch.nn.functional.pad(weight, (0, 0, 0, pad_len), "constant", 0)
        shape = weight.shape

        # Reshape for grouping
        if (group_size is not None) and channel_wise:
            weight = weight.reshape([-1, group_size]) if (axis == 1) else weight.reshape([group_size, -1])

        # Get min/max values
        if channel_wise is False:
            _min, _max = weight.min(), weight.max()
            optimize = False
        else:
            _min = weight.min(axis=axis, keepdim=True)[0]
            _max = weight.max(axis=axis, keepdim=True)[0]

        max_v = 2**bits - 1
        min_v = 0
        min_max = [min_v, max_v]

        # Note: here we work with the inverse of the scale to avoid division and quantize instead via weight*scale + zero, the scale is inverted later on.
        # clamp to avoid half-precision problems
        scale = (max_v / (_max - _min)).clamp(max=2e4)
        #!!!!!!!!!!!!!!!
        min_max_axis = _max - _min
        if (min_max_axis == 0).sum().item() > 0:
            min_max_axis[min_max_axis == 0] = max_v
            scale = (max_v / min_max_axis).clamp(max=2e4)
        zero = -_min * scale

        if round_zero:
            zero = torch.round(zero)

        # Fine-tune weights
        if optimize:
            scale, zero = self.optimize_weights(tensor=weight, scale=scale, zero=zero, min_max=min_max, axis=axis)

        # Quantize
        # Necessary for fake quantization backprop
        w_q = torch.round(weight * scale + zero).clamp(min_max[0], min_max[1])
        w_q = w_q.reshape(shape).int()

        scale = 1.0 / scale
        if axis == 1:
            scale = scale.reshape(shape[0], -1)
            zero = zero.reshape(shape[0], -1)
        else:
            scale = scale.reshape(-1, shape[-1])
            zero = zero.reshape(-1, shape[-1])
        # cleanup
        del weight, _min, _max

        return w_q, scale.to(tensor.dtype), zero.to(tensor.dtype)

    def quantize(self, node: NodeProto, graph_stack: list[GraphProto]) -> list[NodeProto]:
        """
        If the node is MatMul with fp32 const weight, quantize the weight with int4, and return the new node.
        If QOperator format, return MatMulNbits. If QDQ format, return DeQuantizeLinear + MatMul.
        """
        if node.op_type != "MatMul":
            return [node]  # only care about MatMul for now
        import torch

        logger.info(f"start to quantize {node.name} ...")
        input_b = node.input[1]
        b_pb, bs_graph = get_initializer(input_b, graph_stack)
        if b_pb is None:
            logger.info("MatMul doesn't have const weight. Skip to quantize")
            return [node]  # only care about constant weight

        b_array = onnx.numpy_helper.to_array(b_pb)
        if len(b_array.shape) != 2:
            logger.info("MatMul weight is not 2D. Skip to quantize")
            return [node]  # can only process 2-D matrix
        b_array_torch = torch.from_numpy(b_array)
        if torch.cuda.is_available():
            b_array_torch = b_array_torch.cuda()
        quant_weight_torch, scales_torch, zero_points_torch = self.quantize_internal(
            b_array_torch.T, bits=self.config.bits, group_size=self.config.block_size
        )
        quant_weight_torch = quant_weight_torch.contiguous()
        scales_torch = scales_torch.contiguous()
        zero_points_torch = zero_points_torch.contiguous()

        packed_torch = torch.zeros(
            (quant_weight_torch.shape[0], quant_weight_torch.shape[1] // 2),
            dtype=torch.uint8,
            device=quant_weight_torch.device,
        )
        self.pack_on_row_fast_248bit(packed_torch, quant_weight_torch, self.config.bits)
        scales = scales_torch.cpu().numpy()
        zero_points = zero_points_torch.cpu().numpy()
        # reshape to the predefined shape in MatmulNbits
        scales = scales.reshape(-1)
        zero_points = zero_points.reshape(-1)
        rows, cols = b_array_torch.shape
        block_size = self.config.block_size
        blob_size = block_size // 2
        k_blocks = (rows + block_size - 1) // block_size
        packed_torch = packed_torch.reshape(cols, k_blocks, blob_size)

        b_quant = onnx.numpy_helper.from_array(packed_torch.cpu().numpy())
        b_quant.name = b_pb.name + "_Q4"
        for input in bs_graph.input:
            if input.name == input_b:
                bs_graph.input.remove(input)
                break

        scales_tensor = onnx.numpy_helper.from_array(scales)
        scales_tensor.name = b_pb.name + "_scales"
        bs_graph.initializer.extend([b_quant, scales_tensor])

        input_names = [node.input[0], b_quant.name, scales_tensor.name]
        zp_tensor = onnx.numpy_helper.from_array(zero_points)
        zp_tensor.name = b_pb.name + "_zero_points"
        bs_graph.initializer.extend([zp_tensor])
        input_names.append(zp_tensor.name)

        kwargs = {}
        rows, cols = b_array.shape
        kwargs["K"] = rows
        kwargs["N"] = cols
        kwargs["bits"] = self.config.bits
        kwargs["block_size"] = self.config.block_size

        matmul_q4_node = onnx.helper.make_node(
            "MatMulNBits",
            inputs=input_names,
            outputs=[node.output[0]],
            name=node.name + "_Q4" if node.name else "",
            domain="com.microsoft",
            **kwargs,
        )

        logger.info(f"complete quantization of {node.name} ...")

        return [matmul_q4_node]


def get_initializer(name, graph_path: list[GraphProto]) -> tuple[TensorProto, GraphProto]:
    for gid in range(len(graph_path) - 1, -1, -1):
        graph = graph_path[gid]
        for tensor in graph.initializer:
            if tensor.name == name:
                return tensor, graph
    return None, None


class DefaultWeightOnlyQuantizer:
    def __init__(self, config: DefaultWeightOnlyQuantConfig):
        self.config = config

    def int4_block_quant(self, fp32weight: npt.ArrayLike) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """4b quantize fp32 weight to a blob"""

        if len(fp32weight.shape) != 2:
            raise ValueError("Current int4 block quantization only supports 2D tensors!")
        rows, cols = fp32weight.shape

        block_size = self.config.block_size
        k_blocks = (rows + block_size - 1) // block_size

        if self.config.quant_format == QuantFormat.QOperator:
            blob_size = block_size // 2
            padded_rows = k_blocks * block_size
            pad_len = padded_rows - rows
            if pad_len > 0:
                fp32weight = np.pad(fp32weight, ((0, pad_len), (0, 0)), "constant")

            # block wise quantization, each block comes from a single column
            packed = np.zeros((cols, k_blocks, blob_size), dtype="uint8")
            zero_point = np.zeros(cols * ((k_blocks + 1) // 2), dtype="uint8")
            scales = np.zeros((cols * k_blocks), dtype=fp32weight.dtype)
            quantize_matmul_4bits(
                packed, fp32weight, scales, zero_point, block_size, cols, rows, self.config.is_symmetric
            )
        else:
            packed = np.zeros((rows * cols + 1) // 2, dtype="uint8")
            zero_point = np.zeros((cols * k_blocks + 1) // 2, dtype="uint8")
            scales = np.zeros((k_blocks, cols), dtype=fp32weight.dtype)
            quantize_qdq_matmul_4bits(
                packed, fp32weight, scales, zero_point, block_size, cols, rows, self.config.is_symmetric
            )

        return (packed, scales, zero_point)

    def quantize(self, node: NodeProto, graph_stack: list[GraphProto]) -> list[NodeProto]:
        """
        If the node is MatMul with fp32 const weight, quantize the weight with int4, and return the new node.
        If QOperator format, return MatMulNbits. If QDQ format, return DeQuantizeLinear + MatMul.
        """

        if node.op_type != "MatMul":
            return [node]  # only care about MatMul for now

        logger.info(f"start to quantize {node.name} ...")
        qtype = TensorProto.INT4 if self.config.is_symmetric else TensorProto.UINT4
        input_b = node.input[1]
        b_tensor, b_graph = get_initializer(input_b, graph_stack)
        if b_tensor is None:
            logger.info("MatMul doesn't have const weight. Skip to quantize")
            return [node]  # only care about constant weight

        b_ndarray = onnx.numpy_helper.to_array(b_tensor)
        if len(b_ndarray.shape) != 2:
            logger.info("MatMul weight is not 2D. Skip to quantize")
            return [node]  # can only process 2-D matrix

        packed, scales, zero_points = self.int4_block_quant(b_ndarray)

        if self.config.quant_format == QuantFormat.QOperator:
            b_quant = onnx.numpy_helper.from_array(packed, b_tensor.name + "_Q4")
            scales_tensor = onnx.numpy_helper.from_array(scales, b_tensor.name + "_scales")
        else:
            b_quant = onnx.helper.make_tensor(b_tensor.name + "_DQ_Q4", qtype, b_ndarray.shape, packed.tobytes(), True)
            scales_tensor = onnx.numpy_helper.from_array(scales, b_tensor.name + "_DQ_scales")

        for input in b_graph.input:
            if input.name == input_b:
                b_graph.input.remove(input)
                break

        b_graph.initializer.extend([b_quant, scales_tensor])

        output_nodes = []

        if self.config.quant_format == QuantFormat.QOperator:
            input_names = [node.input[0], b_quant.name, scales_tensor.name]
            if not self.config.is_symmetric:
                zp_tensor = onnx.numpy_helper.from_array(zero_points, b_tensor.name + "_zero_points")
                input_names.append(zp_tensor.name)
                b_graph.initializer.extend([zp_tensor])
            kwargs = {}
            rows, cols = b_ndarray.shape
            kwargs["K"] = rows
            kwargs["N"] = cols
            kwargs["bits"] = 4
            kwargs["block_size"] = self.config.block_size
            if self.config.accuracy_level is not None:
                kwargs["accuracy_level"] = self.config.accuracy_level

            matmul_q4_node = onnx.helper.make_node(
                "MatMulNBits",
                inputs=input_names,
                outputs=[node.output[0]],
                name=node.name + "_Q4" if node.name else "",
                domain="com.microsoft",
                **kwargs,
            )

            output_nodes.append(matmul_q4_node)
        else:
            dq_input_names = [b_quant.name, scales_tensor.name]
            dq_output_names = [b_quant.name + "_output"]
            matmul_input_names = [node.input[0], dq_output_names[0]]
            matmul_output_names = [node.output[0]]
            if not self.config.is_symmetric:
                zp_tensor = onnx.helper.make_tensor(
                    b_tensor.name + "_DQ_zero_points", qtype, scales.shape, zero_points.tobytes(), True
                )
                dq_input_names.append(zp_tensor.name)
                b_graph.initializer.extend([zp_tensor])
            dq_kwargs = {"axis": 0, "block_size": self.config.block_size}
            dq_node = onnx.helper.make_node(
                "DequantizeLinear",
                inputs=dq_input_names,
                outputs=dq_output_names,
                name=node.name + "_DQ_Q4" if node.name else "",
                **dq_kwargs,
            )
            matmul_node = onnx.helper.make_node(
                "MatMul",
                inputs=matmul_input_names,
                outputs=matmul_output_names,
                name=node.name + "_matmul_Q4" if node.name else "",
            )
            output_nodes.extend([dq_node, matmul_node])

        logger.info(f"complete quantization of {node.name} ...")
        return output_nodes


class MatMul4BitsQuantizer:
    """
    Perform 4b quantization of constant MatMul weights.
    If algo_config.quant_format is QOperator, the quantized weight is stored in a MatMulNBits node, which relaces the
    MatMul node.
    If algo_config.quant_format is QDQ, the quantized weight is stored in a DeQuantizeLinear node. The MatMul node is
    replaced by the DequantizeLinear + MatMul nodes.
    """

    def __init__(
        self,
        model: ModelProto | str,
        block_size: int = 128,
        is_symmetric: bool = False,
        accuracy_level: int | None = None,
        nodes_to_exclude=None,
        quant_format=QuantFormat.QOperator,
        algo_config: WeightOnlyQuantConfig | None = None,
    ):
        if nodes_to_exclude is None:
            nodes_to_exclude = []
        self.model = ONNXModel(onnx.load(model)) if isinstance(model, str) else ONNXModel(model)
        self.model_path = model if isinstance(model, str) else None
        self.block_size = block_size
        self.is_symmetric = is_symmetric
        self.accuracy_level = accuracy_level
        self.nodes_to_exclude = set(nodes_to_exclude)
        self.node_quantizer = None
        if algo_config is None:
            algo_config = DefaultWeightOnlyQuantConfig(
                block_size=block_size,
                is_symmetric=is_symmetric,
                accuracy_level=accuracy_level,
                quant_format=quant_format,
            )
        self.algo_config = algo_config
        if algo_config.algorithm == "HQQ":
            self.node_quantizer = HQQWeightOnlyQuantizer(self.algo_config)
        elif algo_config.algorithm == "DEFAULT":
            self.node_quantizer = DefaultWeightOnlyQuantizer(self.algo_config)

    def _process_subgraph(self, graph_stack: list[GraphProto]):
        new_nodes = []
        graph = graph_stack[-1]

        for node in graph.node:
            graph_attrs = [
                attr
                for attr in node.attribute
                if attr.type == onnx.AttributeProto.GRAPH or attr.type == onnx.AttributeProto.GRAPHS
            ]
            if len(graph_attrs):
                kwargs = {}
                for attr in node.attribute:
                    if attr.type == onnx.AttributeProto.GRAPH:
                        # recursive call to take care of sub-graph
                        graph_stack.append(attr.g)
                        kv = {attr.name: self._process_subgraph(graph_stack)}
                    elif attr.type == onnx.AttributeProto.GRAPHS:
                        value = []
                        for subgraph in attr.graphs:
                            # recursive call to take care of sub-graph
                            graph_stack.append(subgraph)
                            value.extend([self._process_subgraph(graph_stack)])
                        kv = {attr.name: value}
                    else:
                        kv = attribute_to_kwarg(attr)
                    kwargs.update(kv)
                node = onnx.helper.make_node(  # noqa: PLW2901
                    node.op_type, node.input, node.output, name=node.name, **kwargs
                )
            out_nodes = []
            if node.name in self.nodes_to_exclude:
                logger.info(f"exclude to quantize {node.name} as specified by nodes_to_exclude...")
                out_nodes = [node]
            elif self.algo_config is not None and self.algo_config.algorithm == "HQQ":
                out_nodes = self.node_quantizer.quantize(node, graph_stack)
            else:
                out_nodes = self.node_quantizer.quantize(node, graph_stack)
            new_nodes.extend(out_nodes)

        graph.ClearField("node")
        graph.node.extend(new_nodes)
        graph_stack.pop()
        return graph

    def _generate_q4_node_config(self):
        """Generate weight only quant configuration for nodes."""
        q4_node_config = {}
        template_config_q4 = {
            "bits": 4,
            "group_size": self.block_size,
            "scheme": "sym" if self.is_symmetric else "asym",
        }
        for node in self.model.model.graph.node:
            if node.op_type in ["MatMul"]:
                if not all([self.model.get_initializer(i) is None for i in node.input]):
                    q4_node_config[node.name] = template_config_q4
        return q4_node_config

    def int4_quant_algo(self):
        """4b quantize a model with RTN or GPTQ algorithm. Please refer to
        https://github.com/intel/neural-compressor/blob/master/docs/source/quantization_weight_only.md
        for more details on weight only quantization using Intel® Neural Compressor.
        """

        def inc_dataloader():
            data_reader = copy.deepcopy(self.algo_config.calibration_data_reader)
            for data in data_reader:
                yield data, None

        kwargs = {}
        if self.accuracy_level is not None:
            kwargs["accuracy_level"] = self.accuracy_level
        weight_only_node_config = self._generate_q4_node_config()

        algorithm = self.algo_config.algorithm
        logger.info(f"start to quantize model with {algorithm} algorithm...")
        if algorithm == "RTN":
            from neural_compressor.adaptor.ox_utils.weight_only import rtn_quantize

            kwargs["ratios"] = self.algo_config.ratios

            self.model = rtn_quantize(
                model=self.model_path if self.model_path is not None else self.model.model,
                weight_config=weight_only_node_config,
                **kwargs,
            )
        elif algorithm == "GPTQ":
            from neural_compressor.adaptor.ox_utils.weight_only import gptq_quantize

            kwargs["percdamp"] = self.algo_config.percdamp
            kwargs["blocksize"] = self.algo_config.block_size
            kwargs["actorder"] = self.algo_config.actorder
            kwargs["mse"] = self.algo_config.mse
            kwargs["perchannel"] = self.algo_config.perchannel
            kwargs["n_samples"] = -1
            dataloader = inc_dataloader()

            self.model = gptq_quantize(
                model=self.model_path if self.model_path is not None else self.model.model,
                weight_config=weight_only_node_config,
                dataloader=dataloader,
                **kwargs,
            )
        logger.info(f"complete quantization of model with {algorithm} algorithm.")

    def process(self):
        if self.algo_config.algorithm in ["HQQ", "DEFAULT"]:
            # use a stack to keep track of sub-graphs
            graph_stack = [self.model.graph()]

            # Update domain opset
            if self.algo_config.quant_format == QuantFormat.QOperator:
                self.model.set_opset_import("com.microsoft", 1)
            else:
                opset_import = self.model.opset_import()
                for opset in opset_import:
                    if opset.domain in [None, "ai.onnx", ""] and opset.version < 21:
                        logger.warning(
                            "The opset of the input model is under 21 and doesn't support int4 data type. "
                            "Force to update it to opset 21, but the generated model may not be a valid model."
                        )
                        self.model.set_opset_import(opset.domain, 21)

            self._process_subgraph(graph_stack)
            self.model.clean_initializers()
        else:
            # use Intel® Neural Compressor for RTN or GPTQ weight-only quantize algorithm
            try:
                importlib.import_module("neural_compressor")
            except Exception as e:
                logging.error(f"{e}.")
                raise RuntimeError(
                    "neural-compressor is not correctly installed. Please check your environment."
                ) from e

            import neural_compressor

            assert version.parse(neural_compressor.__version__) >= version.parse(
                "2.3.2"
            ), "Require neural-compressor >= 2.3.2 to support weight only quantization!"

            self.int4_quant_algo()


def ort_convert_str_to_bool(value):
    return value.lower() in ("true", "1")


def parse_args():
    parser = argparse.ArgumentParser(
        description="""Blockwise int4 quantization for MatMul 2D weight matrices.

A weight matrix is partitioned into into blocks, where each block is a
continguous subset inside each column. Each block is quantized into a
set of 4b integers with a scaling factor and an optional offset.
"""
    )

    parser.add_argument("--input_model", required=True, help="Path to the input model file")
    parser.add_argument("--output_model", required=True, help="Path to the output model file")
    parser.add_argument("--block_size", required=False, default=32, type=int, help="Block size for quantization")
    parser.add_argument(
        "--quant_method",
        default="default",
        type=str,
        choices=["default", "hqq", "rtn", "gptq"],
        help="the algorithm used to quantize weight, \nrtn and gptq leverage Intel® Neural Compressor",
    )
    parser.add_argument("--bits", default=4, type=int, help="the target bits to represent weight")
    parser.add_argument(
        "--symmetric",
        required=False,
        default=True,
        const=True,
        nargs="?",
        type=ort_convert_str_to_bool,
        choices=[True, False],
        help="Indicate whether to quantize the model symmetrically, symmetric is not supported by hqq",
    )
    parser.add_argument(
        "--accuracy_level",
        required=False,
        type=int,
        help="Accuracy level of the 4-bit quantized MatMul computation. "
        "Refer to the MatMulNBits contrib op's 'accuracy_level' attribute for details "
        "(https://github.com/microsoft/onnxruntime/blob/main/docs/ContribOperators.md#commicrosoftmatmulnbits).",
    )
    parser.add_argument("-v", "--verbose", required=False, action="store_true")
    parser.set_defaults(verbose=False)
    parser.add_argument(
        "--nodes_to_exclude",
        nargs="+",
        type=str,
        required=False,
        default=[],
        help="Specify the nodes to be excluded from quantization with node names",
    )
    parser.add_argument(
        "--quant_format",
        default="QOperator",
        type=str,
        choices=["QOperator", "QDQ"],
        help="QuantFormat {QOperator, QDQ}"
        "QOperator format quantizes the model with quantized operators directly."
        "QDQ format quantize the model by inserting DeQuantizeLinear before the MatMul.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    input_model_path = args.input_model
    output_model_path = args.output_model
    quant_format = QuantFormat[args.quant_format]

    if os.path.exists(output_model_path):
        logger.error(f"file {output_model_path} already exists")
        raise Exception(f"file {output_model_path} already exists")

    if args.symmetric and args.quant_method == "hqq":
        logger.warning("symmetric is not supportted by hqq, will force to symmetric=False")
        args.symmetric = False

    model = onnx.load(input_model_path)
    if args.quant_method == "hqq":
        quant_config = HQQWeightOnlyQuantConfig(block_size=args.block_size, bits=args.bits)
    elif args.quant_method == "default":
        quant_config = DefaultWeightOnlyQuantConfig(
            block_size=args.block_size,
            is_symmetric=args.symmetric,
            accuracy_level=args.accuracy_level,
            quant_format=quant_format,
        )
    elif args.quant_method == "rtn":
        quant_config = RTNWeightOnlyQuantConfig()
    elif args.quant_method == "gptq":
        quant_config = GPTQWeightOnlyQuantConfig(block_size=args.block_size)
    else:
        raise ValueError(f"Unsupported quantization method: {args.quant_method}")

    quant = MatMul4BitsQuantizer(
        model=model,
        accuracy_level=args.accuracy_level,
        nodes_to_exclude=args.nodes_to_exclude,
        algo_config=quant_config,
    )
    quant.process()
    quant.model.save_model_to_file(output_model_path, True)
