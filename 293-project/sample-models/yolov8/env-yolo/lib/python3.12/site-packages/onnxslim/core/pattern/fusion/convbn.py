import numpy as np

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class ConvBatchNormMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes the ConvBatchNormMatcher for fusing Conv and BatchNormalization layers in an ONNX graph."""
        pattern = Pattern(
            """
            input              input  0  1 conv_0
            Conv               conv_0 1+ 1 input bn_0
            BatchNormalization bn_0   5  1 conv_0 ? ? ? ? output
            output             output 1  0 bn_0
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the FusionConvBN pattern."""
        return "FusionConvBN"

    def rewrite(self, opset=11):
        """Rewrites the weights and biases of a BatchNormalization layer fused with a convolution layer."""
        match_case = {}
        conv_transpose_node = self.conv_0
        conv_transpose_node_users = get_node_users(conv_transpose_node)
        node = self.bn_0
        if len(conv_transpose_node_users) == 1:
            conv_transpose_weight = conv_transpose_node.inputs[1].values
            bn_node = node
            bn_scale = bn_node.inputs[1].values
            bn_bias = bn_node.inputs[2].values
            bn_running_mean = bn_node.inputs[3].values
            bn_running_var = bn_node.inputs[4].values
            bn_eps = bn_node.attrs["epsilon"]

            if len(conv_transpose_node.inputs) == 2:
                conv_transpose_bias = np.zeros_like(bn_running_mean)
            else:
                conv_transpose_bias = conv_transpose_node.inputs[2].values

            bn_var_rsqrt = 1.0 / np.sqrt(bn_running_var + bn_eps)
            shape = [1] * len(conv_transpose_weight.shape)
            if bn_node.i(0).op == "Conv":
                shape[0] = -1
            else:
                shape[1] = -1
            conv_w = conv_transpose_weight * (bn_scale * bn_var_rsqrt).reshape(shape)
            conv_b = (conv_transpose_bias - bn_running_mean) * bn_var_rsqrt * bn_scale + bn_bias

            inputs = []
            inputs.append(list(conv_transpose_node.inputs)[0])
            weight_name = list(conv_transpose_node.inputs)[1].name
            if weight_name.endswith("weight"):
                bias_name = f"{weight_name[:-6]}bias"
            else:
                bias_name = f"{weight_name}_bias"
            inputs.extend(
                (
                    gs.Constant(weight_name, values=conv_w),
                    gs.Constant(bias_name, values=conv_b),
                )
            )
            outputs = list(bn_node.outputs)

            conv_transpose_node.outputs.clear()
            bn_node.inputs.clear()
            bn_node.outputs.clear()

            match_case[conv_transpose_node.name] = {
                "op": conv_transpose_node.op,
                "inputs": inputs,
                "outputs": outputs,
                "name": conv_transpose_node.name,
                "attrs": conv_transpose_node.attrs,
                "domain": None,
            }

        return match_case


register_fusion_pattern(ConvBatchNormMatcher(1))
