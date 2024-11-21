import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class PadConvMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes the PadConvMatcher with a specified priority and defines its matching pattern."""
        pattern = Pattern(
            """
            input  input  0  1 pad_0
            Pad    pad_0  1+ 1 input conv_0
            Conv   conv_0 1+ 1 pad_0 output
            output output 1  0 conv_0
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the fusion pattern used."""
        return "FusionPadConv"

    def parameter_check(self) -> bool:
        """Validates if the padding parameter for a convolutional node is a constant."""
        pad_node = self.pad_0
        return isinstance(pad_node.inputs[1], gs.Constant)

    def rewrite(self, opset=11):
        """Rewrites the padding parameter for a convolutional node to use a constant if the current parameter is not a
        constant.
        """
        node = self.conv_0
        pad_node = self.pad_0
        input_variable = self.pad_0.inputs[0]
        pad_node_users = get_node_users(pad_node)

        pad_value = pad_node.inputs[1].values.tolist()

        pad_variable = pad_node.outputs[0]  # pad output variable
        index = node.inputs.index(pad_variable)
        node.inputs.pop(index)
        node.inputs.insert(index, input_variable)

        inputs = list(node.inputs)
        outputs = list(node.outputs)
        attrs = node.attrs

        node.inputs.clear()
        node.outputs.clear()
        # remove pad node if it has only one user
        if len(pad_node_users) == 1:
            input_variable.outputs.remove(pad_node)
            pad_node.inputs.clear()
            pad_node.outputs.clear()

        conv_pads = attrs["pads"]
        len_conv_pads = len(conv_pads) // 2

        len_pads = len(pad_value) // 2
        pads = pad_value[len_pads - len_conv_pads : len_pads] + pad_value[len_pads + len_conv_pads :]

        pads = [pad + conv_pad for pad, conv_pad in zip(pads, conv_pads)]
        attrs["pads"] = pads

        return {
            node.name: {
                "op": "Conv",
                "inputs": inputs,
                "outputs": outputs,
                "name": node.name,
                "attrs": node.attrs,
                "domain": None,
            }
        }


register_fusion_pattern(PadConvMatcher(1))
