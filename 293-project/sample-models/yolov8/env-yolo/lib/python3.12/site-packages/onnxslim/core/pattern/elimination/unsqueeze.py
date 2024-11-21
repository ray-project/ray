import numpy as np

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class UnsqueezePatternMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes the UnsqueezePatternMatcher with a specified priority using a predefined graph pattern."""
        pattern = Pattern(
            """
            input      input       0  1 unsqueeze_0
            Unsqueeze  unsqueeze_0 1+ 1 input unsqueeze_1
            Unsqueeze  unsqueeze_1 1+ 1 unsqueeze_0 output
            output     output      1  0 unsqueeze_1
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the elimination pattern, 'EliminationUnsqueeze'."""
        return "EliminationUnsqueeze"

    def rewrite(self, opset=11):
        """Rewrites an elimination pattern for unsqueeze nodes by optimizing nested slice operations."""
        match_case = {}
        node_unsqueeze_0 = self.unsqueeze_0
        users_node_unsqueeze_0 = get_node_users(node_unsqueeze_0)
        node_unsqueeze_1 = self.unsqueeze_1
        if len(users_node_unsqueeze_0) == 1 and node_unsqueeze_0.inputs[0].shape and node_unsqueeze_1.inputs[0].shape:
            if opset < 13 or (
                isinstance(node_unsqueeze_0.inputs[1], gs.Constant)
                and isinstance(node_unsqueeze_1.inputs[1], gs.Constant)
            ):

                def get_unsqueeze_axes(unsqueeze_node, opset):
                    dim = len(unsqueeze_node.inputs[0].shape)
                    if opset < 13:
                        axes = unsqueeze_node.attrs["axes"]
                    else:
                        axes = unsqueeze_node.inputs[1].values
                    return [axis + dim + len(axes) if axis < 0 else axis for axis in axes]

                axes_node_unsqueeze_0 = get_unsqueeze_axes(node_unsqueeze_0, opset)
                axes_node_unsqueeze_1 = get_unsqueeze_axes(node_unsqueeze_1, opset)

                axes_node_unsqueeze_0 = [
                    axis + sum(1 for axis_ in axes_node_unsqueeze_1 if axis_ <= axis) for axis in axes_node_unsqueeze_0
                ]

                inputs = [node_unsqueeze_0.inputs[0]]
                outputs = list(node_unsqueeze_1.outputs)
                node_unsqueeze_0.inputs.clear()
                node_unsqueeze_0.outputs.clear()
                node_unsqueeze_1.inputs.clear()
                node_unsqueeze_1.outputs.clear()

                if opset < 13:
                    attrs = {"axes": axes_node_unsqueeze_0 + axes_node_unsqueeze_1}
                else:
                    attrs = None
                    inputs.append(
                        gs.Constant(
                            name=f"{node_unsqueeze_0.name}_axes",
                            values=np.array(axes_node_unsqueeze_0 + axes_node_unsqueeze_1, dtype=np.int64),
                        )
                    )

                match_case[node_unsqueeze_0.name] = {
                    "op": "Unsqueeze",
                    "inputs": inputs,
                    "outputs": outputs,
                    "name": node_unsqueeze_0.name,
                    "attrs": attrs,
                    "domain": None,
                }

        return match_case


register_fusion_pattern(UnsqueezePatternMatcher(1))
