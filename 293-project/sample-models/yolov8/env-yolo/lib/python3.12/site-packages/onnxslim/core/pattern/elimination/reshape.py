import numpy as np

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class ReshapePatternMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes the ReshapePatternMatcher with a priority and a specific pattern for detecting nested reshape
        operations.
        """
        pattern = Pattern(
            """
            input    input     0 1 reshape_0
            Reshape  reshape_0 2 1 input   ? reshape_1
            Reshape  reshape_1 2 1 reshape_0 ? output
            output   output    1 0 reshape_1
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name 'EliminationReshape'."""
        return "EliminationReshape"

    def rewrite(self, opset=11):
        """Rewrite the computational graph by eliminating redundant reshape operations when certain conditions are
        met.
        """
        match_case = {}
        node = self.reshape_1
        first_reshape_node = node.i(0)
        first_reshape_node_inputs = list(first_reshape_node.inputs)
        first_reshape_node_users = get_node_users(first_reshape_node)
        if len(first_reshape_node_users) == 1:
            second_reshape_node = node

            def check_constant_mergeable(reshape_node):
                """Check if a reshape node's shape input, containing zero dimensions, can be merged with its input
                node's shape.
                """
                if isinstance(reshape_node.inputs[1], gs.Constant):
                    input_shape = reshape_node.inputs[0].shape
                    reshape_shape = reshape_node.inputs[1].values
                    if input_shape is not None and np.any(reshape_shape == 0):
                        shape = [
                            input_shape[i] if dim_size == 0 else dim_size for i, dim_size in enumerate(reshape_shape)
                        ]
                        if not all(isinstance(item, int) for item in shape):
                            return False
                return True

            if check_constant_mergeable(first_reshape_node) and check_constant_mergeable(second_reshape_node):
                inputs = []
                inputs.append(first_reshape_node_inputs[0])
                inputs.append(second_reshape_node.inputs[1])
                outputs = list(second_reshape_node.outputs)
                first_reshape_node.outputs.clear()
                second_reshape_node.inputs.clear()
                second_reshape_node.outputs.clear()

                match_case[first_reshape_node.name] = {
                    "op": "Reshape",
                    "inputs": inputs,
                    "outputs": outputs,
                    "name": first_reshape_node.name,
                    "attrs": first_reshape_node.attrs,
                    "domain": None,
                }

        return match_case


register_fusion_pattern(ReshapePatternMatcher(1))
