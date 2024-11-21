import numpy as np

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class SlicePatternMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes the SlicePatternMatcher with a specified priority using a predefined graph pattern."""
        pattern = Pattern(
            """
            input  input   0 1 slice_0
            Slice  slice_0 5 1 input   ? ? ? ? slice_1
            Slice  slice_1 5 1 slice_0 ? ? ? ? output
            output output  1 0 slice_1
            """
        )  # to check here slice_0
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the elimination pattern, 'EliminationSlice'."""
        return "EliminationSlice"

    def rewrite(self, opset=11):
        """Rewrites an elimination pattern for slice nodes by optimizing nested slice operations."""
        match_case = {}
        first_slice_node = self.slice_0
        first_slice_node_inputs = list(first_slice_node.inputs)
        if all(isinstance(input, gs.Constant) for input in first_slice_node_inputs[1:]):
            first_slice_node_users = get_node_users(first_slice_node)
            if all(
                user.op == "Slice" and all(isinstance(input, gs.Constant) for input in list(user.inputs)[1:])
                for user in first_slice_node_users
            ):
                first_slice_node_starts = first_slice_node_inputs[1].values.tolist()
                first_slice_node_ends = first_slice_node_inputs[2].values.tolist()
                first_slice_node_axes = first_slice_node_inputs[3].values.tolist()
                first_slice_node_steps = first_slice_node_inputs[4].values.tolist()

                for user_node in first_slice_node_users:
                    second_slice_node = user_node
                    second_slice_node_inputs = list(second_slice_node.inputs)
                    second_slice_node_starts = second_slice_node_inputs[1].values.tolist()
                    second_slice_node_ends = second_slice_node_inputs[2].values.tolist()
                    second_slice_node_axes = second_slice_node_inputs[3].values.tolist()
                    second_slice_node_steps = second_slice_node_inputs[4].values.tolist()

                    new_starts = first_slice_node_starts + second_slice_node_starts
                    new_ends = first_slice_node_ends + second_slice_node_ends
                    new_axes = first_slice_node_axes + second_slice_node_axes
                    new_steps = first_slice_node_steps + second_slice_node_steps

                    if len(new_axes) != len(set(new_axes)):
                        continue

                    inputs = []
                    inputs.extend(
                        (
                            list(first_slice_node.inputs)[0],
                            gs.Constant(
                                second_slice_node_inputs[1].name,
                                values=np.array(new_starts, dtype=np.int64),
                            ),
                            gs.Constant(
                                second_slice_node_inputs[2].name,
                                values=np.array(new_ends, dtype=np.int64),
                            ),
                            gs.Constant(
                                second_slice_node_inputs[3].name,
                                values=np.array(new_axes, dtype=np.int64),
                            ),
                            gs.Constant(
                                second_slice_node_inputs[4].name,
                                values=np.array(new_steps, dtype=np.int64),
                            ),
                        )
                    )
                    outputs = list(second_slice_node.outputs)

                    first_slice_node.outputs.clear()
                    second_slice_node.inputs.clear()
                    second_slice_node.outputs.clear()

                    if len(first_slice_node_users) == 1:
                        match_case[first_slice_node.name] = {
                            "op": "Slice",
                            "inputs": inputs,
                            "outputs": outputs,
                            "name": first_slice_node.name,
                            "attrs": first_slice_node.attrs,
                            "domain": None,
                        }
                    else:
                        match_case[second_slice_node.name] = {
                            "op": "Slice",
                            "inputs": inputs,
                            "outputs": outputs,
                            "name": second_slice_node.name,
                            "attrs": second_slice_node.attrs,
                            "domain": None,
                        }

        return match_case


register_fusion_pattern(SlicePatternMatcher(1))
