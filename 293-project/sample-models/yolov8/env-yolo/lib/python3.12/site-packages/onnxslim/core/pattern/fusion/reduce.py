from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class ReducePatternMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes the ReducePatternMatcher with a specified pattern matching priority level."""
        pattern = Pattern(
            """
            input     input       0 1 reduce_0
            ReduceSum reduce_0    1 1 input unsqueeze_0
            Unsqueeze unsqueeze_0 1 1 reduce_0 output
            output    output      1 0 unsqueeze_0
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the fusion pattern 'FusionReduce'."""
        return "FusionReduce"

    def rewrite(self, opset=11):
        """Rewrites the graph pattern based on opset version; reuses Reduce and Unsqueeze nodes if possible."""
        match_case = {}
        node = self.unsqueeze_0
        reduce_node = self.reduce_0
        reduce_node_node_users = get_node_users(reduce_node)
        if len(reduce_node_node_users) == 1:
            unsqueeze_node = node

            reduce_node_axes = reduce_node.attrs.get("axes", None)
            reduce_node_keepdims = reduce_node.attrs.get("keepdims", 1)
            unsqueeze_node_axes = unsqueeze_node.attrs.get("axes", None)

            if opset < 13 and reduce_node_axes == [-1] and unsqueeze_node_axes == [-1] and reduce_node_keepdims == 0:
                inputs = list(reduce_node.inputs)
                outputs = list(unsqueeze_node.outputs)
                attrs = reduce_node.attrs
                reduce_node.outputs.clear()
                unsqueeze_node.inputs.clear()
                unsqueeze_node.outputs.clear()
                attrs["keepdims"] = 1
                match_case[reduce_node.name] = {
                    "op": reduce_node.op,
                    "inputs": inputs,
                    "outputs": outputs,
                    "name": reduce_node.name,
                    "attrs": attrs,
                    "domain": None,
                }

        return match_case


register_fusion_pattern(ReducePatternMatcher(1))
