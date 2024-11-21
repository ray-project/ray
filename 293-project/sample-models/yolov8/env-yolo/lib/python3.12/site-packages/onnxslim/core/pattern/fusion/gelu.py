from onnxslim.core.pattern import Pattern, PatternMatcher


class GeluPatternMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes a `GeluPatternMatcher` to identify and fuse GELU patterns in a computational graph."""
        pattern = Pattern(
            """
            input  input  0 2 mul_0 div_0
            Div    div_0  2 1 input ? erf_0
            Erf    erf_0  1 1 div_0 add_0
            Add    add_0  2 1 erf_0 ? mul_0
            Mul    mul_0  2 1 input add_0 mul_1
            Mul    mul_1  2 1 mul_0 ? output
            output output 1 0 mul_1
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the fusion pattern, 'FusionGelu'."""
        return "FusionGelu"

    def rewrite(self, opset=11):
        """Rewrite the computation graph pattern to fuse GELU operations."""
        input_variable = self.div_0.inputs[0]
        mul_node = self.mul_0
        div_node = self.div_0

        input_variable.outputs.remove(mul_node)
        input_variable.outputs.remove(div_node)

        output_variable = self.mul_1.outputs[0]
        output_variable.inputs.clear()

        return {
            self.mul_1.name: {
                "op": "Gelu",
                "inputs": [input_variable],
                "outputs": [output_variable],
                "domain": None,
            }
        }


# register_fusion_pattern(GeluPatternMatcher(1))
