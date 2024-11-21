import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.optimization.dead_node_elimination import get_constant_variable
from onnxslim.core.pattern import Pattern, PatternMatcher, get_node_users
from onnxslim.core.pattern.registry import register_fusion_pattern


class MatMulAddPatternMatcher(PatternMatcher):
    def __init__(self, priority):
        """Initializes a matcher for fusing MatMul and Add operations in ONNX graph optimization."""
        pattern = Pattern(
            """
            input    input    0 1 matmul_0
            MatMul   matmul_0 2 1 input ? add_0
            Add      add_0    2 1 matmul_0 ? output
            output   output   1 0 add_0
            """
        )
        super().__init__(pattern, priority)

    @property
    def name(self):
        """Returns the name of the fusion pattern as a string 'FusionGemm'."""
        return "FusionGemm"

    def rewrite(self, opset=11):
        """Rewrites the graph for the fusion pattern 'FusionGemm' based on matching criteria and constant variables in
        matmul nodes.
        """
        match_case = {}
        node = self.add_0
        matmul_node = self.matmul_0
        matmul_bias_variable = get_constant_variable(matmul_node)
        input_variable = (
            matmul_node.inputs[0] if isinstance(matmul_node.inputs[1], gs.Constant) else matmul_node.inputs[1]
        )
        users = get_node_users(matmul_node)
        if len(users) == 1 and matmul_bias_variable:
            if (
                input_variable.shape
                and len(input_variable.shape) > 2
                and all([isinstance(value, int) for value in input_variable.shape])
            ):
                pre_reshape_const = gs.Constant(
                    f"{matmul_node.name}_pre_reshape_in",
                    values=np.array([-1, matmul_bias_variable.values.shape[0]], dtype=np.int64),
                )
                inputs = []
                inputs.append(input_variable)
                inputs.append(pre_reshape_const)

                reshape_out_variable = gs.Variable(
                    f"{matmul_node.name}_pre_reshape_out",
                    dtype=input_variable.dtype,
                )
                outputs = [reshape_out_variable]

                match_case.update(
                    {
                        f"{matmul_node.name}_pre_reshape": {
                            "op": "Reshape",
                            "inputs": inputs,
                            "outputs": outputs,
                            "name": f"{matmul_node.name}_pre_reshape",
                            "domain": None,
                        }
                    }
                )

                add_node = node
                add_bias_variable = get_constant_variable(add_node)

                output_variable = add_node.inputs[0]
                output_variable.outputs.remove(add_node)

                matmul_bias_transpose_constant = gs.Constant(
                    matmul_bias_variable.name, values=matmul_bias_variable.values.T
                )

                inputs = []
                inputs.append(reshape_out_variable)
                inputs.append(matmul_bias_transpose_constant)
                inputs.append(add_bias_variable)

                gemm_out_variable = gs.Variable(f"{matmul_node.name}_gemm_out", dtype=output_variable.dtype)
                outputs = [gemm_out_variable]

                match_case.update(
                    {
                        matmul_node.name: {
                            "op": "Gemm",
                            "inputs": inputs,
                            "outputs": outputs,
                            "name": matmul_node.name,
                            "attrs": {
                                "alpha": 1.0,
                                "beta": 1.0,
                                "transA": 0,
                                "transB": 1,
                            },
                            "domain": None,
                        }
                    }
                )

                values = input_variable.shape[:-1] + [matmul_bias_variable.values.shape[-1]]
                post_reshape_const = gs.Constant(
                    f"{matmul_node.name}_post_reshape_in",
                    values=np.array(values, dtype=np.int64),
                )

                inputs = []
                inputs.append(gemm_out_variable)
                inputs.append(post_reshape_const)
                outputs = list(add_node.outputs)

                matmul_node.outputs.clear()
                add_node.inputs.clear()
                add_node.outputs.clear()

                match_case.update(
                    {
                        f"{matmul_node.name}_post_reshape": {
                            "op": "Reshape",
                            "inputs": inputs,
                            "outputs": outputs,
                            "name": f"{matmul_node.name}_post_reshape",
                            "domain": None,
                        }
                    }
                )
            elif (
                input_variable.shape
                and len(input_variable.shape) == 2
                and all([isinstance(value, int) for value in input_variable.shape])
            ):
                add_node = node
                add_bias_variable = get_constant_variable(add_node)

                output_variable = add_node.inputs[0]
                output_variable.outputs.remove(add_node)

                matmul_bias_transpose_constant = gs.Constant(
                    matmul_bias_variable.name, values=matmul_bias_variable.values.T
                )

                inputs = []
                inputs.append(input_variable)
                inputs.append(matmul_bias_transpose_constant)
                inputs.append(add_bias_variable)

                outputs = list(add_node.outputs)
                add_node.inputs.clear()
                add_node.outputs.clear()
                match_case.update(
                    {
                        matmul_node.name: {
                            "op": "Gemm",
                            "inputs": inputs,
                            "outputs": outputs,
                            "name": matmul_node.name,
                            "attrs": {
                                "alpha": 1.0,
                                "beta": 1.0,
                                "transA": 0,
                                "transB": 1,
                            },
                            "domain": None,
                        }
                    }
                )
        return match_case


register_fusion_pattern(MatMulAddPatternMatcher(1))
