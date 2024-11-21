import logging

logger = logging.getLogger("onnxslim")
import onnxslim.third_party.onnx_graphsurgeon as gs


def tie_weights(graph):
    """Tie weights in a computational graph to reduce the number of parameters."""
    tensor_map = graph.tensors()
    constant_tensors = [tensor for tensor in tensor_map.values() if isinstance(tensor, gs.Constant)]

    sub_graphs = graph.subgraphs(recursive=True)
    sub_graphs_constant_tensors = [
        [tensor for name, tensor in sub_graph.tensors().items() if isinstance(tensor, gs.Constant)]
        for sub_graph in sub_graphs
    ]

    constant_tensors.extend([tensor for tensors in sub_graphs_constant_tensors for tensor in tensors])

    def replace_constant_references(existing_constant, to_be_removed_constant):
        users = list(to_be_removed_constant.outputs)

        for user in users:
            for idx, inp in enumerate(user.inputs):
                if (inp == to_be_removed_constant) and (inp.name == to_be_removed_constant.name):
                    user.inputs.pop(idx)
                    user.inputs.insert(idx, existing_constant)

    if len(constant_tensors) > 1:
        keep_constants = [True] * len(constant_tensors)
        for i, constant_tensor in enumerate(constant_tensors):
            if keep_constants[i]:
                for j in range(i + 1, len(constant_tensors)):
                    if keep_constants[j]:
                        if constant_tensor == constant_tensors[j]:
                            keep_constants[j] = False
                            replace_constant_references(constant_tensor, constant_tensors[j])
                            logger.debug(
                                f"Constant {constant_tensors[j].name} can be replaced by {constant_tensor.name}"
                            )
