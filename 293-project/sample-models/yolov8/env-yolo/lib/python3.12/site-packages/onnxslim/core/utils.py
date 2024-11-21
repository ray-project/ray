from onnxslim.core.pattern import get_node_users, get_node_feeds
from onnxslim.third_party.onnx_graphsurgeon.ir.tensor import Variable


def delete_node(node, input_var_idx=0, output_var_idx=0):
    """Delete a node from the computation graph while re-linking its input and output to maintain graph integrity."""
    input_variable = node.inputs[input_var_idx]
    node_variable = node.outputs[output_var_idx]
    next_nodes = get_node_users(node)

    output_var = None
    for next_node in next_nodes:
        if isinstance(next_node, Variable) and next_node.is_output:
            output_var = next_node
            break

    if output_var:
        feeds = get_node_feeds(node)
        feed = feeds[0]
        if not isinstance(feed, Variable):
            feed.outputs.remove(node.inputs[input_var_idx])
            feed.outputs.append(node.outputs[output_var_idx])
            node.outputs.clear()
    else:
        for next_node in next_nodes:
            index = next_node.inputs.index(node_variable)
            next_node.inputs.pop(index)
            next_node.inputs.insert(index, input_variable)
