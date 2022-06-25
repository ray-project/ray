from typing import Dict

from ray.dag import (
    DAGNode,
    InputNode,
    InputAttributeNode,
    FunctionNode,
    ClassNode,
    ClassMethodNode,
)


class _DAGNodeNameGenerator(object):
    """
    Generate unique suffix for each given Node in the DAG.
    Apply monotonic increasing id suffix for duplicated names.
    """

    def __init__(self):
        self.name_to_suffix: Dict[str, int] = dict()

    def get_node_name(self, node: DAGNode):
        if isinstance(node, InputNode):
            node_name = "INPUT_NODE"
        elif isinstance(node, InputAttributeNode):
            node_name = "INPUT_ATTRIBUTE_NODE"
        elif isinstance(node, ClassMethodNode):
            node_name = node.get_options().get("name", None) or node._method_name
        elif isinstance(node, (ClassNode, FunctionNode)):
            node_name = node.get_options().get("name", None) or node._body.__name__
        # we use instance class name check here to avoid importing ServeNodes as
        # serve components are not included in Ray Core.
        elif type(node).__name__ in ("DeploymentNode", "DeploymentFunctionNode"):
            node_name = node.get_deployment_name()
        elif type(node).__name__ == "DeploymentMethodNode":
            node_name = node.get_deployment_method_name()
        else:
            raise ValueError(
                "get_node_name() should only be called on DAGNode instances."
            )

        if node_name not in self.name_to_suffix:
            self.name_to_suffix[node_name] = 0
            return node_name
        else:
            self.name_to_suffix[node_name] += 1
            suffix_num = self.name_to_suffix[node_name]

            return f"{node_name}_{suffix_num}"

    def reset(self):
        self.name_to_suffix = dict()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.reset()
