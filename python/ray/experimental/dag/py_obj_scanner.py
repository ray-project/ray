import ray

import uuid
import io
import sys

# For python < 3.8 we need to explicitly use pickle5 to support protocol 5
if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401

from typing import List, Dict, Any, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.experimental.dag.dag_node import DAGNode

T = TypeVar("T")


class _PyObjScanner(ray.cloudpickle.CloudPickler):
    """Utility to find and replace DAGNodes in Python objects.

    This uses pickle to walk the PyObj graph and find first-level DAGNode
    instances on ``find_nodes()``. The caller can then compute a replacement
    table and then replace the nodes via ``replace_nodes()``.
    """

    # Used in deserialization hooks to reference scanner instances.
    _instances: Dict[str, "_PyObjScanner"] = {}

    def __init__(self):
        # Buffer to keep intermediate serialized state.
        self._buf = io.BytesIO()
        # List of top-level DAGNodes found during the serialization pass.
        self._found = None
        # Replacement table to consult during deserialization.
        self._replace_table: Dict["DAGNode", T] = None
        # UUID of this scanner.
        self._uuid = uuid.uuid4().hex
        _PyObjScanner._instances[self._uuid] = self
        # Register pickler override for DAGNode types.
        from ray.experimental.dag.function_node import FunctionNode
        from ray.experimental.dag.class_node import ClassNode, ClassMethodNode
        from ray.experimental.dag.input_node import InputNode, InputAttributeNode
        from ray.serve.pipeline.deployment_node import DeploymentNode
        from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode
        from ray.serve.pipeline.deployment_function_node import DeploymentFunctionNode

        self.dispatch_table[FunctionNode] = self._reduce_dag_node
        self.dispatch_table[ClassNode] = self._reduce_dag_node
        self.dispatch_table[ClassMethodNode] = self._reduce_dag_node
        self.dispatch_table[InputNode] = self._reduce_dag_node
        self.dispatch_table[InputAttributeNode] = self._reduce_dag_node
        self.dispatch_table[DeploymentNode] = self._reduce_dag_node
        self.dispatch_table[DeploymentMethodNode] = self._reduce_dag_node
        self.dispatch_table[DeploymentFunctionNode] = self._reduce_dag_node
        super().__init__(self._buf)

    def find_nodes(self, obj: Any) -> List["DAGNode"]:
        """Find top-level DAGNodes."""
        assert (
            self._found is None
        ), "find_nodes cannot be called twice on the same PyObjScanner instance."
        self._found = []
        self.dump(obj)
        return self._found

    def replace_nodes(self, table: Dict["DAGNode", T]) -> Any:
        """Replace previously found DAGNodes per the given table."""
        assert self._found is not None, "find_nodes must be called first"
        self._replace_table = table
        self._buf.seek(0)
        return pickle.load(self._buf)

    def _replace_index(self, i: int) -> "DAGNode":
        return self._replace_table[self._found[i]]

    def _reduce_dag_node(self, obj):
        uuid = self._uuid
        index = len(self._found)
        res = (lambda i: _PyObjScanner._instances[uuid]._replace_index(i)), (index,)
        self._found.append(obj)
        return res

    def __del__(self):
        del _PyObjScanner._instances[self._uuid]
