import ray

import io
import pickle
from typing import List, Dict, Any, TypeVar

T = TypeVar("T")
DAGNodeT = TypeVar("DAGNodeT")


class _PyObjScanner(ray.cloudpickle.CloudPickler):
    """Utility to find and replace DAGNodes in Python objects.

    This uses pickle to walk the PyObj graph and find first-level DAGNode
    instances on ``find_nodes()``. The caller can then compute a replacement
    table and then replace the nodes via ``replace_nodes()``.
    """

    # XXX(ekl) static instance ref used in deserialization hook.
    _cur = None

    def __init__(self, dag_node_type: DAGNodeT):
        # Buffer to keep intermediate serialized state.
        self._buf = io.BytesIO()
        # List of top-level DAGNodes found during the serialization pass.
        self._found = None
        # Replacement table to consult during deserialization.
        self._replace_table: Dict[DAGNodeT, T] = None
        # Type of the DAGNode.
        self._dag_node_type: DAGNodeT = dag_node_type
        super().__init__(self._buf)

    def find_nodes(self, obj: Any) -> List[DAGNodeT]:
        """Find top-level DAGNodes."""
        assert self._found is None, "find_nodes cannot be called twice"
        self._found = []
        self.dump(obj)
        return self._found

    def replace_nodes(self, table: Dict[DAGNodeT, T]) -> Any:
        """Replace previously found DAGNodes per the given table."""
        assert self._found is not None, "find_nodes must be called first"
        _PyObjScanner._cur = self
        self._replace_table = table
        self._buf.seek(0)
        return pickle.load(self._buf)

    def _replace_index(self, i: int) -> DAGNodeT:
        return self._replace_table[self._found[i]]

    def reducer_override(self, obj):
        if isinstance(obj, self._dag_node_type):
            index = len(self._found)
            res = (lambda i: _PyObjScanner._cur._replace_index(i)), (index,)
            self._found.append(obj)
            return res
        else:
            return super().reducer_override(obj)
