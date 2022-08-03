import ray

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

from typing import Generic, List, Dict, Any, Type, TypeVar
from ray.dag.base import DAGNodeBase


# Used in deserialization hooks to reference scanner instances.
_instances: Dict[int, "_PyObjScanner"] = {}


def _get_node(instance_id: int, node_index: int) -> DAGNodeBase:
    """Get the node instance.

    Note: This function should be static and globally importable,
    otherwise the serialization overhead would be very significant.
    """
    return _instances[instance_id]._replace_index(node_index)


def _get_object(instance_id, node_index):
    return _instances[instance_id]._objects[node_index]


SourceType = TypeVar("SourceType")
TransformedType = TypeVar("TransformedType")


class _PyObjScanner(ray.cloudpickle.CloudPickler, Generic[SourceType, TransformedType]):
    """Utility to find and replace DAGNodes in Python objects.

    This uses pickle to walk the PyObj graph and find first-level DAGNode
    instances on ``find_nodes()``. The caller can then compute a replacement
    table and then replace the nodes via ``replace_nodes()``.
    """

    def __init__(self, scan_type: Type = DAGNodeBase):
        self.scan_type = scan_type
        # Buffer to keep intermediate serialized state.
        self._buf = io.BytesIO()
        # List of top-level DAGNodes found during the serialization pass.
        self._found = None
        # List of other objects found during the serializatoin pass.
        # TODO: docs
        # TODO: make weakref
        self._objects = {}
        # Replacement table to consult during deserialization.
        self._replace_table: Dict[SourceType, TransformedType] = None
        _instances[id(self)] = self
        super().__init__(self._buf)

    def reducer_override(self, obj):
        """Hook for reducing objects."""
        if isinstance(obj, self.scan_type):
            index = len(self._found)
            self._found.append(obj)
            return _get_node, (id(self), index)
        elif obj is _get_object:
            return super().reducer_override(obj)
        else:
            index = len(self._objects)
            self._objects.append(obj)
            return _get_object, (id(self), index)

    def find_nodes(self, obj: Any) -> List[SourceType]:
        """Find top-level DAGNodes."""
        assert (
            self._found is None
        ), "find_nodes cannot be called twice on the same PyObjScanner instance."
        self._found = []
        self._objects = []
        self.dump(obj)
        return self._found

    def replace_nodes(self, table: Dict[SourceType, TransformedType]) -> Any:
        """Replace previously found DAGNodes per the given table."""
        assert self._found is not None, "find_nodes must be called first"
        self._replace_table = table
        self._buf.seek(0)
        return pickle.load(self._buf)

    def _replace_index(self, i: int) -> SourceType:
        return self._replace_table[self._found[i]]

    def __del__(self):
        del _instances[id(self)]
