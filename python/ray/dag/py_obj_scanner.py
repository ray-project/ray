import io
import sys
from typing import Generic, List, Dict, Any, Type, TypeVar

# For python < 3.8 we need to explicitly use pickle5 to support protocol 5
if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401

import ray
from ray.dag.base import DAGNodeBase


# Used in deserialization hooks to reference scanner instances.
_instances: Dict[int, "_PyObjScanner"] = {}

# Generic types for the scanner to transform from and to.
SourceType = TypeVar("SourceType")
TransformedType = TypeVar("TransformedType")


def _get_node(instance_id: int, node_index: int) -> SourceType:
    """Get the node instance.

    Note: This function should be static and globally importable,
    otherwise the serialization overhead would be very significant.
    """
    return _instances[instance_id]._replace_index(node_index)


def _get_object(instance_id: int, node_index: int) -> Any:
    """Used to get arbitrary object other than SourceType.

    Note: This function should be static and globally importable,
    otherwise the serialization overhead would be very significant.
    """
    return _instances[instance_id]._objects[node_index]


class _PyObjScanner(ray.cloudpickle.CloudPickler, Generic[SourceType, TransformedType]):
    """Utility to find and replace the `source_type` in Python objects.

    This uses pickle to walk the PyObj graph and find first-level DAGNode
    instances on ``find_nodes()``. The caller can then compute a replacement
    table and then replace the nodes via ``replace_nodes()``.

    Args:
        source_type: the type of object to find and replace. Default to DAGNodeBase.
    """

    def __init__(self, source_type: Type = DAGNodeBase):
        self.source_type = source_type
        # Buffer to keep intermediate serialized state.
        self._buf = io.BytesIO()
        # List of top-level SourceType found during the serialization pass.
        self._found = None
        # List of other objects found during the serialization pass.
        # This is used to store references to objects so they won't be
        # serialized by cloudpickle.
        self._objects = []
        # Replacement table to consult during deserialization.
        self._replace_table: Dict[SourceType, TransformedType] = None
        _instances[id(self)] = self
        super().__init__(self._buf)

    def reducer_override(self, obj):
        """Hook for reducing objects.

        The function intercepts serialization of all objects and store them
        to internal data structures, preventing actually writing them to
        the buffer.
        """
        if obj is _get_node or obj is _get_object:
            # Only fall back to cloudpickle for these two functions.
            return super().reducer_override(obj)
        elif isinstance(obj, self.source_type):
            index = len(self._found)
            self._found.append(obj)
            return _get_node, (id(self), index)
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

    def clear(self):
        """Clear the scanner from the _instances"""
        if id(self) in _instances:
            del _instances[id(self)]

    def __del__(self):
        self.clear()
