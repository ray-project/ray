import io
import pickle  # noqa: F401
from typing import Any, Dict, Generic, List, Tuple, Type, TypeVar, Union

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


class _PyObjScanner(ray.cloudpickle.CloudPickler, Generic[SourceType, TransformedType]):
    """Utility to find and replace the `source_type` in Python objects.

    `source_type` can either be a single type or a tuple of multiple types.

    The caller must first call `find_nodes()`, then compute a replacement table and
    pass it to `replace_nodes`.

    This uses cloudpickle under the hood, so all sub-objects that are not `source_type`
    must be serializable.

    Args:
        source_type: the type(s) of object to find and replace. Default to DAGNodeBase.
    """

    def __init__(self, source_type: Union[Type, Tuple] = DAGNodeBase):
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

        Objects of `self.source_type` are saved to `self._found` and a global map so
        they can later be replaced.

        All other objects fall back to the default `CloudPickler` serialization.
        """
        if isinstance(obj, self.source_type):
            index = len(self._found)
            self._found.append(obj)
            return _get_node, (id(self), index)

        return super().reducer_override(obj)

    def find_nodes(self, obj: Any) -> List[SourceType]:
        """
        Serialize `obj` and store all instances of `source_type` found in `_found`.

        Args:
            obj: The object to scan for `source_type`.
        Returns:
            A list of all instances of `source_type` found in `obj`.
        """
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
