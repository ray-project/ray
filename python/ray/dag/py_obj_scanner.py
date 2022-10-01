import io
import threading
from typing import Generic, List, Any, Type, TypeVar, Callable, Dict

import ray
from ray.dag.base import DAGNodeBase
from ray.cloudpickle.compat import pickle

_local = threading.local()

# Generic types for the scanner to transform from and to.
SourceType = TypeVar("SourceType")
TransformedType = TypeVar("TransformedType")


def _get_object(index: int) -> SourceType:
    """Get the object of type SourceType.

    Note: This function should be static and globally importable,
    otherwise the serialization overhead would be very significant.
    """
    return _local.found_objects[index]


def _get_unrelated_object(index: int) -> Any:
    """Used to get arbitrary object other than SourceType.

    Note: This function should be static and globally importable,
    otherwise the serialization overhead would be very significant.
    """
    return _local.scanner.unrelated_objects[index]


def _in_context() -> bool:
    return hasattr(_local, "scanner")


class PyObjScanner(ray.cloudpickle.CloudPickler, Generic[SourceType, TransformedType]):
    """Utility to find and replace the `source_type` in Python objects.

    This uses pickle to walk the PyObj graph and find first-level DAGNode
    instances on ``find_nodes()``. The caller can then compute a replacement
    table and then replace the nodes via ``replace_nodes()``.

    Args:
        source_type: the type of object to find and replace. Default to DAGNodeBase.
    """

    def __init__(self, obj, source_type: Type = DAGNodeBase):
        self._obj = obj
        self._source_type = source_type
        # Buffer to keep intermediate serialized state.
        self._buf = io.BytesIO()

        # List of top-level SourceType found during the serialization pass.
        self.found_objects: List[SourceType] = []
        # List of other objects found during the serialization pass.
        # This is used to store references to objects so they won't be
        # serialized by cloudpickle.
        self.unrelated_objects: List[Any] = []
        super().__init__(self._buf)

    def __enter__(self):
        if _in_context():
            raise RuntimeError("Cannot use object scanner in a nested way.")
        _local.scanner = self
        self.dump(self._obj)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del _local.scanner  # cleanup

    def reducer_override(self, obj):
        """Hook for reducing objects.

        The function intercepts serialization of all objects and store them
        to internal data structures, preventing actually writing them to
        the buffer.
        """
        if obj is _get_object or obj is _get_unrelated_object:
            # Handle global function with pickle.
            return NotImplemented
        elif isinstance(obj, self._source_type):
            index = len(self.found_objects)
            self.found_objects.append(obj)
            return _get_object, (index,)
        else:
            index = len(self.unrelated_objects)
            self.unrelated_objects.append(obj)
            return _get_unrelated_object, (index,)

    def replace_with_list(self, replace_list: List[TransformedType]):
        """Replace found objects with a list.

        Args:
            replace_list: The list to replace the found objects.
        """
        if not _in_context():
            raise RuntimeError("PyObjScanner must be used under 'with' context.")
        if len(replace_list) != len(self.found_objects):
            raise ValueError(
                "The length of the replacement list should be same "
                "as the found objects."
            )
        self.found_objects = replace_list

    def replace_with_func(
        self,
        replace_func: Callable[[SourceType], TransformedType],
        cache_inputs: bool = True,
    ):
        """Replace found objects with a function.

        Args:
            replace_func: The function that transforms the found objects.
            cache_inputs: If True, the function would apply to the same objects
                only once.
        """
        if not _in_context():
            raise RuntimeError("PyObjScanner must be used under 'with' context.")
        if cache_inputs:
            replace_list = []
            replace_table = {}
            for v in self.found_objects:
                if v not in replace_table:
                    replace_table[v] = replace_func(v)
                replace_list.append(replace_table[v])
        else:
            replace_list = [replace_func(v) for v in self.found_objects]
        self.replace_with_list(replace_list)

    def replace_with_dict(self, replace_dict: Dict[SourceType, TransformedType]):
        """Replace found objects with a dict.

        Args:
            replace_dict: The dict to replace the found objects.
        """
        self.replace_with_func(replace_dict.__getitem__, False)

    def reconstruct(self):
        """Reconstruct the object. If objects in 'found_objects' are replaced,
        these objects would reveal in the reconstructed object."""
        if not _in_context():
            raise RuntimeError("PyObjScanner must be used under 'with' context.")
        self._buf.seek(0)
        return pickle.load(self._buf)
