import ray

import io
import pickle
from typing import Union, List, Tuple, Dict, Any, TypeVar, Callable, Set

T = TypeVar("T")


class DAGNode:
    """Abstract class for a node in a Ray task graph.

    A node has a type (e.g., TaskNode), data (e.g., function options and body),
    and arguments (Python values, DAGNodes, and DAGNodes nested within Python
    argument values).
    """

    def __init__(self, args: Tuple[Any], kwargs: Dict[str, Any]):
        self._bound_args: Tuple[Any] = args
        self._bound_kwargs: Dict[str, Any] = kwargs

    def get_args(self) -> Tuple[Any]:
        """Return the tuple of arguments for this node."""

        return self._bound_args

    def get_kwargs(self) -> Dict[str, Any]:
        """Return the dict of keyword arguments for this node."""

        return self._bound_kwargs.copy()

    def get_toplevel_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes specified as top-level args.

        For example, in `f.remote(a, [b])`, only `a` is a top-level arg.

        This set of nodes are those that are typically resolved prior to
        task execution in Ray. This does not include nodes nested within args.
        For that, use ``get_all_child_nodes()``.
        """

        children = set()
        for a in self.get_args():
            if isinstance(a, DAGNode):
                children.add(a)
        for a in self.get_kwargs().values():
            if isinstance(a, DAGNode):
                children.add(a)
        return children

    def get_all_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes referenced by the args of this node.

        For example, in `f.remote(a, [b])`, this includes both `a` and `b`.
        """

        f = _PyObjFindReplace()
        children = set()
        for n in f.find_nodes([self._bound_args, self._bound_kwargs]):
            children.add(n)
        return children

    def transform_up(self,
                     visitor: "Callable[[DAGNode], T]",
                     _cache: Dict["DAGNode", T] = None) -> T:
        """Transform each node in this DAG in a bottom-up tree walk.

        Args:
            visitor: Callable that will be applied once to each node in the
                DAG. It will be applied recursively bottom-up, so nodes can
                assume the visitor has been applied to their args already.
            _cache: Dict used to de-duplicate applications of visitor.

        Returns:
            Return type of the visitor after application to the tree.
        """

        if _cache is None:
            _cache = {}

        # Find all first-level nested DAGNode children in args.
        f = _PyObjFindReplace()
        children = f.find_nodes([self._bound_args, self._bound_kwargs])

        # Update replacement table and execute the replace.
        for node in children:
            if node not in _cache:
                new_node = node.transform_up(visitor, _cache)
                _cache[node] = new_node
        new_args, new_kwargs = f.replace_nodes(_cache)

        # Apply visitor after args have been recursively updated.
        return visitor(self.copy(new_args, new_kwargs))

    def execute(self) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this DAG using the Ray default executor."""
        return self.transform_up(lambda node: node._execute())

    def tree_string(self) -> str:
        """Return a string representation of the entire DAG."""
        # TODO(ekl) format with indentation, etc.
        return self.transform_up(str)

    def _execute(self) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this node, assuming args have been transformed already."""
        raise NotImplementedError

    def copy(self, new_args: List[Any],
             new_kwargs: Dict[str, Any]) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        raise NotImplementedError

    def __reduce__(self):
        """We disallow serialization to prevent inadvertent closure-capture.

        Use ``.to_json()`` and ``.from_json()`` to convert DAGNodes to a
        serializable form.
        """
        raise ValueError("DAGNode cannot be serialized.")


class _PyObjFindReplace(ray.cloudpickle.CloudPickler):
    """Utility to find and replace DAGNodes in Python objects.

    This uses pickle to walk the PyObj graph and find first-level DAGNode
    instances on ``find_nodes()``. The caller can then compute a replacement
    table and then replace the nodes via ``replace_nodes()``.
    """

    # XXX(ekl) static instance ref used in deserialization hook.
    _cur = None

    def __init__(self):
        # Buffer to keep intermediate serialized state.
        self._buf = io.BytesIO()
        # List of top-level DAGNodes found during the serialization pass.
        self._found = None
        # Replacement table to consult during deserialization.
        self._replace_table: Dict[DAGNode, T] = None
        super().__init__(self._buf)

    def find_nodes(self, obj: Any) -> List[DAGNode]:
        """Find top-level DAGNodes."""
        assert self._found is None, "find_nodes cannot be called twice"
        self._found = []
        self.dump(obj)
        return self._found

    def replace_nodes(self, table: Dict[DAGNode, T]) -> Any:
        """Replace previously found DAGNodes per the given table."""
        assert self._found is not None, "find_nodes must be called first"
        _PyObjFindReplace._cur = self
        self._replace_table = table
        self._buf.seek(0)
        return pickle.load(self._buf)

    def _replace_index(self, i: int) -> DAGNode:
        return self._replace_table[self._found[i]]

    def reducer_override(self, obj):
        if isinstance(obj, DAGNode):
            index = len(self._found)
            res = (lambda i: _PyObjFindReplace._cur._replace_index(i)), (
                index, )
            self._found.append(obj)
            return res
        else:
            return super().reducer_override(obj)
