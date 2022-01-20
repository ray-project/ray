import ray

from typing import Union, List, Dict, Any, TypeVar, Callable

T = TypeVar("T")


class DAGNode:
    """Abstract class for a node in a Ray task graph."""

    def __init__(self, args: List[Any], kwargs: Dict[str, Any]):
        self._bound_args: List[Any] = args
        self._bound_kwargs: Dict[str, Any] = kwargs

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

        if self not in _cache:
            new_args, new_kwargs = [], {}
            # TODO(ekl) handle nested args.
            for a in self._bound_args:
                if isinstance(a, DAGNode):
                    new_args.append(a.transform_up(visitor, _cache))
                else:
                    new_args.append(a)
            for k, v in self._bound_kwargs:
                if isinstance(v, DAGNode):
                    new_kwargs[k] = v.transform_up(visitor, _cache)
                else:
                    new_kwargs[k] = v

            _cache[self] = visitor(self.copy(new_args, new_kwargs))

        return _cache[self]

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
