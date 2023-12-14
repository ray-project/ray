import ray
from typing import Any, Dict, List, Union, Tuple

from ray.dag import DAGNode
from ray.dag.format_utils import get_dag_node_str
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class MultiOutputNode(DAGNode):
    """Ray dag node used in DAG building API to mark the endpoint of DAG"""

    def __init__(
        self,
        args: Union[List[DAGNode], Tuple[DAGNode]],
        other_args_to_resolve: Dict[str, Any] = None,
    ):
        if isinstance(args, tuple):
            args = list(args)
        if not isinstance(args, list):
            raise ValueError(f"Invalid input type for `args`, {type(args)}.")
        if not isinstance(args, list):
            args = (args,)
        super().__init__(
            args,
            {},
            {},
            other_args_to_resolve=other_args_to_resolve or {},
        )

    def _execute_impl(
        self, *args, **kwargs
    ) -> Union[ray.ObjectRef, "ray.actor.ActorHandle"]:
        return self._bound_args

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        return MultiOutputNode(new_args, new_other_args_to_resolve)

    def __str__(self) -> str:
        return get_dag_node_str(self, "__MultiOutputNode__")
