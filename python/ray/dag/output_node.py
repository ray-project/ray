import ray
from typing import Any, Dict, List, Union, Tuple

from ray.dag import DAGNode
from ray.dag.format_utils import get_dag_node_str
from ray.experimental.gradio_utils import type_to_string
from ray.util.annotations import Deprecated

IN_CONTEXT_MANAGER = "__in_context_manager__"


class OutputNode(DAGNode):
    r"""Ray dag node used in DAG building API to mark the endpoint of DAG
    """

    def __init__(
        self,
        args: Union[DAGNode, List[DAGNode], Tuple[DAGNode]],
        other_args_to_resolve: Dict[str, Any] = None,
    ):
        if isinstance(args, tuple):
            args = list(args)
        if not isinstance(args, list):
            args = (args,)
        super().__init__(
            args,
            {},
            {},
            other_args_to_resolve=other_args_to_resolve or {},
        )

    def _execute_impl(self, *args, **kwargs) -> Union[ray.ObjectRef, "ray.actor.ActorHandle"]:
        if len(self._bound_args) == 1:
            return self._bound_args[0]
        else:
            return self._bound_args

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        return OutputNode(new_args, new_other_args_to_resolve)
    
    def __str__(self) -> str:
        return get_dag_node_str(self, "__OutputNode__")
