from typing import Any, Dict, List


import ray
from ray.experimental.dag.dag_node import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str


class FunctionNode(DAGNode):
    """Represents a bound task node in a Ray task DAG."""

    def __init__(
        self,
        func_body,
        func_args,
        func_kwargs,
        func_options,
        other_args_to_resolve=None,
    ):
        self._body = func_body
        super().__init__(
            func_args,
            func_kwargs,
            func_options,
            other_args_to_resolve=other_args_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return FunctionNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args):
        """Executor of FunctionNode by ray.remote()"""
        return (
            ray.remote(self._body)
            .options(**self._bound_options)
            .remote(*self._bound_args, **self._bound_kwargs)
        )

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))
