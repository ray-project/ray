from typing import Any, Dict, List


import ray
from ray.experimental.dag import DAGNode


class TaskNode(DAGNode):
    """Represents a bound task node in a Ray task DAG."""

    def __init__(self, func_body, func_args, func_kwargs, func_options):
        self._body = func_body
        DAGNode.__init__(self, func_args, func_kwargs, func_options)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ):
        return TaskNode(self._body, new_args, new_kwargs, new_options)

    def _execute_impl(self):
        if self._bound_options:
            return (
                ray.remote(self._body)
                .options(**self._bound_options)
                .remote(*self._bound_args, **self._bound_kwargs)
            )
        else:
            return ray.remote(self._body).remote(
                *self._bound_args, **self._bound_kwargs
            )
