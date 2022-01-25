import ray
from ray.experimental.dag.dag_node import DAGNode


class TaskNode(DAGNode):
    """Represents a bound task node in a Ray task DAG."""

    # TODO(ekl) support task options
    def __init__(self, func_body, func_args, func_kwargs):
        self._body = func_body
        self._node_type = f"{self.__class__.__name__}"
        DAGNode.__init__(self, func_args, func_kwargs)

    def _copy(self, new_args, new_kwargs):
        return TaskNode(self._body, new_args, new_kwargs)

    def _execute(self):
        return ray.remote(self._body).remote(*self._bound_args,
                                                  **self._bound_kwargs)