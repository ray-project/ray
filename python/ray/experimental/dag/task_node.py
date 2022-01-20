import ray
from ray.experimental.dag.dag_node import DAGNode


class TaskNode(DAGNode):
    def __init__(self, func_body, func_args, func_kwargs):
        self._func_body = func_body
        DAGNode.__init__(self, func_args, func_kwargs)

    def copy(self, new_args, new_kwargs):
        return TaskNode(self._func_body, new_args, new_kwargs)

    def _execute(self):
        return ray.remote(self._func_body).remote(*self._bound_args,
                                                  **self._bound_kwargs)

    def __str__(self):
        return "TaskNode(func={}, args={}, kwargs={})".format(
            self._func_body, self._bound_args, self._bound_kwargs)
