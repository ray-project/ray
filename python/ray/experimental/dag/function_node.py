from typing import Any, Dict, List


import ray
import ray.experimental.dag as ray_dag


class FunctionNode(ray_dag.DAGNode):
    """Represents a bound task node in a Ray task DAG."""

    def __init__(
        self,
        func_body,
        func_args,
        func_kwargs,
        func_options,
        kwargs_to_resolve=None,
    ):
        self._body = func_body
        ray_dag.DAGNode.__init__(
            self,
            func_args,
            func_kwargs,
            func_options,
            kwargs_to_resolve=kwargs_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_kwargs_to_resolve: Dict[str, Any],
    ):
        return FunctionNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            kwargs_to_resolve=new_kwargs_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of FunctionNode by ray.remote()"""
        if (
            len(self._bound_args) == 1
            and self._bound_args[0] == ray_dag.DAG_ENTRY_POINT
        ):
            # DAG entrypoint, execute with user input rather than bound args.
            return (
                ray.remote(self._body)
                .options(**self._bound_options)
                .remote(*args, **kwargs)
            )
        else:
            # Execute with bound args.
            return (
                ray.remote(self._body)
                .options(**self._bound_options)
                .remote(*self._bound_args, **self._bound_kwargs)
            )
