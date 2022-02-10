from typing import Any, Dict, List


import ray
from ray.experimental.dag.dag_node import DAGNode
from ray.experimental.dag.input_node import InputNode


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
        # TODO: (jiaodong) Disallow binding other args if InputNode is used.
        # revisit this constraint before moving out of experimental folder
        has_input_node = self._contain_input_node()
        if has_input_node:
            # Invalid usecases:
            # f.bind(InputNode(), 1, 2)
            # f.bind(1, 2, key=InputNode())
            # f.bind({"nested": InputNode()})
            # f.bind(InputNode(), key=123)
            if (
                len(self.get_args()) != 1
                or not isinstance(self.get_args()[0], InputNode)
                or self.get_kwargs() != {}
                or self.get_other_args_to_resolve() != {}
            ):
                raise ValueError(
                    "InputNode marks the entrypoint of user request to the "
                    "DAG, please ensure InputNode is the only input to a "
                    "FunctionNode, and NOT used in conjunction with, or "
                    "nested within other args or kwargs."
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
