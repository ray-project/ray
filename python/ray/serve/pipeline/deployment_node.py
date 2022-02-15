
from typing import Any, Dict, List

from ray.experimental.dag import DAGNode
from ray.serve.api import Deployment
from ray.serve.handle import RayServeHandle


class DeploymentNode(DAGNode):
    """Represents a deployment node in a DAG authored Ray DAG API."""

    def __init__(
        self,
        deployment_body,
        deployment_name,
        cls_args,
        cls_kwargs,
        cls_options,
        other_args_to_resolve=None,
    ):
        self._body: Deployment = deployment_body
        self._deployment_name: str = deployment_name
        # TODO: (jiaodong) Support async handle
        self._deployment_handle: RayServeHandle = deployment_body.get_handle()

        super().__init__(
            cls_args,
            cls_kwargs,
            cls_options,
            other_args_to_resolve=other_args_to_resolve,
        )

        if self._contain_input_node():
            raise ValueError(
                "InputNode handles user dynamic input the the DAG, and "
                "cannot be used as args, kwargs, or other_args_to_resolve "
                "in DeploymentNode constructor because it is not available at "
                "class construction or binding time."
            )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args):
        """Executor of ClassNode by ray.remote()"""
        return self._deployment_handle.options(**self._bound_options).remote(
            *self._bound_args, **self._bound_kwargs
        )