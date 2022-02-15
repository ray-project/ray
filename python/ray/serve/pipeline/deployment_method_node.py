from typing import Any, Dict, Tuple, List

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import (
    get_args_lines,
    get_kwargs_lines,
    get_options_lines,
    get_other_args_to_resolve_lines,
    get_indentation,
)
from ray.serve.api import Deployment
from ray.serve.handle import RayServeHandle

class DeploymentMethodNode(DAGNode):
    """Represents a deployment method invocation in a Ray function DAG."""

    def __init__(
        self,
        deployment_body: Deployment,
        deployment_name: str,
        method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
    ):
        self._body = deployment_body
        self._deployment_name: str = deployment_name
        self._method_name: str = method_name
        self._deployment_handle: RayServeHandle = deployment_body.get_handle()

        self._bound_args = method_args or []
        self._bound_kwargs = method_kwargs or {}
        self._bound_options = method_options or {}

        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve={},
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentMethodNode(
            self._body,
            self._deployment_name,
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
        )

    def _execute_impl(self, *args):
        """Executor of ClassMethodNode by ray.remote()"""
        # Execute with bound args.
        return self._deployment_handle.options(**self._bound_options).remote(
            *self._bound_args,
            **self._bound_kwargs,
        )

    def __str__(self) -> str:
        indent = get_indentation()

        method_line = str(self._method_name) + "()"
        body_line = str(self._body)

        args_line = get_args_lines(self._bound_args)
        kwargs_line = get_kwargs_lines(self._bound_kwargs)
        options_line = get_options_lines(self._bound_options)
        other_args_to_resolve_line = get_other_args_to_resolve_lines(
            self._bound_other_args_to_resolve
        )
        node_type = f"{self.__class__.__name__}"

        return (
            f"({node_type})(\n"
            f"{indent}method={method_line}\n"
            f"{indent}deployment={body_line}\n"
            f"{indent}args={args_line}\n"
            f"{indent}kwargs={kwargs_line}\n"
            f"{indent}options={options_line}\n"
            f"{indent}other_args_to_resolve={other_args_to_resolve_line}\n"
            f")"
        )