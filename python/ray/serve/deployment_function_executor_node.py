from typing import Any, Dict, List, Union

from ray import ObjectRef
from ray.dag import DAGNode
from ray.serve.handle import RayServeSyncHandle, RayServeHandle
from ray.dag.constants import DAGNODE_TYPE_KEY
from ray.dag.format_utils import get_dag_node_str


class DeploymentFunctionExecutorNode(DAGNode):
    """The lightweight executor DAGNode of DeploymentFunctionNode that optimizes
    for efficiency.

        - We need Ray DAGNode's traversal and replacement mechanism to deal
            with deeply nested nodes as args in the DAG
        - Meanwhile, __init__, _copy_impl and _execute_impl are on the critical
            pass of execution for every request.

    Therefore for serve we introduce a minimal weight node as the final product
    of DAG transformation, and will be used in actual execution as well as
    deployment.
    """

    def __init__(
        self,
        deployment_function_handle: Union[RayServeSyncHandle, RayServeHandle],
        func_args,
        func_kwargs,
    ):
        super().__init__(
            func_args,
            func_kwargs,
            {},
            {},
        )
        self._deployment_function_handle = deployment_function_handle

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DeploymentFunctionExecutorNode":
        return DeploymentFunctionExecutorNode(
            self._deployment_function_handle, new_args, new_kwargs
        )

    def _execute_impl(self, *args, **kwargs) -> ObjectRef:
        """Executor of DeploymentNode getting called each time on dag.execute.

        The execute implementation is recursive, that is, the method nodes will
        receive whatever this method returns. We return a handle here so method
        node can directly call upon.
        """
        return self._deployment_function_handle.remote(
            *self._bound_args, **self._bound_kwargs
        )

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._deployment_function_handle))

    def to_json(self) -> Dict[str, Any]:
        return {
            DAGNODE_TYPE_KEY: DeploymentFunctionExecutorNode.__name__,
            "deployment_function_handle": self._deployment_function_handle,
            "args": self.get_args(),
            "kwargs": self.get_kwargs(),
        }

    @classmethod
    def from_json(cls, input_json):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentFunctionExecutorNode.__name__
        return cls(
            input_json["deployment_function_handle"],
            input_json["args"],
            input_json["kwargs"],
        )
