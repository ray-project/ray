from typing import Any, Dict, List

from ray import ObjectRef
from ray.experimental.dag import DAGNode
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY, PARENT_CLASS_NODE_KEY
from ray.experimental.dag.format_utils import get_dag_node_str


class DeploymentMethodExecutorNode(DAGNode):
    """The lightweight executor DAGNode of DeploymentMethodNode that optimizes
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
        deployment_method_name: str,
        dag_args,
        dag_kwargs,
        other_args_to_resolve=None,
    ):
        super().__init__(
            dag_args, dag_kwargs, {}, other_args_to_resolve=other_args_to_resolve
        )
        self._deployment_node_replaced_by_handle = other_args_to_resolve[
            PARENT_CLASS_NODE_KEY
        ]
        self._deployment_method_name = deployment_method_name

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DeploymentMethodExecutorNode":
        return DeploymentMethodExecutorNode(
            self._deployment_method_name,
            new_args,
            new_kwargs,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs) -> ObjectRef:
        """Executor of DeploymentNode getting called each time on dag.execute.

        The execute implementation is recursive, that is, the method nodes will
        receive whatever this method returns. We return a handle here so method
        node can directly call upon.
        """
        method_body = getattr(
            self._deployment_node_replaced_by_handle, self._deployment_method_name
        )
        return method_body.remote(*self._bound_args, **self._bound_kwargs)

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._deployment_method_name))

    def to_json(self) -> Dict[str, Any]:
        return {
            DAGNODE_TYPE_KEY: DeploymentMethodExecutorNode.__name__,
            "deployment_method_name": self._deployment_method_name,
            "args": self.get_args(),
            "kwargs": self.get_kwargs(),
            "other_args_to_resolve": self.get_other_args_to_resolve(),
        }

    @classmethod
    def from_json(cls, input_json):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentMethodExecutorNode.__name__
        return cls(
            input_json["deployment_method_name"],
            input_json["args"],
            input_json["kwargs"],
            other_args_to_resolve=input_json["other_args_to_resolve"],
        )
