import base64
from importlib import import_module
import json
from typing import Any, Dict, List


import ray
import ray.cloudpickle as pickle
from ray.experimental.dag.dag_node import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY
from ray.serve.utils import parse_import_path


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

    def _execute_impl(self, *args, **kwargs):
        """Executor of FunctionNode by ray.remote().

        Args and kwargs are to match base class signature, but not in the
        implementation. All args and kwargs should be resolved and replaced
        with value in bound_args and bound_kwargs via bottom-up recursion when
        current node is executed.
        """
        return (
            ray.remote(self._body)
            .options(**self._bound_options)
            .remote(*self._bound_args, **self._bound_kwargs)
        )

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))

    def get_import_path(self):
        return f"{self._body.__module__}.{self._body.__qualname__}"

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, FunctionNode.__name__)
        import_path = self.get_import_path()
        if "__main__" in import_path or "<locals>" in import_path:
            # Best effort to get FQN string import path
            json_dict["import_path"] = base64.b64encode(
                pickle.dumps(self._body)
            ).decode()
        else:
            json_dict["import_path"] = import_path

        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == FunctionNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)

        import_path = input_json["import_path"]
        module = import_path
        if isinstance(import_path, bytes):
            # In dev mode we store pickled class or function body in import_path
            # if we failed to get a FQN import path for it.
            module = pickle.loads(base64.b64decode(json.loads(import_path)))
        else:
            module_name, attr_name = parse_import_path(import_path)
            module = getattr(import_module(module_name), attr_name)

        node = cls(
            module._function,
            args_dict["args"],
            args_dict["kwargs"],
            args_dict["options"],
            other_args_to_resolve=args_dict["other_args_to_resolve"],
        )
        node._stable_uuid = args_dict["uuid"]
        return node
