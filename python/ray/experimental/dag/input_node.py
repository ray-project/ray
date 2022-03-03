from calendar import c
from typing import Any, Callable, Dict, List, Optional

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY

# Internal keys used to keep track of key fields throughout Ray DAG execution,
# copy and replacement.
INPUT_SCHEMA_KEY = "__input_schema__"
ADAPTER_FN_KEY = "__adapter_fn__"

#TODO (jiaodong): Better interface without depending on pydantic
class InputSchema:
    def __init__(self, validator: Optional[Callable] = None):
        self.validator = validator

    def validate(self, input_data: Any):
        self.validator(input_data)

class InputNode(DAGNode):
    """Ray dag node used in DAG building API to mark entrypoints of a DAG.

    Should only be function or class method. A DAG can have multiple
    entrypoints, but only one instance of InputNode exists per DAG, shared
    among all DAGNodes.

    Ex:
                A.forward
             /            \
        input               ensemble -> output
             \            /
                B.forward

    In this pipeline, each user input is broadcasted to both A.forward and
    B.forward as first stop of the DAG, and authored like

    input = ray.dag.InputNode()
    a = A.forward.bind(input)
    b = B.forward.bind(input)
    dag = ensemble.bind(a, b)

    dag.execute(user_input) --> broadcast to a and b
    """

    """
    1) Binding value to InputNode ? No, just schema
    """

    def __init__(
        self,
        input_schema: Optional[InputSchema] = None,
        adapter_fn: Optional[Callable] = None,
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        self._input_schema = input_schema
        self._adapter_fn = adapter_fn

        super().__init__(
            [],
            {
                INPUT_SCHEMA_KEY: input_schema,
                ADAPTER_FN_KEY: adapter_fn,
            },
            {},
            other_args_to_resolve=other_args_to_resolve
        )

    def _validate_input(self, input_data):
        if self._input_schema:
            self._input_schema.validate(input_data)
        else:
            pass

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return InputNode(
            new_kwargs[INPUT_SCHEMA_KEY],
            new_kwargs[ADAPTER_FN_KEY],
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, input_data):
        """Executor of InputNode by ray.remote()"""
        # TODO: (jiaodong) Maybe a contenxt manager ?
        self._validate_input(input_data)
        converted_data = self._adapter_fn(input_data)
        print(f"Returned converted_data: {converted_data}")
        return converted_data

    def __str__(self) -> str:
        return get_dag_node_str(self, "__InputNode__")

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        # TODO: (jiaodong) Support arbitrary InputNode args and pydantic
        # input schema.
        json_dict = super().to_json_base(encoder_cls, InputNode.__name__)
        return json_dict

    @classmethod
    def from_json(cls, input_json):
        assert input_json[DAGNODE_TYPE_KEY] == InputNode.__name__
        # TODO: (jiaodong) Support user passing inputs to InputNode in JSON
        return cls()
