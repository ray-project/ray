from typing import Any, Dict, List

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY


class InputNode(DAGNode):
    """Ray dag node used in DAG building API to mark entrypoints of a DAG.

    Should only be function or class method. A DAG can have multiple
    entrypoints.

    Ex:
                A.forward
             /            \
        input               ensemble -> output
             \            /
                B.forward

    In this pipeline, each user input is broadcasted to both A.forward and
    B.forward as first stop of the DAG, and authored like

    a = A.forward.bind(ray.dag.InputNode())
    b = B.forward.bind(ray.dag.InputNode())
    dag = ensemble.bind(a, b)

    dag.execute(user_input) --> broadcast to a and b
    """

    def __init__(self, *args, **kwargs):
        # TODO: (jiaodong) Support better structured user input data
        if len(args) != 0 or len(kwargs) != 0:
            raise ValueError("InputNode should not take any args or kwargs.")
        super().__init__([], {}, {}, {})

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return InputNode()

    def _execute_impl(self, *args):
        """Executor of InputNode by ray.remote()"""
        # TODO: (jiaodong) Extend this to take more complicated user inputs
        return args[0]

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
