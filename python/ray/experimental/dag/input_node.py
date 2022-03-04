from typing import Any, Dict, List, Union

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY


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
    DONE - Binding value to InputNode ? No, just schema
    - Take input data python object
    - Globally unique InputNode
    - Split attributes
    """

    def __init__(self, *args, **kwargs):
        """InputNode should only take attributes of validating and converting
        input data rather than the input data itself. User input should be
        provided via `ray_dag.execute(user_input)`.
        """
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

    def _execute_impl(self, *args, **kwargs):
        """Executor of InputNode by ray.remote()"""
        # If user only passed in one value, for simplicity we just return it.
        if len(args) == 1 and len(kwargs) == 0:
            return args[0]

        return DAGInputData(*args, **kwargs)

    def __str__(self) -> str:
        return get_dag_node_str(self, "__InputNode__")

    def __getattr__(self, key: str):
        return InputAtrributeNode(self, key)

    def __getitem__(self, key: Union[int, str]) -> Any:
        return InputAtrributeNode(self, key)

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


class InputAtrributeNode(DAGNode):
    """Represents partial acces of user input based on an attribute key.

    Examples:
        >>> input = InputNode()
        >>> a = input[0]
        >>> b = input.x

        >>> # This makes a = 1 and b = 2
        >>> ray_dag.execute(1, x=2)
    """

    def __init__(self, dag_input_node: InputNode, key: str):
        self._dag_input_node = dag_input_node
        self._key = key
        super().__init__([], {}, {}, {"dag_input_node": dag_input_node, "key": key})

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return InputAtrributeNode(
            new_other_args_to_resolve["dag_input_node"],
            new_other_args_to_resolve["key"],
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of InputNode by ray.remote()"""

        if isinstance(self._dag_input_node, DAGInputData):
            return self._dag_input_node[self._key]
        else:
            #
            return self._dag_input_node

    def __str__(self) -> str:
        return get_dag_node_str(self, f"__InputNode__[{self._key}]")


class DAGInputData:
    """Wrapped all user inputs as one object, accessible via attribute key.

    Example:
        >>> @ray.remote
        >>> class Model:
        ...     def __init__(self, val):
        ...         self.val = val
        ...     def forward(self, input):
        ...         return self.val * input

        >>> @ray.remote
        >>> def combine(a, b):
        ...     return a + b

        >>> input = InputNode()
        >>> m1 = Model.bind(1)
        >>> m2 = Model.bind(2)
        >>> m1_output = m1.forward.bind(input[0])
        >>> m2_output = m2.forward.bind(input[1])
        >>> ray_dag = combine.bind(m1_output, m2_output)

        >>> # Pass mix of args and kwargs as input.
        >>> print(ray_dag.execute(1, 2)) # 1 sent to m1, 2 sent to m2
        >>> 5

    """

    def __init__(self, *args, **kwargs):
        self._args = list(args)
        self._kwargs = kwargs

    def __getitem__(self, key: Union[int, str]) -> Any:
        if isinstance(key, int):
            # Accessing list args,
            return self._args[key]
        else:
            return self._kwargs[key]
