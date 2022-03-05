from typing import Any, Dict, List, Union

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY

IN_CONTEXT_MANAGER = "__in_context_manager__"


class InputNode(DAGNode):
    """Ray dag node used in DAG building API to mark entrypoints of a DAG.

    Should only be function or class method. A DAG can have multiple
    entrypoints, but only one instance of InputNode exists per DAG, shared
    among all DAGNodes.

    Ex:
                   A.forward
                /            \
        dag_input              ensemble -> dag_output
                \            /
                   B.forward

    In this pipeline, each user input is broadcasted to both A.forward and
    B.forward as first stop of the DAG, and authored like

    with InputNode() as dag_input:
        a = A.forward.bind(dag_input)
        b = B.forward.bind(dag_input)
        dag = ensemble.bind(a, b)

    dag.execute(user_input) --> broadcast to a and b
    """

    def __init__(self, *args, _other_args_to_resolve=None, **kwargs):
        """InputNode should only take attributes of validating and converting
        input data rather than the input data itself. User input should be
        provided via `ray_dag.execute(user_input)`.

        Args:
            _other_args_to_resolve: Internal only to keep InputNode's execution
                context throughput pickling, replacement and serialization.
                User should not use or pass this field.
        """
        if len(args) != 0 or len(kwargs) != 0:
            raise ValueError("InputNode should not take any args or kwargs.")

        super().__init__([], {}, {}, other_args_to_resolve=_other_args_to_resolve)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return InputNode(_other_args_to_resolve=new_other_args_to_resolve)

    def _execute_impl(self, *args, **kwargs):
        """Executor of InputNode."""
        # Catch and assert singleton context at dag execution time.
        assert self._in_context_manager(), (
            "InputNode is a singleton instance that should be only used in "
            "context manager for dag building and execution. See the docstring "
            "of class InputNode for examples."
        )
        # If user only passed in one value, for simplicity we just return it.
        if len(args) == 1 and len(kwargs) == 0:
            return args[0]

        return DAGInputData(*args, **kwargs)

    def _in_context_manager(self) -> bool:
        """Return if InputNode is created in context manager."""
        if (
            not self._bound_other_args_to_resolve
            or IN_CONTEXT_MANAGER not in self._bound_other_args_to_resolve
        ):
            return False
        else:
            return self._bound_other_args_to_resolve[IN_CONTEXT_MANAGER]

    def _set_context(self, key: str, val: Any):
        """Set field in parent DAGNode attribute that can be resolved in both
        pickle and JSON serialization
        """
        self._bound_other_args_to_resolve[key] = val

    def __str__(self) -> str:
        return get_dag_node_str(self, "__InputNode__")

    def __getattr__(self, key: str):
        return InputAtrributeNode(self, key)

    def __getitem__(self, key: Union[int, str]) -> Any:
        return InputAtrributeNode(self, key)

    def __enter__(self):
        self._set_context(IN_CONTEXT_MANAGER, True)
        return self

    def __exit__(self, *args):
        pass

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        return super().to_json_base(encoder_cls, InputNode.__name__)

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == InputNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        return cls(_other_args_to_resolve=args_dict["other_args_to_resolve"])


class InputAtrributeNode(DAGNode):
    """Represents partial acces of user input based on an attribute key.

    Examples:
        >>> with InputNode() as dag_input:
        >>>     a = input[0]
        >>>     b = input.x
        >>>     ray_dag = add.bind(a, b)

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
        """Executor of InputAtrributeNode.

        Args and kwargs are to match base class signature, but not in the
        implementation. All args and kwargs should be resolved and replaced
        with value in bound_args and bound_kwargs via bottom-up recursion when
        current node is executed.
        """
        if isinstance(self._dag_input_node, DAGInputData):
            return self._dag_input_node[self._key]
        else:
            # dag.execute() is called with only one arg, thus when an
            # InputAtrributeNode is executed, its dependent InputNode is
            # resolved with original user input value.
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

        >>> with InputNode() as dag_input:
        >>>     m1 = Model.bind(1)
        >>>     m2 = Model.bind(2)
        >>>     m1_output = m1.forward.bind(dag_input[0])
        >>>     m2_output = m2.forward.bind(dag_input[1])
        >>>     ray_dag = combine.bind(m1_output, m2_output)

        >>> # Pass mix of args and kwargs as input.
        >>> print(ray_dag.execute(1, 2)) # 1 sent to m1, 2 sent to m2
        >>> 5

    """

    def __init__(self, *args, **kwargs):
        self._args = list(args)
        self._kwargs = kwargs

    def __getitem__(self, key: Union[int, str]) -> Any:
        if isinstance(key, int):
            # Access list args by index.
            return self._args[key]
        else:
            # Access kwarg by key.
            return self._kwargs[key]
