from typing import Any, Type, Callable, Dict, List, Union, Optional

from ray.experimental.dag import InputNode, InputAtrributeNode, DAGInputData
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY


class PipelineInputNode(InputNode):
    """Node used in DAG building API to mark entrypoints of a Serve Pipeline.
    The extension of Ray DAG level InputNode with additional abilities such
    as input schema validation and input conversion for HTTP.

    # TODO (jiaodong): Add a concrete example here
    """

    def __init__(self, input_schema: Optional[Union[Type, Callable]] = None, _other_args_to_resolve=None):
        """InputNode should only take attributes of validating and converting
        input data rather than the input data itself. User input should be
        provided via `ray_dag.execute(user_input)`.

        Args:
            input_schema: User class that

            _other_args_to_resolve: Internal only to keep InputNode's execution
                context throughput pickling, replacement and serialization.
                User should not use or pass this field.
        """
        self._input_schema = input_schema

        super().__init__(_other_args_to_resolve=_other_args_to_resolve)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return PipelineInputNode(_other_args_to_resolve=new_other_args_to_resolve)

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


    def __str__(self) -> str:
        return get_dag_node_str(self, "__PipelineInputNode__")


    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, PipelineInputNode.__name__)
        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == PipelineInputNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        node = cls(_other_args_to_resolve=args_dict["other_args_to_resolve"])
        node._stable_uuid = input_json["uuid"]
        return node


class PipelineInputAtrributeNode(InputAtrributeNode):
    """Represents partial access of user input based on an index (int),
     object attribute or dict key (str).

    Examples:
        >>> with InputNode() as dag_input:
        >>>     a = input[0]
        >>>     b = input.x
        >>>     ray_dag = add.bind(a, b)

        >>> # This makes a = 1 and b = 2
        >>> ray_dag.execute(1, x=2)

        >>> with InputNode() as dag_input:
        >>>     a = input[0]
        >>>     b = input[1]
        >>>     ray_dag = add.bind(a, b)

        >>> # This makes a = 2 and b = 3
        >>> ray_dag.execute([2, 3])
    """

    def __init__(self, dag_input_node: InputNode, key: str, accessor_method: str):
        self._dag_input_node = dag_input_node
        self._key = key
        self._accessor_method = accessor_method
        super().__init__(
            [],
            {},
            {},
            {
                "dag_input_node": dag_input_node,
                "key": key,
                "accessor_method": accessor_method,
            },
        )

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
            new_other_args_to_resolve["accessor_method"],
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
            # resolved with original user input python object.
            user_input_python_object = self._dag_input_node
            if isinstance(self._key, str):
                if self._accessor_method == "__getitem__":
                    return user_input_python_object[self._key]
                elif self._accessor_method == "__getattr__":
                    return getattr(user_input_python_object, self._key)
            elif isinstance(self._key, int):
                return user_input_python_object[self._key]
            else:
                raise ValueError(
                    "Please only use int index or str as first-level key to "
                    "access fields of dag input."
                )

    def __str__(self) -> str:
        return get_dag_node_str(self, f"[\"{self._key}\"]")
