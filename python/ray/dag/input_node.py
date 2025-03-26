from typing import Any, Dict, List, Union, Optional

from ray.dag import DAGNode
from ray.dag.format_utils import get_dag_node_str
from ray.experimental.gradio_utils import type_to_string
from ray.util.annotations import DeveloperAPI

IN_CONTEXT_MANAGER = "__in_context_manager__"


@DeveloperAPI
class InputNode(DAGNode):
    r"""Ray dag node used in DAG building API to mark entrypoints of a DAG.

    Should only be function or class method. A DAG can have multiple
    entrypoints, but only one instance of InputNode exists per DAG, shared
    among all DAGNodes.

    Example:

    .. code-block::

                m1.forward
                /       \
        dag_input     ensemble -> dag_output
                \       /
                m2.forward

    In this pipeline, each user input is broadcasted to both m1.forward and
    m2.forward as first stop of the DAG, and authored like

    .. code-block:: python

        import ray

        @ray.remote
        class Model:
            def __init__(self, val):
                self.val = val
            def forward(self, input):
                return self.val * input

        @ray.remote
        def combine(a, b):
            return a + b

        with InputNode() as dag_input:
            m1 = Model.bind(1)
            m2 = Model.bind(2)
            m1_output = m1.forward.bind(dag_input[0])
            m2_output = m2.forward.bind(dag_input.x)
            ray_dag = combine.bind(m1_output, m2_output)

        # Pass mix of args and kwargs as input.
        ray_dag.execute(1, x=2) # 1 sent to m1, 2 sent to m2

        # Alternatively user can also pass single data object, list or dict
        # and access them via list index, object attribute or dict key str.
        ray_dag.execute(UserDataObject(m1=1, m2=2))
        # dag_input.m1, dag_input.m2
        ray_dag.execute([1, 2])
        # dag_input[0], dag_input[1]
        ray_dag.execute({"m1": 1, "m2": 2})
        # dag_input["m1"], dag_input["m2"]
    """

    def __init__(
        self,
        *args,
        input_type: Optional[Union[type, Dict[Union[int, str], type]]] = None,
        _other_args_to_resolve=None,
        **kwargs,
    ):
        """InputNode should only take attributes of validating and converting
        input data rather than the input data itself. User input should be
        provided via `ray_dag.execute(user_input)`.

        Args:
            input_type: Describes the data type of inputs user will be giving.
                - if given through singular InputNode: type of InputNode
                - if given through InputAttributeNodes: map of key -> type
                Used when deciding what Gradio block to represent the input nodes with.
            _other_args_to_resolve: Internal only to keep InputNode's execution
                context throughput pickling, replacement and serialization.
                User should not use or pass this field.
        """
        if len(args) != 0 or len(kwargs) != 0:
            raise ValueError("InputNode should not take any args or kwargs.")

        self.input_attribute_nodes = {}

        self.input_type = input_type
        if input_type is not None and isinstance(input_type, type):
            if _other_args_to_resolve is None:
                _other_args_to_resolve = {}
            _other_args_to_resolve["result_type_string"] = type_to_string(input_type)

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

    def set_context(self, key: str, val: Any):
        """Set field in parent DAGNode attribute that can be resolved in both
        pickle and JSON serialization
        """
        self._bound_other_args_to_resolve[key] = val

    def __str__(self) -> str:
        return get_dag_node_str(self, "__InputNode__")

    def __getattr__(self, key: str):
        assert isinstance(
            key, str
        ), "Please only access dag input attributes with str key."
        if key not in self.input_attribute_nodes:
            self.input_attribute_nodes[key] = InputAttributeNode(
                self, key, "__getattr__"
            )
        return self.input_attribute_nodes[key]

    def __getitem__(self, key: Union[int, str]) -> Any:
        assert isinstance(key, (str, int)), (
            "Please only use int index or str as first-level key to "
            "access fields of dag input."
        )

        input_type = None
        if self.input_type is not None and key in self.input_type:
            input_type = type_to_string(self.input_type[key])

        if key not in self.input_attribute_nodes:
            self.input_attribute_nodes[key] = InputAttributeNode(
                self, key, "__getitem__", input_type
            )
        return self.input_attribute_nodes[key]

    def __enter__(self):
        self.set_context(IN_CONTEXT_MANAGER, True)
        return self

    def __exit__(self, *args):
        pass

    def get_result_type(self) -> str:
        """Get type of the output of this DAGNode.

        Generated by ray.experimental.gradio_utils.type_to_string().
        """
        if "result_type_string" in self._bound_other_args_to_resolve:
            return self._bound_other_args_to_resolve["result_type_string"]


@DeveloperAPI
class InputAttributeNode(DAGNode):
    """Represents partial access of user input based on an index (int),
     object attribute or dict key (str).

    Examples:

        .. code-block:: python

            with InputNode() as dag_input:
                a = dag_input[0]
                b = dag_input.x
                ray_dag = add.bind(a, b)

            # This makes a = 1 and b = 2
            ray_dag.execute(1, x=2)

            with InputNode() as dag_input:
                a = dag_input[0]
                b = dag_input[1]
                ray_dag = add.bind(a, b)

            # This makes a = 2 and b = 3
            ray_dag.execute(2, 3)

            # Alternatively, you can input a single object
            # and the inputs are automatically indexed from the object:
            # This makes a = 2 and b = 3
            ray_dag.execute([2, 3])
    """

    def __init__(
        self,
        dag_input_node: InputNode,
        key: Union[int, str],
        accessor_method: str,
        input_type: str = None,
    ):
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
                # Type of the input tied to this node. Used by
                # gradio_visualize_graph.GraphVisualizer to determine which Gradio
                # component should be used for this node.
                "result_type_string": input_type,
            },
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return InputAttributeNode(
            new_other_args_to_resolve["dag_input_node"],
            new_other_args_to_resolve["key"],
            new_other_args_to_resolve["accessor_method"],
            new_other_args_to_resolve["result_type_string"],
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of InputAttributeNode.

        Args and kwargs are to match base class signature, but not in the
        implementation. All args and kwargs should be resolved and replaced
        with value in bound_args and bound_kwargs via bottom-up recursion when
        current node is executed.
        """

        if isinstance(self._dag_input_node, DAGInputData):
            return self._dag_input_node[self._key]
        else:
            # dag.execute() is called with only one arg, thus when an
            # InputAttributeNode is executed, its dependent InputNode is
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
        return get_dag_node_str(self, f'["{self._key}"]')

    def get_result_type(self) -> str:
        """Get type of the output of this DAGNode.

        Generated by ray.experimental.gradio_utils.type_to_string().
        """
        if "result_type_string" in self._bound_other_args_to_resolve:
            return self._bound_other_args_to_resolve["result_type_string"]

    @property
    def key(self) -> Union[int, str]:
        return self._key


@DeveloperAPI
class DAGInputData:
    """If user passed multiple args and kwargs directly to dag.execute(), we
    generate this wrapper for all user inputs as one object, accessible via
    list index or object attribute key.
    """

    def __init__(self, *args, **kwargs):
        self._args = list(args)
        self._kwargs = kwargs

    def __getitem__(self, key: Union[int, str]) -> Any:
        if isinstance(key, int):
            # Access list args by index.
            return self._args[key]
        elif isinstance(key, str):
            # Access kwarg by key.
            return self._kwargs[key]
        else:
            raise ValueError(
                "Please only use int index or str as first-level key to "
                "access fields of dag input."
            )
