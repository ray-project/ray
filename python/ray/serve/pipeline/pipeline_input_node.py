from typing import Any, Callable, Dict, List, Union, Optional

from ray.experimental.dag import InputNode, DAGInputData
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY


class PipelineInputNode(InputNode):
    """Node used in DAG building API to mark entrypoints of a Serve Pipeline.
    The extension of Ray DAG level InputNode with additional abilities such
    as input schema validation and input conversion for HTTP.

    # TODO (jiaodong): Add a concrete example here
    """

    def __init__(
        self,
        preprocessor: Union[Callable, str],
        _other_args_to_resolve=None,
    ):
        """InputNode should only take attributes of validating and converting
        input data rather than the input data itself. User input should be
        provided via `ray_dag.execute(user_input)`.

        Args:
            preprocessor: User function that handles input http conversion to
                python objects. Not on critical path of DAG execution, only used
                to pass its import path to generate Ingress deployment.

            _other_args_to_resolve: Internal only to keep InputNode's execution
                context throughput pickling, replacement and serialization.
                User should not use or pass this field.
        """
        self._preprocessor = preprocessor
        # Create InputNode instance
        super().__init__(_other_args_to_resolve=_other_args_to_resolve)
        # Need to set in other args to resolve in order to carry the context
        # throughout dag node replacement and json serde
        self._bound_other_args_to_resolve[
            "preprocessor_import_path"
        ] = self.get_preprocessor_import_path()

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return PipelineInputNode(
            self._preprocessor,
            _other_args_to_resolve=new_other_args_to_resolve,
        )

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

    def get_preprocessor_import_path(self) -> Optional[str]:
        if isinstance(self._preprocessor, str):
            # We're processing a deserilized JSON node where preprocessor value
            # is the resolved import path.
            return self._preprocessor
        else:
            return f"{self._preprocessor.__module__}.{self._preprocessor.__qualname__}"

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, PipelineInputNode.__name__)
        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        assert input_json[DAGNODE_TYPE_KEY] == PipelineInputNode.__name__
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        node = cls(
            args_dict["other_args_to_resolve"]["preprocessor_import_path"],
            _other_args_to_resolve=args_dict["other_args_to_resolve"],
        )
        node._stable_uuid = input_json["uuid"]
        return node
