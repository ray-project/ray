from typing import Any, Callable, Dict, List, Union, Optional

from ray.experimental.dag import InputNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY


class PipelineInputNode(InputNode):
    """Node used in DAG building API to mark entrypoints of a Serve Pipeline.
    The extension of Ray DAG level InputNode with additional abilities such
    as input conversion from HTTP.

    The execution of PipelineInputNode is the same as its parent InputNode,
    therefore no divergence from Ray DAG execution.

    Example:
        >>> # Provide your own async http to python data convert function
        >>> async def request_to_data_int(request: starlette.requests.Request):
        ...    data = await request.body()
        ...    return int(data)

        >>> # Change your Ray DAG InputNode to PipelineInputNode with
        >>> # preprocessor passed in
        >>> with PipelineInputNode(
        ...     preprocessor=request_to_data_int
        ... ) as dag_input:
        ...    model = Model.bind(2, ratio=0.3)
        ...    ray_dag = model.forward.bind(dag_input)
    """

    def __init__(
        self,
        preprocessor: Union[Callable, str] = "ray.serve.http_adapters.array_to_batch",
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
        # TODO (jiaodong, simonmo): Integrate with ModelWrapper
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

    def __str__(self) -> str:
        return get_dag_node_str(self, f"Preprocessor: {str(self._preprocessor)}")

    def get_preprocessor_import_path(self) -> Optional[str]:
        if isinstance(self._preprocessor, str):
            # We're processing a deserilized JSON node where preprocessor value
            # is the resolved import path.
            return self._preprocessor
        else:
            return f"{self._preprocessor.__module__}.{self._preprocessor.__qualname__}"

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls, PipelineInputNode.__name__)
        preprocessor_import_path = self.get_preprocessor_import_path()
        error_message = (
            "Preprocessor used in DAG should not be in-line defined when "
            "exporting import path for deployment. Please ensure it has fully "
            "qualified name with valid __module__ and __qualname__ for "
            "import path, with no __main__ or <locals>. \n"
            f"Current import path: {preprocessor_import_path}"
        )
        assert "__main__" not in preprocessor_import_path, error_message
        assert "<locals>" not in preprocessor_import_path, error_message

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
