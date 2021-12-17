from abc import ABC
from typing import Any, Callable, Tuple

import ray
from ray import cloudpickle, ObjectRef

from ray.serve.pipeline.common import StepConfig
from ray.serve.pipeline.executor import (create_executor_from_step_config,
                                         Executor)


class PipelineNode(ABC):
    def instantiate(self):
        pass

    # NOTE(simon): used _call as name so user don't confuse deployed and
    # un-deployed pipeline objects.
    def _call(self, input_arg: Tuple[Any]) -> Any:
        pass


class InstantiatedPipeline:
    """An instantiateed pipeline that can be called by the user."""

    def __init__(self, output_node: PipelineNode):
        self._output_node = output_node

    def call(self, input_arg: Tuple[Any]) -> Any:
        result = self._output_node._call(input_arg)
        if isinstance(result, ObjectRef):
            result = ray.get(result)

        return result

    async def call_async(self, input_arg: Tuple[Any]) -> Any:
        raise NotImplementedError("No async support yet.")

    def __repr__(self) -> str:
        return f"[InstantiatedPipeline: output_node {str(self._output_node)}]"


class ExecutorPipelineNode(PipelineNode):
    """Result of constructing a pipeline from user-defined steps.

    Call .instantiate() on this to instantiate the pipeline.
    """

    def __init__(self, callable_factory: Callable[[], Callable],
                class_name: str, config: StepConfig, incoming_edges: Tuple[PipelineNode]):
        # Serialize to make this class environment-independent.
        self._serialized_callable_factory: bytes = cloudpickle.dumps(
            callable_factory)
        self._class_name = class_name
        self._config: StepConfig = config
        self._incoming_edges: PipelineNode = incoming_edges

        # Populated in .instantiate().
        self._executor: Executor = None

        assert len(self._incoming_edges) > 0

    def instantiate(self) -> InstantiatedPipeline:
        """Instantiates executors for this and all its upstream dependent nodes.

        After the pipeline is instantiated, .call() and .call_async() can be used.
        """
        [node.instantiate() for node in self._incoming_edges]
        self._executor = create_executor_from_step_config(
            self._serialized_callable_factory, self._config)

        return InstantiatedPipeline(self)

    def _call(self, input_arg: Tuple[Any]) -> Any:
        if self._executor is None:
            raise RuntimeError(
                "Pipeline hasn't been deployed, call .deploy() first.")
        args = tuple(node._call(input_arg) for node in self._incoming_edges)
        return self._executor.call(*args)

    def __repr__(self) -> str:
        return f"[ExecutorPipelineNode: {self._class_name}]"


class InputPipelineNode(PipelineNode):
    def instantiate(self) -> PipelineNode:
        pass

    def _call(self, input_arg: Tuple[Any]) -> Any:
        return input_arg

    def __repr__(self):
        return "INPUT_NODE"


# Special node that's used to designate the input of a pipeline.
INPUT = InputPipelineNode()
