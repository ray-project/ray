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

    # NOTE(simon): used _call as name so user don't confuse instantiateed and
    # un-instantiateed pipeline objects.
    def _call(self, input_arg: Tuple[Any]) -> Any:
        pass


class Pipeline:
    """A instantiateed pipeline that can be called by the user."""

    def __init__(self, entry_node: PipelineNode):
        self._entry_node = entry_node

    def call(self, input_arg: Tuple[Any]) -> Any:
        result = self._entry_node._call(input_arg)
        if isinstance(result, ObjectRef):
            result = ray.get(result)

        return result

    async def call_async(self, input_arg: Tuple[Any]) -> Any:
        raise NotImplementedError("No async support yet.")


class ExecutorPipelineNode(PipelineNode):
    """Result of constructing a pipeline from user-defined steps.

    Call .instantiate() on this to instantiate the pipeline.
    """

    def __init__(self, callable_factory: Callable[[], Callable],
                 config: StepConfig, incoming_edges: Tuple[PipelineNode]):
        # Serialize to make this class environment-independent.
        self._serialized_callable_factory: bytes = cloudpickle.dumps(
            callable_factory)
        self._config: StepConfig = config
        self._incoming_edges: PipelineNode = incoming_edges

        # Populated in .instantiate().
        self._executor: Executor = None

        assert len(self._incoming_edges) > 0

    def instantiate(self) -> Pipeline:
        """Instantiates executors for this and all dependent nodes.

        After the pipeline is instantiateed, .call() and .call_async() can be used.
        """
        [node.instantiate() for node in self._incoming_edges]
        self._executor = create_executor_from_step_config(
            self._serialized_callable_factory, self._config)

        return Pipeline(self)

    def _call(self, input_arg: Tuple[Any]) -> Any:
        if self._executor is None:
            raise RuntimeError(
                "Pipeline hasn't been instantiateed, call .instantiate() first.")
        args = tuple(node._call(input_arg) for node in self._incoming_edges)
        return self._executor.call(*args)


class InputPipelineNode(PipelineNode):
    def instantiate(self) -> PipelineNode:
        pass

    def _call(self, input_arg: Tuple[Any]) -> Any:
        return input_arg


# Special node that's used to designate the input of a pipeline.
INPUT = InputPipelineNode()
