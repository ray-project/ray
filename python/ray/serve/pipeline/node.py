from abc import ABC
from typing import Any, Callable, Tuple

import ray
from ray import cloudpickle
from ray.actor import ActorHandle

from ray.serve.pipeline.common import StepConfig


@ray.remote
class PipelineNodeActor:
    """Actor that executes the serialized code for a given step."""

    def __init__(self, serialize_callable_factory: bytes):
        self._callable = cloudpickle.loads(serialize_callable_factory)()

    def call(self, *args, **kwargs):
        """Call the underlying callable."""
        return self._callable(*args, **kwargs)

    def ready(self):
        """Used to wait for the actor to become available."""
        pass


class CallableNode(ABC):
    """Node in the pipeline DAG that can be executed sync or async.

    The node is responsible for recursively resolving all of its input args.
    """

    def call(self, user_args: Tuple[Any]) -> Any:
        pass

    async def call_async(self, user_args: Tuple[Any]) -> Any:
        pass


class ActorCallableNode(CallableNode):
    """CallableNode that wraps a Ray actor to execute the step code."""

    def __init__(self, serialized_callable_factory: bytes,
                 incoming_nodes: Tuple[CallableNode]):
        # NOTE(edoakes): in the future this should be replaced with an actor
        # pool instead of an individual actor.
        self._actor: ActorHandle = PipelineNodeActor.remote(
            serialized_callable_factory)
        # Wait for the actor to be ready.
        ray.get(self._actor.ready.remote())
        self._incoming_nodes: Tuple[CallableNode] = incoming_nodes
        assert len(self._incoming_nodes) > 0

    def call(self, input_arg: Tuple[Any]) -> Any:
        args = tuple(node.call(input_arg) for node in self._incoming_nodes)
        return ray.get(self._actor.call.remote(*args))

    async def call_async(self):
        raise NotImplementedError("No async support yet.")


class InlineCallableNode(CallableNode):
    """CallableNode that executes the step code directly in the process."""

    def __init__(self, serialized_callable_factory: bytes,
                 incoming_nodes: Tuple[CallableNode]):
        self._callable = cloudpickle.loads(serialized_callable_factory)()
        self._incoming_nodes = incoming_nodes
        assert len(self._incoming_nodes) > 0

    def call(self, input_arg: Tuple[Any]) -> Any:
        args = tuple(node.call(input_arg) for node in self._incoming_nodes)
        return self._callable(*args)

    async def call_async(self):
        raise NotImplementedError("No async support yet.")


class InputCallableNode(CallableNode):
    """Special CallableNode representing the INPUT step."""

    def call(self, input_arg: Tuple[Any]) -> Any:
        return input_arg

    async def call_async(self, input_arg: Tuple[Any]) -> Any:
        return input_arg


class PipelineNodeBase(ABC):
    def deploy(self) -> CallableNode:
        pass


class PipelineNode(PipelineNodeBase):
    """Result of constructing a pipeline from steps.

    Call .deploy() on this to instantiate the full pipeline and return a
    CallableNode that can be used to execute it.
    """

    def __init__(self, callable_factory: Callable[[], Callable],
                 config: StepConfig, incoming_edges: Tuple[PipelineNodeBase]):
        # Serialize to make this class environment-independent.
        self._serialized_callable_factory: bytes = cloudpickle.dumps(
            callable_factory)
        self._config = config
        self._incoming_edges: PipelineNodeBase = incoming_edges

        assert len(self._incoming_edges) > 0

    def deploy(self) -> CallableNode:
        """Recursively deploy all necessary nodes and return a CallableNode.

        The resulting CallableNode can be used to execute the pipeline.
        """
        incoming_nodes = tuple(node.deploy() for node in self._incoming_edges)

        callable_node = None
        if self._config.inline:
            assert len(incoming_nodes) > 0
            callable_node = InlineCallableNode(
                self._serialized_callable_factory, incoming_nodes)
        else:
            assert len(incoming_nodes) > 0
            callable_node = ActorCallableNode(
                self._serialized_callable_factory, incoming_nodes)

        return callable_node


class InputPipelineNode(PipelineNodeBase):
    def deploy(self):
        return InputCallableNode()


# Special node that's used to designate the input of a pipeline.
INPUT = InputPipelineNode()
