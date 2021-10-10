from abc import ABC
from typing import Any, Callable, Optional, Tuple

import ray
from ray import cloudpickle

from ray.serve.pipeline.common import StepConfig


@ray.remote
class PipelineNodeActor:
    def __init__(self, serialize_callable_factory: bytes):
        self._callable = cloudpickle.loads(serialize_callable_factory)()

    def call(self, *args, **kwargs):
        return self._callable(*args, **kwargs)


class CallableNode(ABC):
    def call(self, user_args: Tuple[Any]) -> Any:
        pass

    async def call_async(self, user_args: Tuple[Any]) -> Any:
        pass


class ActorCallableNode(CallableNode):
    def __init__(self, serialized_callable_factory: bytes,
                 incoming_nodes: Tuple["CallableNode"]):
        self._actor = PipelineNodeActor.remote(serialized_callable_factory)
        self._incoming_nodes = incoming_nodes
        assert len(self._incoming_nodes) > 0

    def call(self, input_arg: Tuple[Any]) -> Any:
        args = tuple(node.call(input_arg) for node in self._incoming_nodes)
        return ray.get(self._actor.remote(*args))

    async def call_async(self):
        raise NotImplementedError("No async support yet.")


class InlineCallableNode(CallableNode):
    def __init__(self, serialized_callable_factory: bytes,
                 incoming_nodes: Tuple["CallableNode"]):
        self._callable = cloudpickle.loads(serialized_callable_factory)()
        self._incoming_nodes = incoming_nodes
        assert len(self._incoming_nodes) > 0

    def call(self, input_arg: Tuple[Any]) -> Any:
        args = tuple(node.call(input_arg) for node in self._incoming_nodes)
        return self._callable(*args)

    async def call_async(self):
        raise NotImplementedError("No async support yet.")


class InputCallableNode(CallableNode):
    def call(self, input_arg: Tuple[Any]) -> Any:
        return input_arg

    async def call_async(self, input_arg: Tuple[Any]) -> Any:
        return input_arg


class CallableNodeFactory:
    def __init__(self,
                 callable_factory: Callable[[], Callable],
                 config: StepConfig,
                 is_input_step: bool = False):
        # Serialize so this factory is environment-independent.
        self._serialized_callable_factory = cloudpickle.dumps(callable_factory)
        self._config = config
        self._is_input_step = is_input_step

    def __call__(self, incoming_nodes: Tuple["CallableNode"]) -> CallableNode:
        if self._is_input_step:
            assert len(incoming_nodes) == 0
            return InputCallableNode()
        elif self._config.inline:
            assert len(incoming_nodes) > 0
            return InlineCallableNode(self._serialized_callable_factory,
                                      incoming_nodes)
        else:
            assert len(incoming_nodes) > 0
            return ActorCallableNode(self._serialized_callable_factory,
                                     incoming_nodes)


class PipelineNode:
    def __init__(self,
                 callable_node_factory: CallableNodeFactory,
                 incoming_edges: Optional[Tuple["PipelineNode"]] = None):
        self._callable_node_factory = callable_node_factory
        self._incoming_edges: Tuple["PipelineNode"] = incoming_edges or tuple()

    def deploy(self) -> CallableNode:
        incoming_nodes = tuple(node.deploy() for node in self._incoming_edges)
        return self._callable_node_factory(incoming_nodes)


INPUT = PipelineNode(CallableNodeFactory(None, None, is_input_step=True))
