from abc import ABC
from typing import Any, Callable, Optional, Tuple


class CallableNode(ABC):
    def call(self, user_args: Tuple[Any]) -> Any:
        pass

    async def call_async(self, user_args: Tuple[Any]) -> Any:
        pass


class InlineCallableNode(CallableNode):
    def __init__(self, _callable: Callable,
                 incoming_nodes: Tuple["CallableNode"]):
        self._callable = _callable
        self._incoming_nodes = incoming_nodes

    def call(self, input_arg: Tuple[Any]) -> Any:
        if len(self._incoming_nodes):
            args = tuple(node.call(input_arg) for node in self._incoming_nodes)
        else:
            # Handle the INPUT case.
            args = (input_arg, )

        return self._callable(*args)

    async def call_async(self):
        pass


class PipelineNode:
    def __init__(self,
                 callable_factory: Callable[[], Callable],
                 incoming_edges: Optional[Tuple["PipelineNode"]] = None):
        self._callable_factory = callable_factory
        self._incoming_edges: Tuple["PipelineNode"] = incoming_edges or tuple()

    def deploy(self):
        incoming_nodes = tuple(node.deploy() for node in self._incoming_edges)
        return InlineCallableNode(self._callable_factory(), incoming_nodes)


def noop(input_arg: Tuple[Any]) -> Any:
    return input_arg


INPUT = PipelineNode(lambda: noop)
