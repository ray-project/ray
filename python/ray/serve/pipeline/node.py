from typing import Any, Callable, Dict, Set, Tuple

from ray.serve.pipeline import INPUT

class PipelineNode:
    def __init__(self, *args: Tuple[Any], **kwargs: Dict[Any, Any]):
        if len(kwargs):
            raise NotImplementedError("No kwargs yet!")

        # TODO: do we need to copy.copy args?
        self._args: Tuple[Any] = args
        self._incoming_edges: Set[PipelineNode] = set()
        self._outgoing_edges: Set[PipelineNode] = set()
        self._is_input_node: bool = False

        # Check arguments for incoming edges.
        for arg in args:
            if isinstance(arg, PipelineStep):
                raise TypeError("PipelineSteps cannot be passed in directly, "
                        "you need to call them with an input first. For "
                        "example: instead of `my_step_2(my_step_1)`, try "
                        "`my_step_2(my_step_1(pipeline.INPUT))`.")

            elif isinstance(arg, PipelineNode):
                self._incoming_edges.add(arg)
                arg.add_outgoing_edge(self)

            elif arg is INPUT:
                self._is_input_node = True


    def add_outgoing_edge(self, other: PipelineNode):
        self._outgoing_edges.add(other)
