from collections import deque
import re
from typing import Tuple, List, Optional, Callable, Set, Iterator
import unicodedata
import uuid

from dataclasses import dataclass

import ray

# Alias types
RRef = ray.ObjectRef  # Alias ObjectRef because it is too long in type hints.
StepID = str
WorkflowOutputType = RRef
WorkflowInputTuple = Tuple[RRef, List["Workflow"], List[RRef]]
StepExecutionFunction = Callable[
    [StepID, WorkflowInputTuple, Optional[StepID]], WorkflowOutputType]
SerializedStepFunction = str


@dataclass
class WorkflowInputs:
    # The workflow step function body.
    func_body: Callable
    # The object ref of the input arguments.
    args: RRef
    # The hex string of object refs in the arguments.
    object_refs: List[str]
    # The ID of workflows in the arguments.
    workflows: List[str]


def slugify(value: str, allow_unicode=False):
    """Adopted from
    https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, dots or hyphens. Also strip leading and
    trailing whitespace.
    """
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = unicodedata.normalize("NFKD", value).encode(
            "ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w.\-]", "", value).strip()
    return re.sub(r"[-\s]+", "-", value)


class Workflow:
    def __init__(self, original_function: Callable,
                 step_execution_function: StepExecutionFunction,
                 input_placeholder: RRef, input_workflows: List["Workflow"],
                 input_object_refs: List[RRef]):
        self._input_placeholder: RRef = input_placeholder
        self._input_workflows: List[Workflow] = input_workflows
        self._input_object_refs: List[RRef] = input_object_refs
        # we need the original function for checkpointing
        self._original_function: Callable = original_function
        self._step_execution_function: StepExecutionFunction = (
            step_execution_function)

        self._executed: bool = False
        self._output: Optional[WorkflowOutputType] = None
        self._step_id: StepID = slugify(
            original_function.__qualname__) + "." + uuid.uuid4().hex
        # When we resuming the workflow, we do not want to override the DAG
        # of the original workflow. This tag helps skip it.
        self.skip_saving_workflow_dag: bool = False

    @property
    def executed(self) -> bool:
        return self._executed

    @property
    def output(self) -> WorkflowOutputType:
        if not self._executed:
            raise Exception("The workflow has not been executed.")
        return self._output

    @property
    def id(self) -> StepID:
        return self._step_id

    def execute(self, outer_most_step_id: Optional[StepID] = None) -> RRef:
        """Trigger workflow execution recursively.

        Args:
            outer_most_step_id: See
                "workflow_manager.postprocess_workflow_step" for explanation.
        """
        if self.executed:
            return self._output
        workflow_outputs = [w.execute() for w in self._input_workflows]
        # NOTE: Input placeholder is only a placeholder. It only can be
        # deserialized under a proper serialization context. Directly
        # deserialize the placeholder without a context would raise
        # an exception. If we pass the placeholder to _step_execution_function
        # as a direct argument, it would be deserialized by Ray without a
        # proper context. To prevent it, we put it inside a tuple.
        step_inputs = (self._input_placeholder, workflow_outputs,
                       self._input_object_refs)
        output = self._step_execution_function(self._step_id, step_inputs,
                                               outer_most_step_id)
        if not isinstance(output, WorkflowOutputType):
            raise TypeError("Unexpected return type of the workflow.")
        self._output = output
        self._executed = True
        return output

    def iter_workflows_in_dag(self) -> Iterator["Workflow"]:
        """Collect all workflows in the DAG linked to the workflow
        using BFS."""
        # deque is used instead of queue.Queue because queue.Queue is aimed
        # at multi-threading. We just need a pure data structure here.
        visited_workflows: Set[Workflow] = {self}
        q = deque([self])
        while q:  # deque's pythonic way to check emptyness
            w: Workflow = q.popleft()
            for p in w._input_workflows:
                if p not in visited_workflows:
                    visited_workflows.add(p)
                    q.append(p)
            yield w

    def get_inputs(self) -> WorkflowInputs:
        """Get the inputs of the workflow."""
        return WorkflowInputs(
            func_body=self._original_function,
            args=self._input_placeholder,
            object_refs=[r.hex() for r in self._input_object_refs],
            workflows=[w.id for w in self._input_workflows],
        )

    def __reduce__(self):
        raise ValueError(
            "Workflow is not supposed to be serialized by pickle. "
            "Maybe you are passing it to a Ray remote function, "
            "returning it from a Ray remote function, or using "
            "'ray.put()' with it?")
