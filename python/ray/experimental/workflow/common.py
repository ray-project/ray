from typing import Tuple, List, Optional, Callable
import uuid

import ray

# Alias types
RRef = ray.ObjectRef  # Alias ObjectRef because it is too long in type hints.
StepID = str
WorkflowOutputType = RRef
WorkflowInputTuple = Tuple[RRef, List["Workflow"], List[RRef]]
StepExecutionFunction = Callable[[StepID, WorkflowInputTuple],
                                 WorkflowOutputType]
SerializedStepFunction = str


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
        self._step_id: StepID = uuid.uuid4().hex

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

    def execute(self) -> RRef:
        """
        Trigger workflow execution recursively.
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
        output = self._step_execution_function(self._step_id, step_inputs)
        if not isinstance(output, WorkflowOutputType):
            raise TypeError("Unexpected return type of the workflow.")
        self._output = output
        self._executed = True
        return output

    def __reduce__(self):
        raise ValueError(
            "Workflow is not supposed to be serialized by pickle. "
            "Maybe you are passing it to a Ray remote function, "
            "returning it from a Ray remote function, or using "
            "'ray.put()' with it?")
