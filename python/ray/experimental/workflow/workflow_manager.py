import functools
import inspect
from typing import List, Tuple, Callable, Optional

import ray

from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.common import (
    RRef, Workflow, StepID, WorkflowOutputType, WorkflowInputTuple)
from ray.experimental.workflow.step_executor import execute_workflow_step

StepInputTupleToResolve = Tuple[RRef, List[RRef], List[RRef]]


class WorkflowStepFunction:
    """This class represents a workflow step."""

    def __init__(self, func: Callable):
        self._func = func
        self._func_signature = list(
            inspect.signature(func).parameters.values())

        # Override signature and docstring
        @functools.wraps(func)
        def _build_workflow(*args, **kwargs) -> Workflow:
            # validate if the input arguments match the signature of the
            # original function.
            reconstructed_signature = inspect.Signature(
                parameters=self._func_signature)
            try:
                reconstructed_signature.bind(*args, **kwargs)
            except TypeError as exc:  # capture a friendlier stacktrace
                raise TypeError(str(exc)) from None
            workflows: List[Workflow] = []
            object_refs: List[RRef] = []
            with serialization_context.workflow_args_serialization_context(
                    workflows, object_refs):
                # NOTE: When calling 'ray.put', we trigger python object
                # serialization. Under our serialization context,
                # Workflows and ObjectRefs are separated from the arguments,
                # leaving a placeholder object with all other python objects.
                # Then we put the placeholder object to object store,
                # so it won't be mutated later. This guarantees correct
                # semantics. See "tests/test_variable_mutable.py" as
                # an example.
                input_placeholder: RRef = ray.put((args, kwargs))
            return Workflow(self._func, self._run_step, input_placeholder,
                            workflows, object_refs)

        self.step = _build_workflow

    def _run_step(
            self,
            step_id: StepID,
            step_inputs: WorkflowInputTuple,
            outer_most_step_id: Optional[StepID] = None) -> WorkflowOutputType:
        return execute_workflow_step(self._func, step_id, step_inputs,
                                     outer_most_step_id)

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow steps cannot be called directly. Instead "
                        f"of running '{self.step.__name__}()', "
                        f"try '{self.step.__name__}.step()'.")
