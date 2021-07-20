import functools
import inspect
from typing import Any, Dict, List, Tuple, Callable, Optional

import ray

from ray import ObjectRef
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.common import (
    Workflow, StepID, WorkflowOutputType, WorkflowInputTuple)

StepInputTupleToResolve = Tuple[ObjectRef, List[ObjectRef], List[ObjectRef]]


class WorkflowStepFunction:
    """This class represents a workflow step."""

    def __init__(self,
                 func: Callable,
                 step_max_retries=1,
                 catch_exceptions=False,
                 ray_options=None):
        if not isinstance(step_max_retries, int) or step_max_retries < 1:
            raise ValueError(
                "step_max_retries should be greater or equal to 1.")
        if ray_options is not None and not isinstance(ray_options, dict):
            raise ValueError("ray_options must be a dict.")

        self._func = func
        self._step_max_retries = step_max_retries
        self._catch_exceptions = catch_exceptions
        self._ray_options = ray_options or {}
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
            object_refs: List[ObjectRef] = []
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
                input_placeholder: ObjectRef = ray.put((args, kwargs))
                if object_refs:
                    raise ValueError(
                        "There are ObjectRefs in workflow inputs. However "
                        "workflow currently does not support checkpointing "
                        "ObjectRefs.")
            return Workflow(self._func, self._run_step, input_placeholder,
                            workflows, object_refs, self._step_max_retries,
                            self._catch_exceptions, self._ray_options)

        self.step = _build_workflow

    def _run_step(
            self,
            step_id: StepID,
            step_inputs: WorkflowInputTuple,
            catch_exceptions: bool,
            step_max_retries: int,
            ray_options: Dict[str, Any],
            outer_most_step_id: Optional[StepID] = None) -> WorkflowOutputType:
        from ray.experimental.workflow.step_executor import (
            execute_workflow_step)
        return execute_workflow_step(self._func, step_id, step_inputs,
                                     catch_exceptions, step_max_retries,
                                     ray_options, outer_most_step_id)

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow steps cannot be called directly. Instead "
                        f"of running '{self.step.__name__}()', "
                        f"try '{self.step.__name__}.step()'.")

    def options(self,
                *,
                step_max_retries: int = 1,
                catch_exceptions: bool = False,
                **ray_options) -> "WorkflowStepFunction":
        """This function set how the step function is going to be executed.

        Args:
            step_max_retries(int): num of retries the step for an application
                level error
            catch_exceptions(bool): Whether the user want to take care of the
                failure mannually.
                If it's set to be true, (Optional[R], Optional[E]) will be
                returned.
                If it's false, the normal result will be returned.
            **kwargs(dict): All parameters in this fields will be passed to
                ray remote function options.

        Returns:
            The step function itself.
        """
        return WorkflowStepFunction(self._func, step_max_retries,
                                    catch_exceptions, ray_options)
