import functools
import inspect
from typing import List, Tuple, Union, Any, Dict, Callable, Optional

import ray
import ray.cloudpickle

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.common import (
    RRef, Workflow, StepID, WorkflowOutputType, WorkflowInputTuple)
from ray.experimental.workflow import storage


def resolve_object_ref(ref: RRef) -> Tuple[Any, RRef]:
    """
    Resolves the ObjectRef into the object instance.

    Returns:
        The object instance and the direct ObjectRef to the instance.
    """
    assert ray.is_initialized()
    last_ref = ref
    while True:
        if isinstance(ref, RRef):
            last_ref = ref
        else:
            break
        ref = ray.get(last_ref)
    return ref, last_ref


def _deref_arguments(args: List, kwargs: Dict) -> Tuple[List, Dict]:
    """
    This function decides how the ObjectRefs in the argument will be presented
    to the user. Currently we dereference arguments like Ray remote functions.

    Args:
        args: Positional arguments.
        kwargs: Keywords arguments.

    Returns:
        Post processed arguments.
    """
    _args = [ray.get(a) if isinstance(a, RRef) else a for a in args]
    _kwargs = {
        k: ray.get(v) if isinstance(v, RRef) else v
        for k, v in kwargs.items()
    }
    return _args, _kwargs


def _resolve_step_inputs(step_inputs: Tuple[RRef, List[RRef], List[RRef]]
                         ) -> Tuple[List, Dict, List[RRef]]:
    """
    This function resolves the inputs for the code inside
    a workflow step (works on the callee side). For outputs from other
    workflows, we resolve them into object instances inplace.

    For each ObjectRef argument, the function returns both the ObjectRef
    and the object instance. If the ObjectRef is a chain of nested
    ObjectRefs, then we resolve it recursively until we get the
    object instance, and we return the *direct* ObjectRef of the
    instance. This function does not resolve ObjectRef
    inside another object (e.g. list of ObjectRefs) to give users some
    flexibility.

    Args:
        step_inputs: Workflow step inputs.
    Returns:
        Instances of arguments and resolved object refs.
    """

    resolved_object_refs = []
    objects_mapping = []
    input_placeholder, input_workflows, input_object_refs = step_inputs
    for rref in input_workflows:
        obj, ref = resolve_object_ref(rref)
        objects_mapping.append(obj)
        resolved_object_refs.append(ref)
    with serialization_context.workflow_args_resolving_context(
            objects_mapping, input_object_refs):
        # reconstruct input arguments under correct serialization context
        args, kwargs = ray.get(input_placeholder)
    _args, _kwargs = _deref_arguments(args, kwargs)
    return _args, _kwargs, resolved_object_refs


class WorkflowStepFunction:
    def __init__(self, func: Callable):
        def _func(context, task_id, step_inputs, forward_output_to):
            # NOTE: must use 'set_current_store_dir' to ensure that we are
            # accessing the correct global variable.
            workflow_context.update_workflow_step_context(context, task_id)
            args, kwargs, resolved_object_refs = _resolve_step_inputs(
                step_inputs)
            # free references to potentially save memory
            del resolved_object_refs

            _output = func(*args, **kwargs)
            output = _commit_workflow(_output, forward_output_to)
            return output

        self._func = func
        self._remote_function = ray.remote(_func)
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
            forward_output_to: Optional[StepID] = None) -> WorkflowOutputType:
        ref = self._remote_function.remote(
            workflow_context.get_workflow_step_context(), step_id, step_inputs,
            forward_output_to)
        return ref

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow steps cannot be called directly. Instead "
                        f"of running '{self.step.__name__}()', "
                        f"try '{self.step.__name__}.step()'.")


def _commit_workflow(output: Union[Workflow, Any],
                     forward_output_to: Optional[StepID] = None):
    """Execute workflow and checkpoint outputs.

    Args:
        output: The output of the workflow step.
        forward_output_to: The output should also forward to the step
            referred by 'forward_output_to'. When resume from that step,
            that step can directly read this output.
    """
    if isinstance(output, Workflow):
        storage.save_workflow_dag(output, forward_output_to)
        if forward_output_to is None:
            # The current workflow step returns a nested workflow, but there is
            # no target to forward the nested workflow to. This means
            # the current step is the target. The target also includes
            # the workflow job driver, so our workflow entrypoint also
            # get updated.
            forward_output_to = workflow_context.get_current_step_id()
        # Passing down "forward_output_to" so deeper nested steps would
        # forward their results to the same "outer most" step.
        output = output.execute(forward_output_to)
    else:
        storage.save_workflow_output(output, forward_output_to)
    return output
