import ray

from typing import List, Tuple, Union, Any, Dict, Callable, Optional
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.common import (
    RRef, Workflow, StepID, WorkflowOutputType, WorkflowInputTuple)
from ray.experimental.workflow import workflow_storage

StepInputTupleToResolve = Tuple[RRef, List[RRef], List[RRef]]


def _resolve_object_ref(ref: RRef) -> Tuple[Any, RRef]:
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


def _resolve_step_inputs(
        step_inputs: StepInputTupleToResolve) -> Tuple[List, Dict]:
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
        Instances of arguments.
    """

    objects_mapping = []
    input_placeholder, input_workflows, input_object_refs = step_inputs
    for rref in input_workflows:
        obj, ref = _resolve_object_ref(rref)
        objects_mapping.append(obj)
    with serialization_context.workflow_args_resolving_context(
            objects_mapping, input_object_refs):
        # reconstruct input arguments under correct serialization context
        args, kwargs = ray.get(input_placeholder)
    _args, _kwargs = _deref_arguments(args, kwargs)
    return _args, _kwargs


def postprocess_workflow_step(ret: Union[Workflow, Any],
                              outer_most_step_id: Optional[StepID] = None):
    """Execute workflow and checkpoint outputs.

    To fully explain what we are doing, we need to introduce some syntax first.
    The syntax for dependencies between workflow steps
    "A.step(B.step())" is "A - B"; the syntax for nested workflow steps
    "def A(): return B.step()" is "A / B".

    In a chain/DAG of step dependencies, the "output step" is the step of last
    (topological) order. For example, in "A - B - C", C is the output step.

    In a chain of nested workflow steps, the initial "output step" is
    called the "outer most step" for other "output steps". For example, in
    "A / B / C / D", "A" is the outer most step for "B", "C", "D";
    in the hybrid workflow "((A - B) / C / D) - (E / (F - G) / H)",
    "B" is the outer most step for "C", "D"; "E" is the outer most step
    for "G", "H".

    Args:
        ret: The returned object of the workflow step.
        outer_most_step_id: The ID of the outer most workflow. None if it
            does not exists.
    """
    store = workflow_storage.WorkflowStorage()
    step_id = workflow_context.get_current_step_id()
    store.commit_step(step_id, ret, outer_most_step_id)
    if isinstance(ret, Workflow):
        if outer_most_step_id is None:
            # The current workflow step returns a nested workflow, and
            # there is no outer step for the current step. So the current
            # step is the outer most step for the inner nested workflow
            # steps.
            outer_most_step_id = step_id
        # Passing down outer most step so inner nested steps would
        # access the same outer most step.
        return ret.execute(outer_most_step_id)
    return ret


@ray.remote
def _workflow_step_executor(
        func: Callable, context: workflow_context.WorkflowStepContext,
        step_id: StepID, step_inputs: StepInputTupleToResolve,
        outer_most_step_id: StepID) -> Any:
    """Executor function for workflow step.

    Args:
        func: The workflow step function.
        context: Workflow step context. Used to access correct storage etc.
        step_id: The ID of the step.
        step_inputs: The inputs tuple of the step.
        outer_most_step_id: See "postprocess_workflow_step" for
            explanation.

    Returns:
        Workflow step output.
    """
    # Before running the actual function, we
    # 1. Setup the workflow context, so we have proper access to
    #    workflow storage.
    # 2. Decode step inputs to arguments and keyword-arguments.
    workflow_context.update_workflow_step_context(context, step_id)
    args, kwargs = _resolve_step_inputs(step_inputs)
    # Running the actual step function
    ret = func(*args, **kwargs)
    # See "postprocess_workflow_step" for explanation of "outer_most_step_id".
    return postprocess_workflow_step(ret, outer_most_step_id)


def execute_workflow_step(step_func: Callable, step_id: StepID,
                          step_inputs: WorkflowInputTuple,
                          outer_most_step_id: StepID) -> WorkflowOutputType:
    return _workflow_step_executor.remote(
        step_func, workflow_context.get_workflow_step_context(), step_id,
        step_inputs, outer_most_step_id)
