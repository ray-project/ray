from typing import (List, Tuple, Any, Dict, Callable, Optional, TYPE_CHECKING,
                    Union)

import ray
from ray import ObjectRef
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow import workflow_storage

if TYPE_CHECKING:
    from ray.experimental.workflow.common import (StepID, WorkflowOutputType,
                                                  WorkflowInputTuple)

StepInputTupleToResolve = Tuple[ObjectRef, List[ObjectRef], List[ObjectRef]]


def _resolve_object_ref(ref: ObjectRef) -> Tuple[Any, ObjectRef]:
    """
    Resolves the ObjectRef into the object instance.

    Returns:
        The object instance and the direct ObjectRef to the instance.
    """
    assert ray.is_initialized()
    last_ref = ref
    while True:
        if isinstance(ref, ObjectRef):
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
    _args = [ray.get(a) if isinstance(a, ObjectRef) else a for a in args]
    _kwargs = {
        k: ray.get(v) if isinstance(v, ObjectRef) else v
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
    for obj_ref in input_workflows:
        obj, ref = _resolve_object_ref(obj_ref)
        objects_mapping.append(obj)
    with serialization_context.workflow_args_resolving_context(
            objects_mapping, input_object_refs):
        # reconstruct input arguments under correct serialization context
        args, kwargs = ray.get(input_placeholder)
    _args, _kwargs = _deref_arguments(args, kwargs)
    return _args, _kwargs


def execute_workflow(workflow: Workflow,
                     outer_most_step_id: Optional[str] = None
                     ) -> ray.ObjectRef:
    """Execute workflow.

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
        workflow: The workflow to be executed.
        outer_most_step_id: The ID of the outer most workflow. None if it
            does not exists. See "step_executor.execute_workflow" for detailed
            explanation.
    Returns:
        An object ref that represent the result.
    """
    if outer_most_step_id is None:
        # The current workflow step returns a nested workflow, and
        # there is no outer step for the current step. So the current
        # step is the outer most step for the inner nested workflow
        # steps.
        outer_most_step_id = workflow_context.get_current_step_id()
    # Passing down outer most step so inner nested steps would
    # access the same outer most step.
    return workflow.execute(outer_most_step_id)


def commit_step(store: workflow_storage.WorkflowStorage,
                step_id: "StepID",
                ret: Union[Workflow, Any],
                outer_most_step_id: Optional[str] = None):
    """Checkpoint the step output.
    Args:
        store: The storage the current workflow is using.
        step_id: The ID of the step.
        ret: The returned object of the workflow step.
        outer_most_step_id: The ID of the outer most workflow. None if it
            does not exists. See "step_executor.execute_workflow" for detailed
            explanation.
    """
    if isinstance(ret, Workflow):
        store.save_subworkflow(ret)
    store.save_step_output(step_id, ret, outer_most_step_id)


@ray.remote
def _workflow_step_executor(
        func: Callable, context: workflow_context.WorkflowStepContext,
        step_id: "StepID", step_inputs: "StepInputTupleToResolve",
        outer_most_step_id: "StepID", catch_exceptions: bool,
        step_max_retries: int) -> Any:
    """Executor function for workflow step.

    Args:
        func: The workflow step function.
        context: Workflow step context. Used to access correct storage etc.
        step_id: The ID of the step.
        step_inputs: The inputs tuple of the step.
        outer_most_step_id: See "step_executor.execute_workflow" for
            explanation.
        catch_exceptions: If set to be true, return
            (Optional[Result], Optional[Error]) instead of Result.
        step_max_retries: Max number of retries encounter of a failure.

    Returns:
        Workflow step output.
    """
    ret = None
    err = None
    # step_max_retries are for application level failure.
    # For ray failure, we should use max_retries.
    for _ in range(step_max_retries):
        try:
            workflow_context.update_workflow_step_context(context, step_id)
            args, kwargs = _resolve_step_inputs(step_inputs)
            # Running the actual step function
            ret = func(*args, **kwargs)
            # Save workflow output
            store = workflow_storage.WorkflowStorage()
            commit_step(store, step_id, ret, outer_most_step_id)
            if isinstance(ret, Workflow):
                # execute sub-workflow
                ret = execute_workflow(ret, outer_most_step_id)
            err = None
            break
        except BaseException as e:
            err = e
    if catch_exceptions:
        return (ret, err)
    else:
        if err is not None:
            raise err
        return ret


def execute_workflow_step(
        step_func: Callable, step_id: "StepID",
        step_inputs: "WorkflowInputTuple", catch_exceptions: bool,
        step_max_retries: int, ray_options: Dict[str, Any],
        outer_most_step_id: "StepID") -> "WorkflowOutputType":
    return _workflow_step_executor.options(**ray_options).remote(
        step_func, workflow_context.get_workflow_step_context(), step_id,
        step_inputs, outer_most_step_id, catch_exceptions, step_max_retries)
