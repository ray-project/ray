from dataclasses import dataclass
import logging
from typing import (List, Tuple, Any, Dict, Callable, Optional, TYPE_CHECKING,
                    Union)
import ray
from ray import ObjectRef
from ray._private import signature

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import recovery
from ray.experimental.workflow.workflow_context import get_step_status_info
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow.workflow_access import (
    MANAGEMENT_ACTOR_NAME, get_or_create_management_actor)
from ray.experimental.workflow.common import (
    Workflow, WorkflowStatus, WorkflowOutputType, WorkflowExecutionResult,
    StepType)

if TYPE_CHECKING:
    from ray.experimental.workflow.common import (StepID, WorkflowData,
                                                  WorkflowRef, WorkflowInputs)

StepInputTupleToResolve = Tuple[ObjectRef, List[ObjectRef], List[ObjectRef]]

logger = logging.getLogger(__name__)


def _resolve_object_ref(ref: ObjectRef) -> Tuple[Any, ObjectRef]:
    """
    Resolves the ObjectRef into the object instance.

    Returns:
        The object instance and the direct ObjectRef to the instance.
    """
    last_ref = ref
    while True:
        if isinstance(ref, ObjectRef):
            last_ref = ref
        else:
            break
        ref = ray.get(last_ref)
    return ref, last_ref


def _resolve_dynamic_workflow_refs(workflow_refs: "List[WorkflowRef]"):
    workflow_manager = get_or_create_management_actor()
    context = workflow_context.get_workflow_step_context()
    workflow_id = context.workflow_id
    storage_url = context.storage_url
    workflow_ref_mapping = []
    for workflow_ref in workflow_refs:
        step_ref = workflow_manager.get_cached_step.remote(
            workflow_id, workflow_ref.step_id)
        try:
            output, _ = _resolve_object_ref(step_ref)
        except Exception:
            step_ref = recovery.resume_workflow_step(
                workflow_id, workflow_ref.step_id, storage_url).state
            output, _ = _resolve_object_ref(step_ref)
        workflow_ref_mapping.append(output)
    return workflow_ref_mapping


def _resolve_step_inputs(
        step_inputs: "_BakedWorkflowInputs") -> Tuple[List, Dict]:
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
    for obj_ref in step_inputs.workflow_outputs:
        obj, ref = _resolve_object_ref(obj_ref)
        objects_mapping.append(obj)

    workflow_ref_mapping = _resolve_dynamic_workflow_refs(
        step_inputs.workflow_refs)

    with serialization_context.workflow_args_resolving_context(
            objects_mapping, step_inputs.object_refs, workflow_ref_mapping):
        # reconstruct input arguments under correct serialization context
        flattened_args: List[Any] = ray.get(step_inputs.args)

    # dereference arguments like Ray remote functions
    flattened_args = [
        ray.get(a) if isinstance(a, ObjectRef) else a for a in flattened_args
    ]
    return signature.recover_args(flattened_args)


def execute_workflow(
        workflow: "Workflow",
        outer_most_step_id: Optional[str] = None,
        last_step_of_workflow: bool = False) -> "WorkflowExecutionResult":
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
        last_step_of_workflow: The step that generates the output of the
            workflow (including nested steps).
    Returns:
        An object ref that represent the result.
    """
    if workflow.executed:
        return workflow.state

    _record_step_status(workflow.id, WorkflowStatus.RUNNING)
    workflow_data = workflow.data
    baked_inputs = _BakedWorkflowInputs.from_workflow_inputs(
        workflow_data.inputs)
    state, output = _workflow_step_executor.options(
        **workflow_data.ray_options).remote(
            workflow_data.step_type, workflow_data.func_body,
            workflow_context.get_workflow_step_context(), workflow.id,
            baked_inputs, outer_most_step_id, workflow_data.catch_exceptions,
            workflow_data.max_retries, last_step_of_workflow)

    if not isinstance(state, WorkflowOutputType):
        raise TypeError("Unexpected return type of the workflow.")
    workflow._state = state
    workflow._executed = True
    return WorkflowExecutionResult(state, output)


def commit_step(store: workflow_storage.WorkflowStorage,
                step_id: "StepID",
                ret: Union["Workflow", Any],
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
    from ray.experimental.workflow.common import Workflow
    if isinstance(ret, Workflow):
        store.save_subworkflow(ret)
    store.save_step_output(step_id, ret, outer_most_step_id)


def _wrap_run(func: Callable, step_type: StepType, step_id: "StepID",
              catch_exceptions: bool, max_retries: int, *args,
              **kwargs) -> Tuple[Any, Any]:
    """Wrap the function and execute it.

    It returns two parts, state and output. State is the part of result
    to persist in a storage and pass to the next step. Output is the part
    of result to return to the user but does not require persistence.

    This table describes their relationships

    +-----------------------------+-------+--------+----------------------+
    | Step Type                   | state | output | catch exception into |
    +-----------------------------+-------+--------+----------------------+
    | Function Step               | Y     | N      | state                |
    +-----------------------------+-------+--------+----------------------+
    | Virtual Actor Step          | Y     | Y      | output               |
    +-----------------------------+-------+--------+----------------------+
    | Readonly Virtual Actor Step | N     | Y      | output               |
    +-----------------------------+-------+--------+----------------------+

    Args:
        step_type: The type of the step producing the result.
        catch_exceptions: True if we would like to catch the exception.
        max_retries: Max retry times for failure.

    Returns:
        State and output.
    """
    exception = None
    result = None
    # max_retries are for application level failure.
    # For ray failure, we should use max_retries.
    for i in range(max_retries):
        logger.info(f"{get_step_status_info(WorkflowStatus.RUNNING)}"
                    f"\t[{i+1}/{max_retries}]")
        try:
            result = func(*args, **kwargs)
            exception = None
            break
        except BaseException as e:
            if i + 1 == max_retries:
                retry_msg = "Maximum retry reached, stop retry."
            else:
                retry_msg = "The step will be retried."
            logger.error(
                f"{workflow_context.get_step_name()} failed with error message"
                f" {e}. {retry_msg}")
            exception = e

    if catch_exceptions:
        if step_type == StepType.FUNCTION:
            state, output = (result, exception), None
        elif step_type == StepType.ACTOR_METHOD:
            # virtual actors do not persist exception
            state, output = result[0], (result[1], exception)
        elif step_type == StepType.READONLY_ACTOR_METHOD:
            state, output = None, (result, exception)
        else:
            raise ValueError(f"Unknown StepType '{step_type}'")
    else:
        if exception is not None:
            if step_type != StepType.READONLY_ACTOR_METHOD:
                status = WorkflowStatus.FAILED
                _record_step_status(step_id, status)
                logger.info(get_step_status_info(status))
            raise exception
        if step_type == StepType.FUNCTION:
            state, output = result, None
        elif step_type == StepType.ACTOR_METHOD:
            state, output = result
        elif step_type == StepType.READONLY_ACTOR_METHOD:
            state, output = None, result
        else:
            raise ValueError(f"Unknown StepType '{step_type}'")

    is_nested = isinstance(state, Workflow)
    if step_type != StepType.FUNCTION and is_nested:
        # TODO(suquark): Support returning a workflow inside
        # a virtual actor.
        raise TypeError("Only a workflow step function "
                        "can return a workflow.")

    return state, output


@ray.remote(num_returns=2)
def _workflow_step_executor(
        step_type: StepType, func: Callable,
        context: workflow_context.WorkflowStepContext, step_id: "StepID",
        baked_inputs: "_BakedWorkflowInputs", outer_most_step_id: "StepID",
        catch_exceptions: bool, max_retries: int,
        last_step_of_workflow: bool) -> Any:
    """Executor function for workflow step.

    Args:
        step_type: The type of workflow step.
        func: The workflow step function.
        context: Workflow step context. Used to access correct storage etc.
        step_id: The ID of the step.
        baked_inputs: The processed inputs for the step.
        outer_most_step_id: See "step_executor.execute_workflow" for
            explanation.
        catch_exceptions: If set to be true, return
            (Optional[Result], Optional[Error]) instead of Result.
        max_retries: Max number of retries encounter of a failure.
        last_step_of_workflow: The step that generates the output of the
            workflow (including nested steps).

    Returns:
        Workflow step output.
    """
    workflow_context.update_workflow_step_context(context, step_id)
    args, kwargs = _resolve_step_inputs(baked_inputs)
    state, output = _wrap_run(func, step_type, step_id, catch_exceptions,
                              max_retries, *args, **kwargs)

    if step_type != StepType.READONLY_ACTOR_METHOD:
        store = workflow_storage.get_workflow_storage()
        # Save workflow output
        commit_step(store, step_id, state, outer_most_step_id)
        # We MUST execute the workflow after saving the output.
        if isinstance(state, Workflow):
            if step_type == StepType.FUNCTION:
                # Passing down outer most step so inner nested steps would
                # access the same outer most step.
                if not outer_most_step_id:
                    # The current workflow step returns a nested workflow, and
                    # there is no outer step for the current step. So the
                    # current step is the outer most step for the inner nested
                    # workflow steps.
                    outer_most_step_id = workflow_context.get_current_step_id()
                # execute sub-workflow
                state = execute_workflow(state, outer_most_step_id,
                                         last_step_of_workflow).state
            else:
                # TODO(suquark): Support returning a workflow inside
                # a virtual actor.
                raise TypeError("Only a workflow step function "
                                "can return a workflow.")
        elif last_step_of_workflow:
            # advance the progress of the workflow
            store.advance_progress(step_id)
        _record_step_status(step_id, WorkflowStatus.SUCCESSFUL)
    logger.info(get_step_status_info(WorkflowStatus.SUCCESSFUL))
    return state, output


@dataclass
class _BakedWorkflowInputs:
    args: "ObjectRef"
    workflow_outputs: "List[ObjectRef]"
    object_refs: "List[ObjectRef]"
    workflow_refs: "List[WorkflowRef]"

    @classmethod
    def from_workflow_inputs(cls, inputs: "WorkflowInputs"):
        workflow_outputs = [
            execute_workflow(w).state for w in inputs.workflows
        ]
        return cls(inputs.args, workflow_outputs, inputs.object_refs,
                   inputs.workflow_refs)

    def __reduce__(self):
        return _BakedWorkflowInputs, (self.args, self.workflow_outputs,
                                      self.object_refs, self.workflow_refs)


def execute_readonly_virtual_actor_step(
        step_id: "StepID",
        workflow_data: "WorkflowData") -> "WorkflowOutputType":
    baked_inputs = _BakedWorkflowInputs.from_workflow_inputs(
        workflow_data.inputs)
    outer_most_step_id = ""
    step_type = StepType.READONLY_ACTOR_METHOD
    ret = _workflow_step_executor.options(**workflow_data.ray_options).remote(
        step_type,
        workflow_data.func_body,
        workflow_context.get_workflow_step_context(),
        step_id,
        baked_inputs,
        outer_most_step_id,
        workflow_data.catch_exceptions,
        workflow_data.max_retries,
        last_step_of_workflow=True)
    return ret[1]  # only return output. skip state


def _record_step_status(step_id: "StepID", status: "WorkflowStatus") -> None:
    workflow_id = workflow_context.get_current_workflow_id()
    workflow_manager = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    ray.get(
        workflow_manager.update_step_status.remote(workflow_id, step_id,
                                                   status))
