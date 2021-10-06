import asyncio
from dataclasses import dataclass
import logging
from typing import (List, Tuple, Any, Dict, Callable, Optional, TYPE_CHECKING,
                    Union)
import ray
from ray import ObjectRef
from ray._private import signature

from ray.workflow import workflow_context
from ray.workflow import recovery
from ray.workflow.workflow_context import get_step_status_info
from ray.workflow import serialization
from ray.workflow import serialization_context
from ray.workflow import workflow_storage
from ray.workflow.workflow_access import (get_or_create_management_actor,
                                          get_management_actor)
from ray.workflow.common import (
    Workflow,
    WorkflowStatus,
    WorkflowOutputType,
    WorkflowExecutionResult,
    StepType,
    StepID,
    WorkflowData,
)

if TYPE_CHECKING:
    from ray.workflow.common import (WorkflowRef, WorkflowInputs)

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
    """Get the output of a workflow step with the step ID at runtime.

    We lookup the output by the following order:
    1. Query cached step output in the workflow manager. Fetch the physical
       output object.
    2. If failed to fetch the physical output object, look into the storage
       to see whether the output is checkpointed. Load the checkpoint.
    3. If failed to load the checkpoint, resume the step and get the output.
    """
    workflow_manager = get_or_create_management_actor()
    context = workflow_context.get_workflow_step_context()
    workflow_id = context.workflow_id
    storage_url = context.storage_url
    workflow_ref_mapping = []
    for workflow_ref in workflow_refs:
        step_ref = ray.get(
            workflow_manager.get_cached_step_output.remote(
                workflow_id, workflow_ref.step_id))
        get_cached_step = False
        if step_ref is not None:
            try:
                output, _ = _resolve_object_ref(step_ref)
                get_cached_step = True
            except Exception:
                get_cached_step = False
        if not get_cached_step:
            wf_store = workflow_storage.get_workflow_storage()
            try:
                output = wf_store.load_step_output(workflow_ref.step_id)
            except Exception:
                current_step_id = workflow_context.get_current_step_id()
                logger.warning("Failed to get the output of step "
                               f"{workflow_ref.step_id}. Trying to resume it. "
                               f"Current step: '{current_step_id}'")
                step_ref = recovery.resume_workflow_step(
                    workflow_id, workflow_ref.step_id, storage_url,
                    None).persisted_output
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
            objects_mapping, workflow_ref_mapping):
        # reconstruct input arguments under correct serialization context
        flattened_args: List[Any] = ray.get(step_inputs.args)

    # dereference arguments like Ray remote functions
    flattened_args = [
        ray.get(a) if isinstance(a, ObjectRef) else a for a in flattened_args
    ]
    return signature.recover_args(flattened_args)


def execute_workflow(workflow: "Workflow") -> "WorkflowExecutionResult":
    """Execute workflow.

    Returns:
        An object ref that represent the result.
    """
    if workflow.executed:
        return workflow.result
    workflow_data = workflow.data
    baked_inputs = _BakedWorkflowInputs.from_workflow_inputs(
        workflow_data.inputs)
    persisted_output, volatile_output = _workflow_step_executor.options(
        **workflow_data.ray_options).remote(
            workflow_data.step_type, workflow_data.func_body,
            workflow_context.get_workflow_step_context(), workflow.step_id,
            baked_inputs, workflow_data.catch_exceptions,
            workflow_data.max_retries)

    if not isinstance(persisted_output, WorkflowOutputType):
        raise TypeError("Unexpected return type of the workflow.")

    if workflow_data.step_type != StepType.READONLY_ACTOR_METHOD:
        _record_step_status(workflow.step_id, WorkflowStatus.RUNNING,
                            [volatile_output])

    result = WorkflowExecutionResult(persisted_output, volatile_output)
    workflow._result = result
    workflow._executed = True
    return result


async def _write_step_inputs(wf_storage: workflow_storage.WorkflowStorage,
                             step_id: StepID, inputs: WorkflowData) -> None:
    """Save workflow inputs."""
    metadata = inputs.to_metadata()
    with serialization_context.workflow_args_keeping_context():
        # TODO(suquark): in the future we should write to storage directly
        # with plasma store object in memory.
        args_obj = ray.get(inputs.inputs.args)
    workflow_id = wf_storage._workflow_id
    storage = wf_storage._storage
    save_tasks = [
        # TODO (Alex): Handle the json case better?
        wf_storage._put(
            wf_storage._key_step_input_metadata(step_id), metadata, True),
        serialization.dump_to_storage(
            wf_storage._key_step_function_body(step_id), inputs.func_body,
            workflow_id, storage),
        serialization.dump_to_storage(
            wf_storage._key_step_args(step_id), args_obj, workflow_id, storage)
    ]
    await asyncio.gather(*save_tasks)


def commit_step(store: workflow_storage.WorkflowStorage, step_id: "StepID",
                ret: Union["Workflow", Any], exception: Optional[Exception]):
    """Checkpoint the step output.
    Args:
        store: The storage the current workflow is using.
        step_id: The ID of the step.
        ret: The returned object of the workflow step.
    """
    from ray.workflow.common import Workflow
    if isinstance(ret, Workflow):
        assert not ret.executed
        tasks = [
            _write_step_inputs(store, w.step_id, w.data)
            for w in ret._iter_workflows_in_dag()
        ]
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))

    context = workflow_context.get_workflow_step_context()
    store.save_step_output(
        step_id,
        ret,
        exception=exception,
        outer_most_step_id=context.outer_most_step_id)


def _wrap_run(func: Callable, step_type: StepType, step_id: "StepID",
              catch_exceptions: bool, max_retries: int, *args,
              **kwargs) -> Tuple[Any, Any]:
    """Wrap the function and execute it.

    It returns two parts, persisted_output (p-out) and volatile_output (v-out).
    P-out is the part of result to persist in a storage and pass to the
    next step. V-out is the part of result to return to the user but does not
    require persistence.

    This table describes their relationships

    +-----------------------------+-------+--------+----------------------+
    | Step Type                   | p-out | v-out  | catch exception into |
    +-----------------------------+-------+--------+----------------------+
    | Function Step               | Y     | N      | p-out                |
    +-----------------------------+-------+--------+----------------------+
    | Virtual Actor Step          | Y     | Y      | v-out                |
    +-----------------------------+-------+--------+----------------------+
    | Readonly Virtual Actor Step | N     | Y      | v-out                |
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
                f"{workflow_context.get_name()} failed with error message"
                f" {e}. {retry_msg}")
            exception = e

    if catch_exceptions:
        if step_type == StepType.FUNCTION:
            if isinstance(result, Workflow):
                # When it returns a nested workflow, catch_exception
                # should be passed recursively.
                assert exception is None
                result.data.catch_exceptions = True
                persisted_output, volatile_output = result, None
            else:
                persisted_output, volatile_output = (result, exception), None
        elif step_type == StepType.ACTOR_METHOD:
            # virtual actors do not persist exception
            persisted_output, volatile_output = result[0], (result[1],
                                                            exception)
        elif step_type == StepType.READONLY_ACTOR_METHOD:
            persisted_output, volatile_output = None, (result, exception)
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
            persisted_output, volatile_output = result, None
        elif step_type == StepType.ACTOR_METHOD:
            persisted_output, volatile_output = result
        elif step_type == StepType.READONLY_ACTOR_METHOD:
            persisted_output, volatile_output = None, result
        else:
            raise ValueError(f"Unknown StepType '{step_type}'")

    return persisted_output, volatile_output


@ray.remote(num_returns=2)
def _workflow_step_executor(step_type: StepType, func: Callable,
                            context: workflow_context.WorkflowStepContext,
                            step_id: "StepID",
                            baked_inputs: "_BakedWorkflowInputs",
                            catch_exceptions: bool, max_retries: int) -> Any:
    """Executor function for workflow step.

    Args:
        step_type: The type of workflow step.
        func: The workflow step function.
        context: Workflow step context. Used to access correct storage etc.
        step_id: The ID of the step.
        baked_inputs: The processed inputs for the step.
        catch_exceptions: If set to be true, return
            (Optional[Result], Optional[Error]) instead of Result.
        max_retries: Max number of retries encounter of a failure.

    Returns:
        Workflow step output.
    """
    workflow_context.update_workflow_step_context(context, step_id)
    args, kwargs = _resolve_step_inputs(baked_inputs)
    store = workflow_storage.get_workflow_storage()
    try:
        persisted_output, volatile_output = _wrap_run(
            func, step_type, step_id, catch_exceptions, max_retries, *args,
            **kwargs)
    except Exception as e:
        commit_step(store, step_id, None, e)
        raise e
    if step_type == StepType.READONLY_ACTOR_METHOD:
        if isinstance(volatile_output, Workflow):
            raise TypeError(
                "Returning a Workflow from a readonly virtual actor "
                "is not allowed.")
        assert not isinstance(persisted_output, Workflow)
    else:
        store = workflow_storage.get_workflow_storage()
        commit_step(store, step_id, persisted_output, None)
        outer_most_step_id = context.outer_most_step_id
        if isinstance(persisted_output, Workflow):
            if step_type == StepType.FUNCTION:
                # Passing down outer most step so inner nested steps would
                # access the same outer most step.
                if not context.outer_most_step_id:
                    # The current workflow step returns a nested workflow, and
                    # there is no outer step for the current step. So the
                    # current step is the outer most step for the inner nested
                    # workflow steps.
                    outer_most_step_id = workflow_context.get_current_step_id()
            assert volatile_output is None
            # Execute sub-workflow. Pass down "outer_most_step_id".
            with workflow_context.fork_workflow_step_context(
                    outer_most_step_id=outer_most_step_id):
                result = execute_workflow(persisted_output)
            # When virtual actor returns a workflow in the method,
            # the volatile_output and persisted_output will be put together
            persisted_output = result.persisted_output
            volatile_output = result.volatile_output
        elif context.last_step_of_workflow:
            # advance the progress of the workflow
            store.advance_progress(step_id)
        _record_step_status(step_id, WorkflowStatus.SUCCESSFUL)
    logger.info(get_step_status_info(WorkflowStatus.SUCCESSFUL))
    if isinstance(volatile_output, Workflow):
        # This is the case where a step method is called in the virtual actor.
        # We need to run the method to get the final result.
        assert step_type == StepType.ACTOR_METHOD
        volatile_output = volatile_output.run_async(
            workflow_context.get_current_workflow_id())
    return persisted_output, volatile_output


@dataclass
class _BakedWorkflowInputs:
    """This class stores pre-processed inputs for workflow step execution.
    Especially, all input workflows to the workflow step will be scheduled,
    and their outputs (ObjectRefs) replace the original workflows."""
    args: "ObjectRef"
    workflow_outputs: "List[ObjectRef]"
    workflow_refs: "List[WorkflowRef]"

    @classmethod
    def from_workflow_inputs(cls, inputs: "WorkflowInputs"):
        with workflow_context.fork_workflow_step_context(
                outer_most_step_id=None, last_step_of_workflow=False):
            workflow_outputs = [
                execute_workflow(w).persisted_output for w in inputs.workflows
            ]
        return cls(inputs.args, workflow_outputs, inputs.workflow_refs)

    def __reduce__(self):
        return _BakedWorkflowInputs, (self.args, self.workflow_outputs,
                                      self.workflow_refs)


def _record_step_status(step_id: "StepID",
                        status: "WorkflowStatus",
                        outputs: Optional[List["ObjectRef"]] = None) -> None:
    if outputs is None:
        outputs = []

    workflow_id = workflow_context.get_current_workflow_id()
    workflow_manager = get_management_actor()
    ray.get(
        workflow_manager.update_step_status.remote(workflow_id, step_id,
                                                   status, outputs))
