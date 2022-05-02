import time
from dataclasses import dataclass
import functools
import logging
from typing import List, Tuple, Any, Dict, Callable, Optional, TYPE_CHECKING, Union
import ray
from ray import ObjectRef
from ray._private import signature

from ray.workflow import workflow_context
from ray.workflow import recovery
from ray.workflow.workflow_context import get_step_status_info
from ray.workflow import serialization
from ray.workflow import serialization_context
from ray.workflow import workflow_storage
from ray.workflow.workflow_access import (
    get_or_create_management_actor,
    get_management_actor,
)
from ray.workflow.common import (
    Workflow,
    WorkflowStatus,
    WorkflowExecutionResult,
    StepType,
    StepID,
    WorkflowData,
    WorkflowStaticRef,
    CheckpointMode,
)

if TYPE_CHECKING:
    from ray.workflow.common import (
        WorkflowRef,
        WorkflowStepRuntimeOptions,
    )
    from ray.workflow.workflow_context import WorkflowStepContext

WaitResult = Tuple[List[Any], List[Workflow]]

logger = logging.getLogger(__name__)


def _resolve_static_workflow_ref(workflow_ref: WorkflowStaticRef):
    """Get the output of a workflow step with the step ID and ObjectRef."""
    while isinstance(workflow_ref, WorkflowStaticRef):
        workflow_ref = ray.get(workflow_ref.ref)
    return workflow_ref


def _resolve_dynamic_workflow_refs(job_id, workflow_refs: "List[WorkflowRef]"):
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
    workflow_ref_mapping = []
    for workflow_ref in workflow_refs:
        step_ref = ray.get(
            workflow_manager.get_cached_step_output.remote(
                workflow_id, workflow_ref.step_id
            )
        )
        get_cached_step = False
        if step_ref is not None:
            try:
                output = _resolve_static_workflow_ref(step_ref)
                get_cached_step = True
            except Exception:
                get_cached_step = False
        if not get_cached_step:
            wf_store = workflow_storage.get_workflow_storage()
            try:
                output = wf_store.load_step_output(workflow_ref.step_id)
            except Exception:
                current_step_id = workflow_context.get_current_step_id()
                logger.warning(
                    "Failed to get the output of step "
                    f"{workflow_ref.step_id}. Trying to resume it. "
                    f"Current step: '{current_step_id}'"
                )
                step_ref = recovery.resume_workflow_step(
                    job_id, workflow_id, workflow_ref.step_id, None
                ).persisted_output
                output = _resolve_static_workflow_ref(step_ref)
        workflow_ref_mapping.append(output)
    return workflow_ref_mapping


def _execute_workflow(job_id, workflow: "Workflow") -> "WorkflowExecutionResult":
    """Internal function of workflow execution."""
    if workflow.executed:
        return workflow.result

    # Stage 1: prepare inputs
    workflow_data = workflow.data
    inputs = workflow_data.inputs
    # Here A is the outer workflow step, B & C are the inner steps.
    # C is the output step for A, because C produces the output for A.
    #
    # @workflow.step
    # def A():
    #     b = B.step()
    #     return C.step(b)
    #
    # If the outer workflow step skips checkpointing, it would
    # update the checkpoint context of all inner steps except
    # the output step, marking them "detached" from the DAG.
    # Output step is not detached from the DAG because once
    # completed, it replaces the output of the outer step.
    step_context = workflow_context.get_workflow_step_context()
    checkpoint_context = step_context.checkpoint_context.copy()
    # "detached" could be defined recursively:
    # detached := already detached or the outer step skips checkpointing
    checkpoint_context.detached_from_dag = (
        checkpoint_context.detached_from_dag
        or not step_context.checkpoint_context.checkpoint
    )
    # Apply checkpoint context to input steps. Since input steps
    # further apply them to their inputs, this would eventually
    # apply to all steps except the output step. This avoids
    # detaching the output step.
    workflow_outputs = []
    with workflow_context.fork_workflow_step_context(
        outer_most_step_id=None,
        last_step_of_workflow=False,
        checkpoint_context=checkpoint_context,
    ):
        for w in inputs.workflows:
            static_ref = w.ref
            if static_ref is None:
                extra_options = w.data.step_options.ray_options
                # The input workflow is not a reference to an executed
                # workflow.
                static_ref = execute_workflow(job_id, w).persisted_output
                static_ref._resolve_like_object_ref_in_args = extra_options.get(
                    "_resolve_like_object_ref_in_args", False
                )
            workflow_outputs.append(static_ref)

    baked_inputs = _BakedWorkflowInputs(
        args=inputs.args,
        workflow_outputs=workflow_outputs,
        workflow_refs=inputs.workflow_refs,
        job_id=job_id,
    )

    # Stage 2: match executors
    step_options = workflow_data.step_options
    if step_options.allow_inplace:
        # TODO(suquark): For inplace execution, it is impossible
        # to get the ObjectRef of the output before execution.
        # Here we use a dummy ObjectRef, because _record_step_status does not
        # even use it (?!).
        _record_step_status(workflow.step_id, WorkflowStatus.RUNNING, [ray.put(None)])
        # Note: we need to be careful about workflow context when
        # calling the executor directly.
        # TODO(suquark): We still have recursive Python calls.
        # This would cause stack overflow if we have a really
        # deep recursive call. We should fix it later.
        if step_options.step_type == StepType.WAIT:
            executor = _workflow_wait_executor
        else:
            # Tell the executor that we are running inplace. This enables
            # tail-recursion optimization.
            executor = functools.partial(_workflow_step_executor, inplace=True)
    else:
        if step_options.step_type == StepType.WAIT:
            # This is very important to set "num_cpus=0" to
            # ensure "workflow.wait" is not blocked by other
            # tasks.
            executor = _workflow_wait_executor_remote.options(num_cpus=0).remote
        else:
            ray_options = step_options.ray_options.copy()
            # cleanup the "_resolve_like_object_ref_in_args" option, it is not for Ray.
            ray_options.pop("_resolve_like_object_ref_in_args", None)
            executor = _workflow_step_executor_remote.options(**ray_options).remote

    # Stage 3: execution
    persisted_output, volatile_output = executor(
        workflow_data.func_body,
        step_context,
        job_id,
        workflow.step_id,
        baked_inputs,
        workflow_data.step_options,
    )

    # Stage 4: post processing outputs
    if step_options.step_type != StepType.READONLY_ACTOR_METHOD:
        if not step_options.allow_inplace:
            # TODO: [Possible flaky bug] Here the RUNNING state may
            # be recorded earlier than SUCCESSFUL. This caused some
            # confusion during development.

            # convert into workflow static ref for step status record.
            volatile_output = WorkflowStaticRef.from_output(
                workflow.step_id, volatile_output
            )
            _record_step_status(
                workflow.step_id, WorkflowStatus.RUNNING, [volatile_output]
            )

    result = WorkflowExecutionResult(persisted_output, volatile_output)
    workflow._result = result
    workflow._executed = True
    return result


@dataclass
class InplaceReturnedWorkflow:
    """Hold information about a workflow returned from an inplace step."""

    # The returned workflow.
    workflow: Workflow
    # The dict that contains the context of the inplace returned workflow.
    context: Dict


def execute_workflow(job_id, workflow: Workflow) -> "WorkflowExecutionResult":
    """Execute workflow.

    This function also performs tail-recursion optimization for inplace
    workflow steps.

    Args:
        workflow: The workflow to be executed.
    Returns:
        An object ref that represent the result.
    """
    # Tail recursion optimization.
    context = {}
    while True:
        with workflow_context.fork_workflow_step_context(**context):
            result = _execute_workflow(job_id, workflow)
        if not isinstance(result.persisted_output, InplaceReturnedWorkflow):
            break
        workflow = result.persisted_output.workflow
        context = result.persisted_output.context

    # Convert the outputs into WorkflowStaticRef.
    result.persisted_output = WorkflowStaticRef.from_output(
        workflow.step_id, result.persisted_output
    )
    result.volatile_output = WorkflowStaticRef.from_output(
        workflow.step_id, result.volatile_output
    )
    return result


def _write_step_inputs(
    wf_storage: workflow_storage.WorkflowStorage, step_id: StepID, inputs: WorkflowData
) -> None:
    """Save workflow inputs."""
    metadata = inputs.to_metadata()
    with serialization_context.workflow_args_keeping_context():
        # TODO(suquark): in the future we should write to storage directly
        # with plasma store object in memory.
        args_obj = ray.get(inputs.inputs.args)
    workflow_id = wf_storage._workflow_id

    # TODO (Alex): Handle the json case better?
    wf_storage._put(wf_storage._key_step_input_metadata(step_id), metadata, True),
    wf_storage._put(
        wf_storage._key_step_user_metadata(step_id), inputs.user_metadata, True
    ),
    serialization.dump_to_storage(
        wf_storage._key_step_function_body(step_id),
        inputs.func_body,
        workflow_id,
        wf_storage,
    ),
    serialization.dump_to_storage(
        wf_storage._key_step_args(step_id), args_obj, workflow_id, wf_storage
    ),


def commit_step(
    store: workflow_storage.WorkflowStorage,
    step_id: "StepID",
    ret: Union["Workflow", Any],
    *,
    exception: Optional[Exception],
):
    """Checkpoint the step output.
    Args:
        store: The storage the current workflow is using.
        step_id: The ID of the step.
        ret: The returned object of the workflow step.
        exception: The exception caught by the step.
    """
    from ray.workflow.common import Workflow

    if isinstance(ret, Workflow):
        assert not ret.executed
        tasks = []
        for w in ret._iter_workflows_in_dag():
            # If this is a reference to a workflow, do not checkpoint
            # its input (again).
            if w.ref is None:
                tasks.append(_write_step_inputs(store, w.step_id, w.data))

    context = workflow_context.get_workflow_step_context()
    store.save_step_output(
        step_id, ret, exception=exception, outer_most_step_id=context.outer_most_step_id
    )


def _wrap_run(
    func: Callable, runtime_options: "WorkflowStepRuntimeOptions", *args, **kwargs
) -> Tuple[Any, Any]:
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
        func: The function body.
        runtime_options: Step execution params.

    Returns:
        State and output.
    """
    exception = None
    result = None
    done = False
    # max_retries are for application level failure.
    # For ray failure, we should use max_retries.
    i = 0
    while not done:
        if i == 0:
            logger.info(f"{get_step_status_info(WorkflowStatus.RUNNING)}")
        else:
            total_retries = (
                runtime_options.max_retries
                if runtime_options.max_retries != -1
                else "inf"
            )
            logger.info(
                f"{get_step_status_info(WorkflowStatus.RUNNING)}"
                f"\tretries: [{i}/{total_retries}]"
            )
        try:
            result = func(*args, **kwargs)
            exception = None
            done = True
        except BaseException as e:
            if i == runtime_options.max_retries:
                retry_msg = "Maximum retry reached, stop retry."
                exception = e
                done = True
            else:
                retry_msg = "The step will be retried."
                i += 1
            logger.error(
                f"{workflow_context.get_name()} failed with error message"
                f" {e}. {retry_msg}"
            )
    step_type = runtime_options.step_type
    if runtime_options.catch_exceptions:
        if step_type == StepType.FUNCTION:
            if isinstance(result, Workflow):
                # When it returns a nested workflow, catch_exception
                # should be passed recursively.
                assert exception is None
                result.data.step_options.catch_exceptions = True
                persisted_output, volatile_output = result, None
            else:
                persisted_output, volatile_output = (result, exception), None
        elif step_type == StepType.ACTOR_METHOD:
            # virtual actors do not persist exception
            persisted_output, volatile_output = result[0], (result[1], exception)
        elif runtime_options.step_type == StepType.READONLY_ACTOR_METHOD:
            persisted_output, volatile_output = None, (result, exception)
        else:
            raise ValueError(f"Unknown StepType '{step_type}'")
    else:
        if exception is not None:
            if step_type != StepType.READONLY_ACTOR_METHOD:
                status = WorkflowStatus.FAILED
                _record_step_status(workflow_context.get_current_step_id(), status)
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


def _workflow_step_executor(
    func: Callable,
    context: "WorkflowStepContext",
    job_id: str,
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
    inplace: bool = False,
) -> Tuple[Any, Any]:
    """Executor function for workflow step.

    Args:
        step_id: ID of the step.
        func: The workflow step function.
        baked_inputs: The processed inputs for the step.
        context: Workflow step context. Used to access correct storage etc.
        runtime_options: Parameters for workflow step execution.
        inplace: Execute the workflow inplace.

    Returns:
        Workflow step output.
    """
    # Part 1: update the context for the step
    workflow_context.update_workflow_step_context(context, step_id)
    context = workflow_context.get_workflow_step_context()
    step_type = runtime_options.step_type
    context.checkpoint_context.checkpoint = runtime_options.checkpoint

    # Part 2: resolve inputs
    args, kwargs = baked_inputs.resolve()

    # Part 3: execute the step
    store = workflow_storage.get_workflow_storage()
    try:
        step_prerun_metadata = {"start_time": time.time()}
        store.save_step_prerun_metadata(step_id, step_prerun_metadata)
        with workflow_context.workflow_execution():
            persisted_output, volatile_output = _wrap_run(
                func, runtime_options, *args, **kwargs
            )
        step_postrun_metadata = {"end_time": time.time()}
        store.save_step_postrun_metadata(step_id, step_postrun_metadata)
    except Exception as e:
        # Always checkpoint the exception.
        commit_step(store, step_id, None, exception=e)
        raise e

    # Part 4: save outputs
    if step_type == StepType.READONLY_ACTOR_METHOD:
        if isinstance(volatile_output, Workflow):
            raise TypeError(
                "Returning a Workflow from a readonly virtual actor is not allowed."
            )
        assert not isinstance(persisted_output, Workflow)
    else:
        # TODO(suquark): Validate checkpoint options before
        # commit the step.
        store = workflow_storage.get_workflow_storage()
        if CheckpointMode(runtime_options.checkpoint) == CheckpointMode.SYNC:
            commit_step(
                store,
                step_id,
                persisted_output,
                exception=None,
            )
        if isinstance(persisted_output, Workflow):
            sub_workflow = persisted_output
            outer_most_step_id = context.outer_most_step_id
            assert volatile_output is None
            if step_type == StepType.FUNCTION:
                # Passing down outer most step so inner nested steps would
                # access the same outer most step.
                if not context.outer_most_step_id:
                    # The current workflow step returns a nested workflow, and
                    # there is no outer step for the current step. So the
                    # current step is the outer most step for the inner nested
                    # workflow steps.
                    outer_most_step_id = workflow_context.get_current_step_id()
            if inplace:
                _step_options = sub_workflow.data.step_options
                if (
                    _step_options.step_type != StepType.WAIT
                    and runtime_options.ray_options != _step_options.ray_options
                ):
                    logger.warning(
                        f"Workflow step '{sub_workflow.step_id}' uses "
                        f"a Ray option different to its caller step '{step_id}' "
                        f"and will be executed inplace. Ray assumes it still "
                        f"consumes the same resource as the caller. This may result "
                        f"in oversubscribing resources."
                    )
                return (
                    InplaceReturnedWorkflow(
                        sub_workflow, {"outer_most_step_id": outer_most_step_id}
                    ),
                    None,
                )
            # Execute sub-workflow. Pass down "outer_most_step_id".
            with workflow_context.fork_workflow_step_context(
                outer_most_step_id=outer_most_step_id
            ):
                result = execute_workflow(job_id, sub_workflow)
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
            workflow_context.get_current_workflow_id()
        )
        volatile_output = WorkflowStaticRef.from_output(step_id, volatile_output)
    return persisted_output, volatile_output


@ray.remote(num_returns=2)
def _workflow_step_executor_remote(
    func: Callable,
    context: "WorkflowStepContext",
    job_id: str,
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Any:
    """The remote version of '_workflow_step_executor'."""
    with workflow_context.workflow_logging_context(job_id):
        return _workflow_step_executor(
            func, context, job_id, step_id, baked_inputs, runtime_options
        )


def _workflow_wait_executor(
    func: Callable,
    context: "WorkflowStepContext",
    job_id: str,
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Tuple[WaitResult, None]:
    """Executor of 'workflow.wait' steps.

    It returns a tuple that contains wait result. The wait result is a list
    of result of workflows that are ready and a list of workflows that are
    pending.
    """
    # Part 1: Update the context for the step.
    workflow_context.update_workflow_step_context(context, step_id)
    context = workflow_context.get_workflow_step_context()
    step_type = runtime_options.step_type
    assert step_type == StepType.WAIT
    wait_options = runtime_options.ray_options.get("wait_options", {})

    # Part 2: Resolve any ready workflows.
    ready_workflows, remaining_workflows = baked_inputs.wait(**wait_options)
    ready_objects = [_resolve_static_workflow_ref(w.ref) for w in ready_workflows]
    persisted_output = (ready_objects, remaining_workflows)

    # Part 3: Save the outputs.
    store = workflow_storage.get_workflow_storage()
    # TODO(suquark): Because the outputs are not generated by "workflow.wait",
    # we do not checkpoint the outputs here. Those steps that generate
    # outputs should checkpoint them.
    commit_step(store, step_id, persisted_output, exception=None)
    if context.last_step_of_workflow:
        # advance the progress of the workflow
        store.advance_progress(step_id)

    _record_step_status(step_id, WorkflowStatus.SUCCESSFUL)
    logger.info(get_step_status_info(WorkflowStatus.SUCCESSFUL))
    return persisted_output, None


@ray.remote(num_returns=2)
def _workflow_wait_executor_remote(
    func: Callable,
    context: "WorkflowStepContext",
    job_id: str,
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Any:
    """The remote version of '_workflow_wait_executor'"""
    with workflow_context.workflow_logging_context(job_id):
        return _workflow_wait_executor(
            func, context, job_id, step_id, baked_inputs, runtime_options
        )


class _SelfDereference:
    """A object that dereferences static object ref during deserialization."""

    def __init__(self, x):
        self.x = x

    def __reduce__(self):
        return _resolve_static_workflow_ref, (self.x,)


@dataclass
class _BakedWorkflowInputs:
    """This class stores pre-processed inputs for workflow step execution.
    Especially, all input workflows to the workflow step will be scheduled,
    and their outputs (ObjectRefs) replace the original workflows."""

    args: "ObjectRef"
    workflow_outputs: "List[WorkflowStaticRef]"
    workflow_refs: "List[WorkflowRef]"
    job_id: str

    def resolve(self) -> Tuple[List, Dict]:
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

        Returns:
            Instances of arguments.
        """
        objects_mapping = []
        for static_workflow_ref in self.workflow_outputs:
            if static_workflow_ref._resolve_like_object_ref_in_args:
                # Keep it unresolved as an ObjectRef. Then we resolve it
                # later in the arguments, like how Ray does with ObjectRefs.
                obj = ray.put(_SelfDereference(static_workflow_ref))
            else:
                obj = _resolve_static_workflow_ref(static_workflow_ref)
            objects_mapping.append(obj)

        workflow_ref_mapping = _resolve_dynamic_workflow_refs(
            self.job_id, self.workflow_refs
        )

        with serialization_context.workflow_args_resolving_context(
            objects_mapping, workflow_ref_mapping
        ):
            # reconstruct input arguments under correct serialization context
            flattened_args: List[Any] = ray.get(self.args)

        # dereference arguments like Ray remote functions
        flattened_args = [
            ray.get(a) if isinstance(a, ObjectRef) else a for a in flattened_args
        ]
        return signature.recover_args(flattened_args)

    def wait(
        self, num_returns: int = 1, timeout: Optional[float] = None
    ) -> Tuple[List[Workflow], List[Workflow]]:
        """Return a list of workflows that are ready and a list of workflows that
        are not. See `api.wait()` for details.

        Args:
            num_returns (int): The number of workflows that should be returned.
            timeout (float): The maximum amount of time in seconds to wait
            before returning.

        Returns:
            A list of workflows that are ready and a list of the remaining
            workflows.
        """
        if self.workflow_refs:
            raise ValueError(
                "Currently, we do not support wait operations "
                "on dynamic workflow refs. They are typically "
                "generated by virtual actors."
            )
        refs_map = {w.ref: w for w in self.workflow_outputs}
        ready_ids, remaining_ids = ray.wait(
            list(refs_map.keys()), num_returns=num_returns, timeout=timeout
        )
        ready_workflows = [Workflow.from_ref(refs_map[i]) for i in ready_ids]
        remaining_workflows = [Workflow.from_ref(refs_map[i]) for i in remaining_ids]
        return ready_workflows, remaining_workflows

    def __reduce__(self):
        return _BakedWorkflowInputs, (
            self.args,
            self.workflow_outputs,
            self.workflow_refs,
            self.job_id,
        )


def _record_step_status(
    step_id: "StepID",
    status: "WorkflowStatus",
    outputs: Optional[List["ObjectRef"]] = None,
) -> None:
    if outputs is None:
        outputs = []

    workflow_id = workflow_context.get_current_workflow_id()
    workflow_manager = get_management_actor()
    ray.get(
        workflow_manager.update_step_status.remote(
            workflow_id, step_id, status, outputs
        )
    )
