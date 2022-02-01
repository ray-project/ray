import time
import asyncio
from dataclasses import dataclass
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
    WorkflowOutputType,
    WorkflowExecutionResult,
    StepType,
    StepID,
    WorkflowData,
    WorkflowStaticRef,
)

if TYPE_CHECKING:
    from ray.workflow.common import WorkflowRef, WorkflowStepRuntimeOptions
    from ray.workflow.workflow_context import WorkflowStepContext

WaitResult = Tuple[List[Any], List[Workflow]]

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
                workflow_id, workflow_ref.step_id
            )
        )
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
                logger.warning(
                    "Failed to get the output of step "
                    f"{workflow_ref.step_id}. Trying to resume it. "
                    f"Current step: '{current_step_id}'"
                )
                step_ref = recovery.resume_workflow_step(
                    workflow_id, workflow_ref.step_id, storage_url, None
                ).persisted_output
                output, _ = _resolve_object_ref(step_ref)
        workflow_ref_mapping.append(output)
    return workflow_ref_mapping


def execute_workflow(workflow: "Workflow") -> "WorkflowExecutionResult":
    """Execute workflow.

    Args:
        workflow: The workflow to be executed.

    Returns:
        An object ref that represent the result.
    """
    if workflow.executed:
        return workflow.result

    # Stage 1: prepare inputs
    workflow_data = workflow.data
    inputs = workflow_data.inputs
    workflow_outputs = []
    with workflow_context.fork_workflow_step_context(
        outer_most_step_id=None, last_step_of_workflow=False
    ):
        for w in inputs.workflows:
            static_ref = w.ref
            if static_ref is None:
                # The input workflow is not a reference to an executed
                # workflow .
                output = execute_workflow(w).persisted_output
                static_ref = WorkflowStaticRef(step_id=w.step_id, ref=output)
            workflow_outputs.append(static_ref)

    baked_inputs = _BakedWorkflowInputs(
        args=workflow_data.inputs.args,
        workflow_outputs=workflow_outputs,
        workflow_refs=inputs.workflow_refs,
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
            executor = _workflow_step_executor
    else:
        if step_options.step_type == StepType.WAIT:
            # This is very important to set "num_cpus=0" to
            # ensure "workflow.wait" is not blocked by other
            # tasks.
            executor = _workflow_wait_executor_remote.options(num_cpus=0).remote
        else:
            executor = _workflow_step_executor_remote.options(
                **step_options.ray_options
            ).remote

    # Stage 3: execution
    persisted_output, volatile_output = executor(
        workflow_data.func_body,
        workflow_context.get_workflow_step_context(),
        workflow.step_id,
        baked_inputs,
        workflow_data.step_options,
    )

    # Stage 4: post processing outputs
    if not isinstance(persisted_output, WorkflowOutputType):
        persisted_output = ray.put(persisted_output)
    if not isinstance(persisted_output, WorkflowOutputType):
        volatile_output = ray.put(volatile_output)

    if step_options.step_type != StepType.READONLY_ACTOR_METHOD:
        if not step_options.allow_inplace:
            # TODO: [Possible flaky bug] Here the RUNNING state may
            # be recorded earlier than SUCCESSFUL. This caused some
            # confusion during development.
            _record_step_status(
                workflow.step_id, WorkflowStatus.RUNNING, [volatile_output]
            )

    result = WorkflowExecutionResult(persisted_output, volatile_output)
    workflow._result = result
    workflow._executed = True
    return result


async def _write_step_inputs(
    wf_storage: workflow_storage.WorkflowStorage, step_id: StepID, inputs: WorkflowData
) -> None:
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
        wf_storage._put(wf_storage._key_step_input_metadata(step_id), metadata, True),
        wf_storage._put(
            wf_storage._key_step_user_metadata(step_id), inputs.user_metadata, True
        ),
        serialization.dump_to_storage(
            wf_storage._key_step_function_body(step_id),
            inputs.func_body,
            workflow_id,
            storage,
        ),
        serialization.dump_to_storage(
            wf_storage._key_step_args(step_id), args_obj, workflow_id, storage
        ),
    ]
    await asyncio.gather(*save_tasks)


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
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))

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
    # max_retries are for application level failure.
    # For ray failure, we should use max_retries.
    for i in range(runtime_options.max_retries):
        logger.info(
            f"{get_step_status_info(WorkflowStatus.RUNNING)}"
            f"\t[{i + 1}/{runtime_options.max_retries}]"
        )
        try:
            result = func(*args, **kwargs)
            exception = None
            break
        except BaseException as e:
            if i + 1 == runtime_options.max_retries:
                retry_msg = "Maximum retry reached, stop retry."
            else:
                retry_msg = "The step will be retried."
            logger.error(
                f"{workflow_context.get_name()} failed with error message"
                f" {e}. {retry_msg}"
            )
            exception = e

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
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Tuple[Any, Any]:
    """Executor function for workflow step.

    Args:
        step_id: ID of the step.
        func: The workflow step function.
        baked_inputs: The processed inputs for the step.
        context: Workflow step context. Used to access correct storage etc.
        runtime_options: Parameters for workflow step execution.

    Returns:
        Workflow step output.
    """
    # Part 1: update the context for the step
    workflow_context.update_workflow_step_context(context, step_id)
    context = workflow_context.get_workflow_step_context()
    step_type = runtime_options.step_type

    # Part 2: resolve inputs
    args, kwargs = baked_inputs.resolve()

    # Part 3: execute the step
    store = workflow_storage.get_workflow_storage()
    try:
        step_prerun_metadata = {"start_time": time.time()}
        store.save_step_prerun_metadata(step_id, step_prerun_metadata)
        persisted_output, volatile_output = _wrap_run(
            func, runtime_options, *args, **kwargs
        )
        step_postrun_metadata = {"end_time": time.time()}
        store.save_step_postrun_metadata(step_id, step_postrun_metadata)
    except Exception as e:
        commit_step(store, step_id, None, exception=e)
        raise e

    # Part 4: save outputs
    if step_type == StepType.READONLY_ACTOR_METHOD:
        if isinstance(volatile_output, Workflow):
            raise TypeError(
                "Returning a Workflow from a readonly virtual actor " "is not allowed."
            )
        assert not isinstance(persisted_output, Workflow)
    else:
        store = workflow_storage.get_workflow_storage()
        commit_step(store, step_id, persisted_output, exception=None)
        if isinstance(persisted_output, Workflow):
            outer_most_step_id = context.outer_most_step_id
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
                outer_most_step_id=outer_most_step_id
            ):
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
            workflow_context.get_current_workflow_id()
        )
    return persisted_output, volatile_output


@ray.remote(num_returns=2)
def _workflow_step_executor_remote(
    func: Callable,
    context: "WorkflowStepContext",
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Any:
    """The remote version of '_workflow_step_executor'."""
    return _workflow_step_executor(
        func, context, step_id, baked_inputs, runtime_options
    )


def _workflow_wait_executor(
    func: Callable,
    context: "WorkflowStepContext",
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
    ready_objects = []
    for w in ready_workflows:
        (
            obj,
            _,
        ) = _resolve_object_ref(w.ref.ref)
        ready_objects.append(obj)
    persisted_output = (ready_objects, remaining_workflows)

    # Part 3: Save the outputs.
    store = workflow_storage.get_workflow_storage()
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
    step_id: "StepID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Any:
    """The remote version of '_workflow_wait_executor'"""
    return _workflow_wait_executor(
        func, context, step_id, baked_inputs, runtime_options
    )


@dataclass
class _BakedWorkflowInputs:
    """This class stores pre-processed inputs for workflow step execution.
    Especially, all input workflows to the workflow step will be scheduled,
    and their outputs (ObjectRefs) replace the original workflows."""

    args: "ObjectRef"
    workflow_outputs: "List[WorkflowStaticRef]"
    workflow_refs: "List[WorkflowRef]"

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
        for obj_ref in self.workflow_outputs:
            obj, ref = _resolve_object_ref(obj_ref.ref)
            objects_mapping.append(obj)

        workflow_ref_mapping = _resolve_dynamic_workflow_refs(self.workflow_refs)

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
