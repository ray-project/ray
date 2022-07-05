import time
from dataclasses import dataclass
import logging
from typing import List, Tuple, Any, Dict, Callable, TYPE_CHECKING
import ray
from ray import ObjectRef
from ray._private import signature

from ray.dag import DAGNode
from ray.workflow import workflow_context
from ray.workflow.workflow_context import get_step_status_info
from ray.workflow import serialization_context
from ray.workflow import workflow_storage

from ray.workflow.common import (
    WorkflowStatus,
    WorkflowExecutionMetadata,
    StepType,
    TaskID,
    WorkflowRef,
    CheckpointMode,
)
from ray.workflow.workflow_state import WorkflowExecutionState
from ray.workflow.workflow_state_from_dag import workflow_state_from_dag

if TYPE_CHECKING:
    from ray.workflow.common import (
        WorkflowStepRuntimeOptions,
    )
    from ray.workflow.workflow_context import WorkflowStepContext


logger = logging.getLogger(__name__)


def get_step_executor(step_options: "WorkflowStepRuntimeOptions"):
    if step_options.step_type == StepType.FUNCTION:
        executor = _workflow_step_executor_remote.options(
            **step_options.ray_options
        ).remote
    else:
        raise ValueError(f"Invalid step type {step_options.step_type}")
    return executor


def _wrap_run(
    func: Callable, runtime_options: "WorkflowStepRuntimeOptions", *args, **kwargs
) -> Tuple[Any, Any]:
    """Wrap the function and execute it.

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
            if isinstance(result, DAGNode):
                assert exception is None
                output = result
            else:
                output = (result, exception)
        else:
            raise ValueError(f"Unknown StepType '{step_type}'")
    else:
        if exception is not None:
            raise exception
        if step_type == StepType.FUNCTION:
            output = result
        else:
            raise ValueError(f"Unknown StepType '{step_type}'")

    return output


def _workflow_step_executor(
    func: Callable,
    context: "WorkflowStepContext",
    task_id: "TaskID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Tuple[Any, Any]:
    """Executor function for workflow step.

    Args:
        task_id: ID of the step.
        func: The workflow step function.
        baked_inputs: The processed inputs for the step.
        context: Workflow step context. Used to access correct storage etc.
        runtime_options: Parameters for workflow step execution.

    Returns:
        Workflow step output.
    """
    with workflow_context.workflow_step_context(context):
        store = workflow_storage.get_workflow_storage()
        # Part 1: resolve inputs
        args, kwargs = baked_inputs.resolve(store)

        # Part 2: execute the step
        try:
            store.save_step_prerun_metadata(task_id, {"start_time": time.time()})
            with workflow_context.workflow_execution():
                output = _wrap_run(func, runtime_options, *args, **kwargs)
            store.save_step_postrun_metadata(task_id, {"end_time": time.time()})
        except Exception as e:
            # Always checkpoint the exception.
            store.save_step_output(task_id, None, exception=e)
            raise e

        if isinstance(output, DAGNode):
            output = workflow_state_from_dag(output, None, context.workflow_id)
            execution_metadata = WorkflowExecutionMetadata(is_output_workflow=True)
        else:
            execution_metadata = WorkflowExecutionMetadata()

        # Part 3: save outputs
        # TODO(suquark): Validate checkpoint options before commit the task.
        if CheckpointMode(runtime_options.checkpoint) == CheckpointMode.SYNC:
            if isinstance(output, WorkflowExecutionState):
                store.save_workflow_execution_state(task_id, output)
            else:
                store.save_step_output(task_id, output, exception=None)
        return execution_metadata, output


@ray.remote(num_returns=2)
def _workflow_step_executor_remote(
    func: Callable,
    context: "WorkflowStepContext",
    job_id: str,
    task_id: "TaskID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowStepRuntimeOptions",
) -> Any:
    """The remote version of '_workflow_step_executor'."""
    with workflow_context.workflow_logging_context(job_id):
        return _workflow_step_executor(
            func, context, task_id, baked_inputs, runtime_options
        )


@dataclass
class _BakedWorkflowInputs:
    """This class stores pre-processed inputs for workflow step execution.
    Especially, all input workflows to the workflow step will be scheduled,
    and their outputs (ObjectRefs) replace the original workflows."""

    args: "ObjectRef"
    workflow_refs: "List[WorkflowRef]"

    def resolve(self, store: workflow_storage.WorkflowStorage) -> Tuple[List, Dict]:
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
        workflow_ref_mapping = []
        for r in self.workflow_refs:
            if r.ref is None:
                workflow_ref_mapping.append(store.load_step_output(r.task_id))
            else:
                workflow_ref_mapping.append(r.ref)

        with serialization_context.workflow_args_resolving_context(
            workflow_ref_mapping
        ):
            # reconstruct input arguments under correct serialization context
            flattened_args: List[Any] = ray.get(self.args)

        # dereference arguments like Ray remote functions
        flattened_args = [
            ray.get(a) if isinstance(a, ObjectRef) else a for a in flattened_args
        ]
        return signature.recover_args(flattened_args)
