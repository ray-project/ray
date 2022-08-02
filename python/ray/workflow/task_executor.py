import time
from dataclasses import dataclass
import logging
from typing import List, Tuple, Any, Dict, Callable, TYPE_CHECKING
import ray
from ray import ObjectRef
from ray._private import signature

from ray.dag import DAGNode
from ray.workflow import workflow_context
from ray.workflow.workflow_context import get_task_status_info
from ray.workflow import serialization_context
from ray.workflow import workflow_storage

from ray.workflow.common import (
    WorkflowStatus,
    WorkflowExecutionMetadata,
    TaskType,
    TaskID,
    WorkflowRef,
    CheckpointMode,
)
from ray.workflow.workflow_state import WorkflowExecutionState
from ray.workflow.workflow_state_from_dag import workflow_state_from_dag

if TYPE_CHECKING:
    from ray.workflow.common import (
        WorkflowTaskRuntimeOptions,
    )
    from ray.workflow.workflow_context import WorkflowTaskContext


logger = logging.getLogger(__name__)


def get_task_executor(task_options: "WorkflowTaskRuntimeOptions"):
    if task_options.task_type == TaskType.FUNCTION:
        # prevent automatic lineage reconstruction
        task_options.ray_options["max_retries"] = 0
        # prevent retrying exception by Ray
        task_options.ray_options["retry_exceptions"] = False
        executor = _workflow_task_executor_remote.options(
            **task_options.ray_options
        ).remote
    else:
        raise ValueError(f"Invalid task type {task_options.task_type}")
    return executor


def _workflow_task_executor(
    func: Callable,
    context: "WorkflowTaskContext",
    task_id: "TaskID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowTaskRuntimeOptions",
) -> Tuple[Any, Any]:
    """Executor function for workflow task.

    Args:
        task_id: ID of the task.
        func: The workflow task function.
        baked_inputs: The processed inputs for the task.
        context: Workflow task context. Used to access correct storage etc.
        runtime_options: Parameters for workflow task execution.

    Returns:
        Workflow task output.
    """
    with workflow_context.workflow_task_context(context):
        store = workflow_storage.get_workflow_storage()
        # Part 1: resolve inputs
        args, kwargs = baked_inputs.resolve(store)

        # Part 2: execute the task
        try:
            store.save_task_prerun_metadata(task_id, {"start_time": time.time()})
            with workflow_context.workflow_execution():
                logger.info(f"{get_task_status_info(WorkflowStatus.RUNNING)}")
                output = func(*args, **kwargs)
            store.save_task_postrun_metadata(task_id, {"end_time": time.time()})
        except Exception as e:
            # Always checkpoint the exception.
            store.save_task_output(task_id, None, exception=e)
            raise e

        if isinstance(output, DAGNode):
            output = workflow_state_from_dag(output, None, context.workflow_id)
            execution_metadata = WorkflowExecutionMetadata(is_output_workflow=True)
        else:
            execution_metadata = WorkflowExecutionMetadata()
            if runtime_options.catch_exceptions:
                output = (output, None)

        # Part 3: save outputs
        # TODO(suquark): Validate checkpoint options before commit the task.
        if CheckpointMode(runtime_options.checkpoint) == CheckpointMode.SYNC:
            if isinstance(output, WorkflowExecutionState):
                store.save_workflow_execution_state(task_id, output)
            else:
                store.save_task_output(task_id, output, exception=None)
        return execution_metadata, output


@ray.remote(num_returns=2)
def _workflow_task_executor_remote(
    func: Callable,
    context: "WorkflowTaskContext",
    job_id: str,
    task_id: "TaskID",
    baked_inputs: "_BakedWorkflowInputs",
    runtime_options: "WorkflowTaskRuntimeOptions",
) -> Any:
    """The remote version of '_workflow_task_executor'."""
    with workflow_context.workflow_logging_context(job_id):
        return _workflow_task_executor(
            func, context, task_id, baked_inputs, runtime_options
        )


@dataclass
class _BakedWorkflowInputs:
    """This class stores pre-processed inputs for workflow task execution.
    Especially, all input workflows to the workflow task will be scheduled,
    and their outputs (ObjectRefs) replace the original workflows."""

    args: "ObjectRef"
    workflow_refs: "List[WorkflowRef]"

    def resolve(self, store: workflow_storage.WorkflowStorage) -> Tuple[List, Dict]:
        """
        This function resolves the inputs for the code inside
        a workflow task (works on the callee side). For outputs from other
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
                workflow_ref_mapping.append(store.load_task_output(r.task_id))
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
