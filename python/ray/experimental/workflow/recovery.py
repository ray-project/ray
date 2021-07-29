from typing import List, Any, Union, Dict, Callable, Tuple

import ray
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow.common import (Workflow, StepID, WorkflowRef,
                                              WorkflowExecutionResult)
from ray.experimental.workflow import storage
from ray.experimental.workflow import workflow_storage
from ray.experimental.workflow.step_function import WorkflowStepFunction


class WorkflowStepNotRecoverableError(Exception):
    """Raise the exception when we find a workflow step cannot be recovered
    using the checkpointed inputs."""

    def __init__(self, step_id: StepID):
        self.message = f"Workflow step[id={step_id}] is not recoverable"
        super().__init__(self.message)


class WorkflowNotResumableError(Exception):
    """Raise the exception when we cannot resume from a workflow."""

    def __init__(self, workflow_id: str):
        self.message = f"Workflow[id={workflow_id}] is not resumable."
        super().__init__(self.message)


@WorkflowStepFunction
def _recover_workflow_step(input_object_refs: List[str],
                           input_workflows: List[Any],
                           input_workflow_refs: List[WorkflowRef],
                           instant_workflow_inputs: Dict[int, StepID]):
    """A workflow step that recovers the output of an unfinished step.

    Args:
        input_object_refs: The object refs in the argument of
            the (original) step.
        input_workflows: The workflows in the argument of the (original) step.
            They are resolved into physical objects (i.e. the output of the
            workflows) here. They come from other recover workflows we
            construct recursively.
        instant_workflow_inputs: Same as 'input_workflows', but they come
            point to workflow steps that have output checkpoints. They override
            corresponding workflows in 'input_workflows'.

    Returns:
        The output of the recovered step.
    """
    reader = workflow_storage.get_workflow_storage()
    for index, _step_id in instant_workflow_inputs.items():
        # override input workflows with instant workflows
        input_workflows[index] = reader.load_step_output(_step_id)
    input_object_refs = [reader.load_object_ref(r) for r in input_object_refs]
    step_id = workflow_context.get_current_step_id()
    func: Callable = reader.load_step_func_body(step_id)
    args, kwargs = reader.load_step_args(
        step_id, input_workflows, input_object_refs, input_workflow_refs)
    return func(*args, **kwargs)


def _construct_resume_workflow_from_step(
        reader: workflow_storage.WorkflowStorage,
        step_id: StepID) -> Union[Workflow, StepID]:
    """Try to construct a workflow (step) that recovers the workflow step.
    If the workflow step already has an output checkpointing file, we return
    the workflow step id instead.

    Args:
        reader: The storage reader for inspecting the step.
        step_id: The ID of the step we want to recover.

    Returns:
        A workflow that recovers the step, or a ID of a step
        that contains the output checkpoint file.
    """
    result: workflow_storage.StepInspectResult = reader.inspect_step(step_id)
    if result.output_object_valid:
        # we already have the output
        return step_id
    if isinstance(result.output_step_id, str):
        return _construct_resume_workflow_from_step(reader,
                                                    result.output_step_id)
    # output does not exists or not valid. try to reconstruct it.
    if not result.is_recoverable():
        raise WorkflowStepNotRecoverableError(step_id)
    input_workflows = []
    instant_workflow_outputs: Dict[int, str] = {}
    for i, _step_id in enumerate(result.workflows):
        r = _construct_resume_workflow_from_step(reader, _step_id)
        if isinstance(r, Workflow):
            input_workflows.append(r)
        else:
            input_workflows.append(None)
            instant_workflow_outputs[i] = r
    workflow_refs = list(map(WorkflowRef, result.workflow_refs))
    recovery_workflow: Workflow = _recover_workflow_step.options(
        max_retries=result.max_retries,
        catch_exceptions=result.catch_exceptions,
        **result.ray_options).step(result.object_refs, input_workflows,
                                   workflow_refs, instant_workflow_outputs)
    recovery_workflow._step_id = step_id
    recovery_workflow.data.step_type = result.step_type
    return recovery_workflow


@ray.remote(num_returns=2)
def _resume_workflow_step_executor(
        workflow_id: str, step_id: "StepID",
        store_url: str) -> Tuple[ray.ObjectRef, ray.ObjectRef]:
    try:
        store = storage.create_storage(store_url)
        wf_store = workflow_storage.WorkflowStorage(workflow_id, store)
        r = _construct_resume_workflow_from_step(wf_store, step_id)
    except Exception as e:
        raise WorkflowNotResumableError(workflow_id) from e

    if isinstance(r, Workflow):
        with workflow_context.workflow_step_context(workflow_id,
                                                    store.storage_url):
            from ray.experimental.workflow.step_executor import (
                execute_workflow)
            result = execute_workflow(r, last_step_of_workflow=True)
            return result.persisted_output, result.volatile_output
    return wf_store.load_step_output(r), None


def resume_workflow_step(workflow_id: str, step_id: "StepID",
                         store_url: str) -> WorkflowExecutionResult:
    """Resume a step of a workflow.

    Args:
        workflow_id: The ID of the workflow job. The ID is used to identify
            the workflow.
        step_id: The step to resume in the workflow.
        store_url: The url of the storage to access the workflow.

    Raises:
        WorkflowNotResumableException: fail to resume the workflow.

    Returns:
        The execution result of the workflow, represented by Ray ObjectRef.
    """
    persisted_output, volatile_output = _resume_workflow_step_executor.remote(
        workflow_id, step_id, store_url)
    return WorkflowExecutionResult(persisted_output, volatile_output)


def get_latest_output(workflow_id: str, store: storage.Storage) -> Any:
    """Get the latest output of a workflow. This function is intended to be
    used by readonly virtual actors. To resume a workflow,
    `resume_workflow_job` should be used instead.

    Args:
        workflow_id: The ID of the workflow.
        store: The storage of the workflow.

    Returns:
        The output of the workflow.
    """
    reader = workflow_storage.WorkflowStorage(workflow_id, store)
    try:
        step_id: StepID = reader.get_latest_progress()
        while True:
            result: workflow_storage.StepInspectResult = reader.inspect_step(
                step_id)
            if result.output_object_valid:
                # we already have the output
                return reader.load_step_output(step_id)
            if isinstance(result.output_step_id, str):
                step_id = result.output_step_id
            else:
                raise ValueError(
                    "Workflow output does not exists or not valid.")
    except Exception as e:
        raise WorkflowNotResumableError(workflow_id) from e
